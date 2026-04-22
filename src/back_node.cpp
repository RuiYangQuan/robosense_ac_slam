#include <rclcpp/rclcpp.hpp>
#include <nav_msgs/msg/odometry.hpp>
#include <sensor_msgs/msg/point_cloud2.hpp>
#include <nav_msgs/msg/path.hpp>
#include <pcl_conversions/pcl_conversions.h>
#include <pcl/point_cloud.h>
#include <pcl/point_types.h>

// GTSAM
#include <gtsam/geometry/Pose3.h>
#include <gtsam/geometry/Rot3.h>
#include <gtsam/nonlinear/NonlinearFactorGraph.h>
#include <gtsam/nonlinear/Values.h>
#include <gtsam/nonlinear/ISAM2.h>
#include <gtsam/slam/BetweenFactor.h>
#include <gtsam/slam/PriorFactor.h>

#include <pcl/registration/gicp.h>
#include <pcl/filters/voxel_grid.h>
#include <optional>

#include "scancontext/Scancontext.h"
#include <pcl/common/transforms.h>
#include <visualization_msgs/msg/marker_array.hpp>
#include <geometry_msgs/msg/point.hpp>
#include <pcl/io/pcd_io.h>
#include <std_srvs/srv/trigger.hpp>

#include <mutex>
#include <cmath>

class Back : public rclcpp::Node
{
public:
  Back() : Node("back_node")
  {
    // 初始化
    gtsam::ISAM2Params parameters;
    parameters.relinearizeThreshold = 0.1;
    parameters.relinearizeSkip = 1;
    isam_ = std::make_shared<gtsam::ISAM2>(parameters);

    rclcpp::QoS qos_profile = rclcpp::SensorDataQoS();

    gtsam::Vector prior_noise_vector(6);
    prior_noise_vector << 1e-6, 1e-6, 1e-6, 1e-6, 1e-6, 1e-6; // 初始位姿先验噪声 (XYZ 极小，Roll/Pitch 较小，Yaw 随意)
    prior_noise_ = gtsam::noiseModel::Diagonal::Variances(prior_noise_vector);

    gtsam::Vector odom_noise_vector(6);
    odom_noise_vector << 1e-4, 1e-4, 1e-4, 1e-2, 1e-2, 1e-2;
    odom_noise_ = gtsam::noiseModel::Diagonal::Variances(odom_noise_vector);

    gtsam::Vector loop_noise_vector(6);
    loop_noise_vector << 0.05, 0.05, 0.05, 0.1, 0.1, 0.1;
    auto base_loop_noise = gtsam::noiseModel::Diagonal::Variances(loop_noise_vector);

    // 引入 Cauchy 鲁棒核：当回环误差过大时，GTSAM 会自动降低这个约束的权重甚至忽略它，防止系统崩溃
    loop_noise_ = gtsam::noiseModel::Robust::Create(
        gtsam::noiseModel::mEstimator::Cauchy::Create(1.0), base_loop_noise);

    // 3. ROS2 订阅与发布
    sub_odom_ = this->create_subscription<nav_msgs::msg::Odometry>(
        "/aft_mapped_to_init", 100,
        std::bind(&Back::odomCallback, this, std::placeholders::_1));

    sub_cloud_ = this->create_subscription<sensor_msgs::msg::PointCloud2>(
        "/cloud_registered", 100,
        std::bind(&Back::cloudCallback, this, std::placeholders::_1));
    pub_path_ = this->create_publisher<nav_msgs::msg::Path>("/backend/optimized_path", 100);

    pub_global_map_ = this->create_publisher<sensor_msgs::msg::PointCloud2>("/backend/global_map", 1);
    srv_save_map_ = this->create_service<std_srvs::srv::Trigger>(
        "/backend/save_map",
        std::bind(&Back::saveMapService, this, std::placeholders::_1, std::placeholders::_2));

    // 后台定时器，每 3 秒执行一次全局地图拼接与发布
    map_timer_ = this->create_wall_timer(
        std::chrono::seconds(3), std::bind(&Back::publishGlobalMap, this));
    pub_loop_constraints_ = this->create_publisher<visualization_msgs::msg::MarkerArray>("/backend/loop_constraints", 10);
    RCLCPP_INFO(this->get_logger(), "Map Saving Service Ready.");
  }

private:
  gtsam::Pose3 odom2GtsamPose(const nav_msgs::msg::Odometry::SharedPtr &msg)
  {
    return gtsam::Pose3(
        gtsam::Rot3::Quaternion(msg->pose.pose.orientation.w,
                                msg->pose.pose.orientation.x,
                                msg->pose.pose.orientation.y,
                                msg->pose.pose.orientation.z),
        gtsam::Point3(msg->pose.pose.position.x,
                      msg->pose.pose.position.y,
                      msg->pose.pose.position.z));
  }

  void odomCallback(const nav_msgs::msg::Odometry::SharedPtr msg)
  {
    std::lock_guard<std::mutex> lock(mtx_);
    latest_odom_pose_ = odom2GtsamPose(msg);
    has_odom_ = true;
  }
  void cloudCallback(const sensor_msgs::msg::PointCloud2::SharedPtr cloud_msg)
  {
    std::lock_guard<std::mutex> lock(mtx_);
    if (!has_odom_)
      return;

    gtsam::Pose3 current_pose = latest_odom_pose_;

    if (keyframe_count_ > 0)
    {
      double translation_diff = (current_pose.translation() - last_keyframe_pose_.translation()).norm();
      double rotation_diff = current_pose.rotation().localCoordinates(last_keyframe_pose_.rotation()).norm();
      // 关键帧
      double translation_threshold = 0.2;
      double rotation_threshold = 0.05;
      if (translation_diff < translation_threshold && rotation_diff < rotation_threshold)
        return;
    }

    pcl::PointCloud<pcl::PointXYZI>::Ptr this_keyframe(new pcl::PointCloud<pcl::PointXYZI>());
    pcl::fromROSMsg(*cloud_msg, *this_keyframe);
    keyframe_clouds_.push_back(this_keyframe);
    frontend_poses_.push_back(current_pose);
    if (keyframe_count_ == 0)
    {
      gtSAMgraph_.add(gtsam::PriorFactor<gtsam::Pose3>(0, current_pose, prior_noise_));
      initial_estimate_.insert(0, current_pose);
    }
    else
    {
      gtsam::Pose3 relative_pose = last_keyframe_pose_.between(current_pose);
      gtSAMgraph_.add(gtsam::BetweenFactor<gtsam::Pose3>(
          keyframe_count_ - 1, keyframe_count_, relative_pose, odom_noise_));
      initial_estimate_.insert(keyframe_count_, current_pose);
    }
    last_keyframe_pose_ = current_pose;
    scManager_.makeAndSaveScancontextAndKeys(*this_keyframe);
    // <历史关键帧 ID, 距离分数>
    std::pair<int, float> loop_result = scManager_.detectLoopClosureID();
    int loop_kf_idx = loop_result.first;

    if (loop_kf_idx != -1)
    {
      RCLCPP_INFO(this->get_logger(), "Loop Candidate Found! Curr: %d, Hist: %d. Running ICP...", keyframe_count_, loop_kf_idx);

      std::optional<gtsam::Pose3> icp_relative_pose = performICP(loop_kf_idx, current_pose, this_keyframe);

      if (icp_relative_pose.has_value())
      {
        if (icp_relative_pose.value().matrix().hasNaN())
        {
          RCLCPP_WARN(this->get_logger(), "Poisonous ICP pose (NaN detected)! Rejecting.");
          return; // 丢弃这个回环
        }
        RCLCPP_INFO(this->get_logger(), "ICP Succeeded! Adding Loop Factor.");
        gtSAMgraph_.add(gtsam::BetweenFactor<gtsam::Pose3>(
            loop_kf_idx, keyframe_count_, icp_relative_pose.value(), loop_noise_));
        loop_edges_.push_back({keyframe_count_, loop_kf_idx});
      }
      else
      {
        RCLCPP_WARN(this->get_logger(), "ICP Failed. Rejecting Loop.");
      }
    }

    isam_->update(gtSAMgraph_, initial_estimate_);
    isam_->update();
    gtSAMgraph_.resize(0);
    initial_estimate_.clear();
    isam_current_estimate_ = isam_->calculateEstimate();

    publishOptimizedPath();
    publishLoopConstraints();
    keyframe_count_++;
  }
  std::optional<gtsam::Pose3> performICP(int loop_kf_idx, const gtsam::Pose3 &pose_curr, pcl::PointCloud<pcl::PointXYZI>::Ptr curr_cloud)
  {
    pcl::PointCloud<pcl::PointXYZI>::Ptr history_cloud = keyframe_clouds_[loop_kf_idx];

    pcl::VoxelGrid<pcl::PointXYZI> downSizeFilter;
    downSizeFilter.setLeafSize(0.15, 0.15, 0.15);
    pcl::PointCloud<pcl::PointXYZI>::Ptr curr_cloud_ds(new pcl::PointCloud<pcl::PointXYZI>());
    pcl::PointCloud<pcl::PointXYZI>::Ptr history_cloud_ds(new pcl::PointCloud<pcl::PointXYZI>());

    downSizeFilter.setInputCloud(curr_cloud);
    downSizeFilter.filter(*curr_cloud_ds);
    downSizeFilter.setInputCloud(history_cloud);
    downSizeFilter.filter(*history_cloud_ds);
    if (curr_cloud_ds->points.size() < 30 || history_cloud_ds->points.size() < 30)
    {
      RCLCPP_WARN(this->get_logger(),
                  "Cloud too sparse after downsampling (Curr: %lu, Hist: %lu). Skipping ICP to prevent crash.",
                  curr_cloud_ds->points.size(), history_cloud_ds->points.size());
      return std::nullopt;
    }
    gtsam::Pose3 pose_hist = isam_current_estimate_.at<gtsam::Pose3>(loop_kf_idx);
    gtsam::Pose3 T_hist_curr_guess = pose_hist.between(pose_curr);
    Eigen::Matrix4f guess_matrix = T_hist_curr_guess.matrix().cast<float>();
    // T_hist_curr = 历史坐标系到当前坐标系的变换
    // gtsam::Pose3 T_hist_curr_guess = pose_hist.between(pose_curr);
    // Eigen::Matrix4f guess_matrix = T_hist_curr_guess.matrix().cast<float>();

    pcl::GeneralizedIterativeClosestPoint<pcl::PointXYZI, pcl::PointXYZI> gicp;
    gicp.setMaxCorrespondenceDistance(2.0); // 允许的最大匹配距离
    gicp.setMaximumIterations(30);
    gicp.setTransformationEpsilon(1e-6);
    gicp.setEuclideanFitnessEpsilon(1e-6);
    gicp.setRANSACIterations(0);

    gicp.setInputSource(curr_cloud_ds);
    gicp.setInputTarget(history_cloud_ds);

    pcl::PointCloud<pcl::PointXYZI>::Ptr aligned_cloud(new pcl::PointCloud<pcl::PointXYZI>());
    gicp.align(*aligned_cloud, guess_matrix);

    if (gicp.hasConverged() && gicp.getFitnessScore() < 0.35)
    {
      Eigen::Matrix4d final_transform = gicp.getFinalTransformation().cast<double>();
      if (final_transform.hasNaN() || !final_transform.allFinite())
      {
        RCLCPP_ERROR(this->get_logger(), "ICP returned NaN or Inf! This is a degenerate scene. Rejecting.");
        return std::nullopt;
      }
      gtsam::Pose3 icp_pose(final_transform);
      gtsam::Pose3 guess_pose(guess_matrix.cast<double>());

      double trans_diff = (icp_pose.translation() - guess_pose.translation()).norm();
      double rot_diff = icp_pose.rotation().localCoordinates(guess_pose.rotation()).norm();

      // 防护
      if (trans_diff > 0.5 || rot_diff > 0.15)
      {
        RCLCPP_WARN(this->get_logger(),
                    "ICP converged but jump is TOO WILD (Trans: %.2fm, Rot: %.2f). Rejecting to prevent explosion!", trans_diff, rot_diff);
        return std::nullopt;
      }

      return icp_pose;
    }
    return std::nullopt;
  }
  void publishOptimizedPath()
  {
    nav_msgs::msg::Path path_msg;
    path_msg.header.stamp = this->now();
    path_msg.header.frame_id = "camera_init";

    for (int i = 0; i < keyframe_count_ + 1; ++i)
    {
      if (!isam_current_estimate_.exists(i))
        continue;

      gtsam::Pose3 pose = isam_current_estimate_.at<gtsam::Pose3>(i);
      geometry_msgs::msg::PoseStamped pose_stamped;
      pose_stamped.header = path_msg.header;

      pose_stamped.pose.position.x = pose.translation().x();
      pose_stamped.pose.position.y = pose.translation().y();
      pose_stamped.pose.position.z = pose.translation().z();

      gtsam::Quaternion quat = pose.rotation().toQuaternion();
      pose_stamped.pose.orientation.x = quat.x();
      pose_stamped.pose.orientation.y = quat.y();
      pose_stamped.pose.orientation.z = quat.z();
      pose_stamped.pose.orientation.w = quat.w();

      path_msg.poses.push_back(pose_stamped);
    }
    pub_path_->publish(path_msg);
  }
  void publishLoopConstraints()
  {
    if (loop_edges_.empty() || pub_loop_constraints_->get_subscription_count() == 0)
      return;

    visualization_msgs::msg::MarkerArray marker_array;
    visualization_msgs::msg::Marker marker;

    marker.header.stamp = this->now();
    marker.header.frame_id = "camera_init";
    marker.ns = "loop_edges";
    marker.id = 0;
    marker.type = visualization_msgs::msg::Marker::LINE_LIST; // 约束线
    marker.action = visualization_msgs::msg::Marker::ADD;
    marker.pose.orientation.w = 1.0;

    marker.scale.x = 0.1;

    marker.color.r = 1.0;
    marker.color.g = 0.9;
    marker.color.b = 0.0;
    marker.color.a = 1.0;

    for (const auto &edge : loop_edges_)
    {
      int curr_idx = edge.first;
      int hist_idx = edge.second;

      // 确保这两帧都已经被 ISAM2 优化过存在于图里
      if (!isam_current_estimate_.exists(curr_idx) || !isam_current_estimate_.exists(hist_idx))
        continue;

      gtsam::Pose3 pose_curr = isam_current_estimate_.at<gtsam::Pose3>(curr_idx);
      gtsam::Pose3 pose_hist = isam_current_estimate_.at<gtsam::Pose3>(hist_idx);

      geometry_msgs::msg::Point p_curr, p_hist;
      p_curr.x = pose_curr.translation().x();
      p_curr.y = pose_curr.translation().y();
      p_curr.z = pose_curr.translation().z();

      p_hist.x = pose_hist.translation().x();
      p_hist.y = pose_hist.translation().y();
      p_hist.z = pose_hist.translation().z();

      marker.points.push_back(p_curr);
      marker.points.push_back(p_hist);
    }

    marker_array.markers.push_back(marker);
    pub_loop_constraints_->publish(marker_array);
  }
  void publishGlobalMap()
  {
    if (pub_global_map_->get_subscription_count() == 0)
      return;

    gtsam::Values estimates_copy;
    int kf_count_copy;

    {
      std::lock_guard<std::mutex> lock(mtx_);
      if (keyframe_count_ == 0 || isam_current_estimate_.empty())
        return;
      estimates_copy = isam_current_estimate_;
      kf_count_copy = keyframe_count_;
    }

    pcl::PointCloud<pcl::PointXYZI>::Ptr global_map(new pcl::PointCloud<pcl::PointXYZI>());

    for (int i = 0; i < kf_count_copy; ++i)
    {
      if (!estimates_copy.exists(i))
        continue;
      gtsam::Pose3 T_optimized = estimates_copy.at<gtsam::Pose3>(i); // 后端优化后的位姿
      gtsam::Pose3 T_frontend = frontend_poses_[i];                  // 当时的前端位姿

      gtsam::Pose3 delta_correction = T_optimized * T_frontend.inverse();
      Eigen::Matrix4f transform_matrix = delta_correction.matrix().cast<float>();

      pcl::PointCloud<pcl::PointXYZI>::Ptr transformed_cloud(new pcl::PointCloud<pcl::PointXYZI>());
      pcl::transformPointCloud(*keyframe_clouds_[i], *transformed_cloud, transform_matrix);
      if (transformed_cloud->points.empty())
        continue;
      // 拼接到全局地图中
      *global_map += *transformed_cloud;
    }

    pcl::VoxelGrid<pcl::PointXYZI> downSizeFilter;
    downSizeFilter.setLeafSize(0.4, 0.4, 0.4);
    pcl::PointCloud<pcl::PointXYZI>::Ptr global_map_ds(new pcl::PointCloud<pcl::PointXYZI>());
    downSizeFilter.setInputCloud(global_map);
    downSizeFilter.filter(*global_map_ds);

    sensor_msgs::msg::PointCloud2 map_msg;
    pcl::toROSMsg(*global_map_ds, map_msg);
    map_msg.header.stamp = this->now();
    map_msg.header.frame_id = "camera_init";
    pub_global_map_->publish(map_msg);
  }
  void saveMapService(const std::shared_ptr<std_srvs::srv::Trigger::Request> req,
                      std::shared_ptr<std_srvs::srv::Trigger::Response> res)
  {
    RCLCPP_INFO(this->get_logger(), "Received save map request! Generating high-res map...");

    gtsam::Values estimates_copy;
    int kf_count_copy;

    {
      std::lock_guard<std::mutex> lock(mtx_);
      if (keyframe_count_ == 0 || isam_current_estimate_.empty())
      {
        res->success = false;
        res->message = "Failed: No point clouds or poses to save!";
        return;
      }
      estimates_copy = isam_current_estimate_;
      kf_count_copy = keyframe_count_;
    }

    pcl::PointCloud<pcl::PointXYZI>::Ptr final_map(new pcl::PointCloud<pcl::PointXYZI>());
    pcl::VoxelGrid<pcl::PointXYZI> downSizeFilter;
    downSizeFilter.setLeafSize(0.03, 0.03, 0.03);
    for (int i = 0; i < kf_count_copy; ++i)
    {
      if (!estimates_copy.exists(i))
        continue;
      if (keyframe_clouds_[i]->points.empty())
        continue;
      gtsam::Pose3 T_optimized = estimates_copy.at<gtsam::Pose3>(i);
      gtsam::Pose3 T_frontend = frontend_poses_[i];
      gtsam::Pose3 delta_correction = T_optimized * T_frontend.inverse();
      Eigen::Matrix4f transform_matrix = delta_correction.matrix().cast<float>();

      pcl::PointCloud<pcl::PointXYZI>::Ptr transformed_cloud(new pcl::PointCloud<pcl::PointXYZI>());
      pcl::transformPointCloud(*keyframe_clouds_[i], *transformed_cloud, transform_matrix);
      if (transformed_cloud->points.empty())
        continue;
      pcl::PointCloud<pcl::PointXYZI>::Ptr transformed_cloud_ds(new pcl::PointCloud<pcl::PointXYZI>());
      downSizeFilter.setInputCloud(transformed_cloud);
      downSizeFilter.filter(*transformed_cloud_ds);

      *final_map += *transformed_cloud_ds;
    }
    if (final_map->points.empty())
    {
      res->success = false;
      res->message = "Failed: The generated map is totally empty!";
      RCLCPP_ERROR(this->get_logger(), "%s", res->message.c_str());
      return;
    }
    pcl::PointCloud<pcl::PointXYZI>::Ptr final_map_ds(new pcl::PointCloud<pcl::PointXYZI>());
    downSizeFilter.setInputCloud(final_map);
    downSizeFilter.filter(*final_map_ds);

    std::string save_path = std::string(PROJECT_PATH) + "/PCD/" + "fast_livo_final_map.pcd";
    pcl::io::savePCDFileBinary(save_path, *final_map_ds);

    res->success = true;
    res->message = "BINGO! High-res map saved to: " + save_path + " (Points: " + std::to_string(final_map_ds->points.size()) + ")";
    RCLCPP_INFO(this->get_logger(), "%s", res->message.c_str());
  }
  // 类成员变量
  std::mutex mtx_;
  bool has_odom_ = false;
  int keyframe_count_ = 0;
  gtsam::Pose3 latest_odom_pose_;
  gtsam::Pose3 last_keyframe_pose_;

  std::vector<pcl::PointCloud<pcl::PointXYZI>::Ptr> keyframe_clouds_;
  std::vector<gtsam::Pose3> frontend_poses_;
  std::shared_ptr<gtsam::ISAM2>
      isam_;
  gtsam::NonlinearFactorGraph gtSAMgraph_;
  gtsam::Values initial_estimate_;
  gtsam::Values isam_current_estimate_;
  gtsam::noiseModel::Diagonal::shared_ptr prior_noise_;
  gtsam::noiseModel::Diagonal::shared_ptr odom_noise_;
  gtsam::noiseModel::Base::shared_ptr loop_noise_;
  // 存放成功回环的对子：<当前帧ID, 历史帧ID>
  std::vector<std::pair<int, int>> loop_edges_;
  rclcpp::Publisher<visualization_msgs::msg::MarkerArray>::SharedPtr pub_loop_constraints_;
  rclcpp::Service<std_srvs::srv::Trigger>::SharedPtr srv_save_map_;

  SCManager scManager_;

  rclcpp::Subscription<nav_msgs::msg::Odometry>::SharedPtr sub_odom_;
  rclcpp::Subscription<sensor_msgs::msg::PointCloud2>::SharedPtr sub_cloud_;
  rclcpp::Publisher<nav_msgs::msg::Path>::SharedPtr pub_path_;
  rclcpp::TimerBase::SharedPtr map_timer_;
  rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr pub_global_map_;
};

int main(int argc, char **argv)
{
  rclcpp::init(argc, argv);
  rclcpp::spin(std::make_shared<Back>());
  rclcpp::shutdown();
  return 0;
}