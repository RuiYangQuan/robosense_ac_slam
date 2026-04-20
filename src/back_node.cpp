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

// ScanContext
#include "scancontext/Scancontext.h"

#include <mutex>
#include <cmath>

class Back : public rclcpp::Node
{
public:
  Back() : Node("back_node")
  {
    // 1. 初始化 ISAM2 参数
    gtsam::ISAM2Params parameters;
    parameters.relinearizeThreshold = 0.1;
    parameters.relinearizeSkip = 1;
    isam_ = std::make_shared<gtsam::ISAM2>(parameters);

    rclcpp::QoS qos_profile = rclcpp::SensorDataQoS();

    // 2. 初始化噪声模型 (协方差矩阵)
    gtsam::Vector prior_noise_vector(6);
    prior_noise_vector << 1e-2, 1e-2, M_PI, 1e8, 1e8, 1e8; // 初始位姿先验噪声 (XYZ 极小，Roll/Pitch 较小，Yaw 随意)
    prior_noise_ = gtsam::noiseModel::Diagonal::Variances(prior_noise_vector);

    gtsam::Vector odom_noise_vector(6);
    odom_noise_vector << 1e-6, 1e-6, 1e-6, 1e-4, 1e-4, 1e-4; // 前端里程计置信度很高
    odom_noise_ = gtsam::noiseModel::Diagonal::Variances(odom_noise_vector);

    // 回环噪声模型 (置信度比里程计低一点，允许图优化拉扯)
    gtsam::Vector loop_noise_vector(6);
    loop_noise_vector << 0.05, 0.05, 0.05, 0.1, 0.1, 0.1;
    loop_noise_ = gtsam::noiseModel::Diagonal::Variances(loop_noise_vector);

    // 3. ROS2 订阅与发布
    sub_odom_ = this->create_subscription<nav_msgs::msg::Odometry>(
        "/aft_mapped_to_init", 100,
        std::bind(&Back::odomCallback, this, std::placeholders::_1));

    sub_cloud_ = this->create_subscription<sensor_msgs::msg::PointCloud2>(
        "/cloud_effected", 100,
        std::bind(&Back::cloudCallback, this, std::placeholders::_1));
    pub_path_ = this->create_publisher<nav_msgs::msg::Path>("/backend/optimized_path", 100);
    RCLCPP_INFO(this->get_logger(), "Back Node Initialized Successfully.");
  }

private:
  // 将 ROS Odometry 转换为 GTSAM Pose3
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

    // ================= 1. 关键帧判定 =================
    if (keyframe_count_ > 0)
    {
      double translation_diff = (current_pose.translation() - last_keyframe_pose_.translation()).norm();
      double rotation_diff = current_pose.rotation().localCoordinates(last_keyframe_pose_.rotation()).norm();
      if (translation_diff < 1 && rotation_diff < 0.2)
        return; // 阈值放宽一点，避免点云过密
    }

    // ================= 2. 存储点云与添加里程计因子 =================
    pcl::PointCloud<pcl::PointXYZI>::Ptr this_keyframe(new pcl::PointCloud<pcl::PointXYZI>());
    pcl::fromROSMsg(*cloud_msg, *this_keyframe);
    keyframe_clouds_.push_back(this_keyframe); // 存入历史库供 ICP 使用
    if(keyframe_count_ == 0)
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

    // ================= 3. ScanContext 回环检测 =================
    scManager_.makeAndSaveScancontextAndKeys(*this_keyframe);
    // 返回值是一个 pair: <历史关键帧 ID, 距离分数>
    std::pair<int, float> loop_result = scManager_.detectLoopClosureID();
    int loop_kf_idx = loop_result.first;

    if (loop_kf_idx != -1)
    {
      RCLCPP_WARN(this->get_logger(), "Loop Candidate Found! Curr: %d, Hist: %d. Running ICP...", keyframe_count_, loop_kf_idx);

      // 执行 ICP 计算精确相对位姿
      std::optional<gtsam::Pose3> icp_relative_pose = performICP(loop_kf_idx, current_pose, this_keyframe);

      if (icp_relative_pose.has_value())
      {
        RCLCPP_INFO(this->get_logger(), "ICP Succeeded! Adding Loop Factor.");
        // 添加闭环约束边: 从历史节点指向当前节点
        gtSAMgraph_.add(gtsam::BetweenFactor<gtsam::Pose3>(
            loop_kf_idx, keyframe_count_, icp_relative_pose.value(), loop_noise_));
      }
      else
      {
        RCLCPP_WARN(this->get_logger(), "ICP Failed. Rejecting Loop.");
      }
    }

    // ================= 4. 执行 ISAM2 图优化 =================
    isam_->update(gtSAMgraph_, initial_estimate_);
    isam_->update();
    gtSAMgraph_.resize(0);
    initial_estimate_.clear();
    isam_current_estimate_ = isam_->calculateEstimate();

    publishOptimizedPath();
    keyframe_count_++;
  }
  // ================= 核心：点云配准 (GICP) =================
  std::optional<gtsam::Pose3> performICP(int loop_kf_idx, const gtsam::Pose3& pose_curr, pcl::PointCloud<pcl::PointXYZI>::Ptr curr_cloud) {
    pcl::PointCloud<pcl::PointXYZI>::Ptr history_cloud = keyframe_clouds_[loop_kf_idx];

    // 1. 体素滤波降采样 (加速配准并防止内存爆满)
    pcl::VoxelGrid<pcl::PointXYZI> downSizeFilter;
    downSizeFilter.setLeafSize(0.3, 0.3, 0.3);
    pcl::PointCloud<pcl::PointXYZI>::Ptr curr_cloud_ds(new pcl::PointCloud<pcl::PointXYZI>());
    pcl::PointCloud<pcl::PointXYZI>::Ptr history_cloud_ds(new pcl::PointCloud<pcl::PointXYZI>());

    downSizeFilter.setInputCloud(curr_cloud);
    downSizeFilter.filter(*curr_cloud_ds);
    downSizeFilter.setInputCloud(history_cloud);
    downSizeFilter.filter(*history_cloud_ds);
  if (curr_cloud_ds->points.size() < 50 || history_cloud_ds->points.size() < 50) {
      RCLCPP_WARN(this->get_logger(), 
                  "Cloud too sparse after downsampling (Curr: %lu, Hist: %lu). Skipping ICP to prevent crash.", 
                  curr_cloud_ds->points.size(), history_cloud_ds->points.size());
      return std::nullopt;
  }
    // 2. 获取当前图优化对这两个位姿的“信念”作为初始猜测 (Initial Guess)
    //gtsam::Pose3 pose_curr = isam_current_estimate_.at<gtsam::Pose3>(curr_kf_idx);
    gtsam::Pose3 pose_hist = isam_current_estimate_.at<gtsam::Pose3>(loop_kf_idx);
    gtsam::Pose3 T_hist_curr_guess = pose_hist.between(pose_curr); 
    Eigen::Matrix4f guess_matrix = T_hist_curr_guess.matrix().cast<float>();
    // T_hist_curr = 历史坐标系到当前坐标系的变换
   // gtsam::Pose3 T_hist_curr_guess = pose_hist.between(pose_curr);
   // Eigen::Matrix4f guess_matrix = T_hist_curr_guess.matrix().cast<float>();

    // 3. 配置 GICP
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

    // 4. 严格校验 ICP 结果
    if (gicp.hasConverged() && gicp.getFitnessScore() < 0.2)
    { // 0.2 是极度严格的阈值，根据雷达线束可调整到 0.3
      Eigen::Matrix4d final_transform = gicp.getFinalTransformation().cast<double>();
      return gtsam::Pose3(final_transform);
    }
    return std::nullopt;
  }
  void publishOptimizedPath()
  {
    nav_msgs::msg::Path path_msg;
    path_msg.header.stamp = this->now();
    path_msg.header.frame_id = "camera_init"; // FAST-LIVO 默认的全局坐标系通常是 camera_init

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

  // 类成员变量
  std::mutex mtx_;
  bool has_odom_ = false;
  int keyframe_count_ = 0;
  gtsam::Pose3 latest_odom_pose_;
  gtsam::Pose3 last_keyframe_pose_;

  std::vector<pcl::PointCloud<pcl::PointXYZI>::Ptr> keyframe_clouds_;
  // GTSAM 相关核心对象
  std::shared_ptr<gtsam::ISAM2> isam_;
  gtsam::NonlinearFactorGraph gtSAMgraph_;
  gtsam::Values initial_estimate_;
  gtsam::Values isam_current_estimate_;
  gtsam::noiseModel::Diagonal::shared_ptr prior_noise_;
  gtsam::noiseModel::Diagonal::shared_ptr odom_noise_;
  gtsam::noiseModel::Diagonal::shared_ptr loop_noise_;

  SCManager scManager_;

  rclcpp::Subscription<nav_msgs::msg::Odometry>::SharedPtr sub_odom_;
  rclcpp::Subscription<sensor_msgs::msg::PointCloud2>::SharedPtr sub_cloud_;
  rclcpp::Publisher<nav_msgs::msg::Path>::SharedPtr pub_path_;
};

int main(int argc, char **argv)
{
  rclcpp::init(argc, argv);
  rclcpp::spin(std::make_shared<Back>());
  rclcpp::shutdown();
  return 0;
}