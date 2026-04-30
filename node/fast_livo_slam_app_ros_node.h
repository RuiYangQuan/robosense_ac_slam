/******************************************************************************
 * Copyright 2017 RoboSense All rights reserved.
 * Suteng Innovation Technology Co., Ltd. www.robosense.ai

 * This software is provided to you directly by RoboSense and might
 * only be used to access RoboSense LiDAR. Any compilation,
 * modification, exploration, reproduction and redistribution are
 * restricted without RoboSense's prior consent.

 * THIS SOFTWARE IS PROVIDED "AS IS" AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL ROBOSENSE BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *****************************************************************************/
#pragma once

#include "ros_dep.h"
#include "laserMapping.h"
#include "preprocess.h"

namespace robosense {
namespace slam {

class FastLivoSlamApp {
 public:
  FastLivoSlamApp(const std::string cfg_path);

  ~FastLivoSlamApp() {
    Stop();
  }

  void Start() {
    slam_ptr_->Start();
  }

  void Stop() {
    // LINFO<< "FastLivoSlamApp stopping"<<REND;
    // slam_ptr_->Stop(); //FIXME: 会析构两次，不必调用
    // LINFO<< "FastLivoSlamApp stopped"<<REND;
  }

  // 重启应用程序的方法
  void Restart(const std::string cfg_path) {
    LINFO << "Restarting FastLivoSlamApp..." << REND;
    resetStatics();
    
    // 创建新的SLAM实例
    slam_ptr_.reset(new slam::FastLivoSlam());
    slam_ptr_->Init(cfg_path);
    slam_ptr_->RegisterGetLIVOResult(full_result_func_);
    slam_ptr_->SetRGBImageCallback(rgb_img_func_);
    slam_ptr_->SetNoiseImageCallback(noise_img_func_);
    slam_ptr_->SetRawImageCallback(rs_raw_img_func_);
    slam_ptr_->SetAllCloudImageCallback(rs_all_cloud_img_func_);
    slam_ptr_->SetLaserCloudFullResCallback(cloud_register_func_);
    slam_ptr_->SetSubVisualCloudCallback(visual_sub_map_func_);
    slam_ptr_->SetLaserCloudEffectCallback(cloud_effected_func_);
    slam_ptr_->SetLaserCloudMapCallback(laser_map_func_);
    slam_ptr_->SetOdomAftMappedCallback(odom_aft_mapped_func_);
    slam_ptr_->SetPathCallback(path_func_);

    // 重新启动
    slam_ptr_->Start();
    
    LINFO << "FastLivoSlamApp restarted successfully" << REND;
  }

 private:
  std::string name() {
    return "FastLivoSlamApp";
  }
  // module
  YAML::Node cfg_node_;
  std::string cfg_path_;  // 存储配置路径
  shared_ptr<Preprocess> p_pre_;
  slam::FastLivoSlam::Ptr slam_ptr_;

  // input
  void LidarCallback(const PointCloud2MsgsConstPtr msg);
  void ImuCallback(const ImuMsgsConstPtr imu_msg_ptr);
  void ImageCallback(const ImageMsgsConstPtr image_ptr);
  void RestartSignalCallback(const EmptyMsgsConstPtr &msg);  // 新增的重启信号回调

  // ROS to common
  void CloudRosToCommon(const PointCloud2MsgsConstPtr &msg, CloudPtr &ptr, double& cloud_abs_ts);
  slam::Imu::Ptr ImuRosToCommon(const ImuMsgsConstPtr &imu_msg_ptr);
  
  // ROS subscriber and publisher
#ifdef USE_ROS1
  ros::NodeHandle nh_;
  ros::Subscriber lidar_sub_, imu_sub_, image_sub_;
  ros::Subscriber restart_signal_sub_;  // 添加重启信号订阅器
  image_transport::Publisher img_pub_, noise_img_pub_, raw_img_pub_, rs_all_cloud_img_pub_;
  ros::Publisher pubLaserCloudFullResRGB_, pubLaserCloudFullRes_,
                 pubVisualCloud_, pubSubVisualCloud_, pubLaserCloudEffect_,
                 pubLaserCloudMap_;
  ros::Publisher pubOdomAftMapped_;
  ros::Publisher pubPath_;
  tf::TransformBroadcaster br_;
#elif defined(USE_ROS2)
  //void ZeroCopyLidarCallback(const ZeroCopyPointCloud2MsgsConstPtr zc_msg);
  //void ZeroCopyImageCallback(const ZeroCopyImageMsgsConstPtr image_ptr);
  
  rclcpp::Subscription<sensor_msgs::msg::PointCloud2>::SharedPtr lidar_sub_;
  //rclcpp::Subscription<robosense_msgs::msg::RsPointCloud>::SharedPtr rs_zerocopy_lidar_sub_;
  rclcpp::Subscription<sensor_msgs::msg::Imu>::SharedPtr imu_sub_;
  rclcpp::Subscription<sensor_msgs::msg::Image>::SharedPtr image_sub_;
  //rclcpp::Subscription<robosense_msgs::msg::RsImage>::SharedPtr rs_zerocopy_image_sub_;
  rclcpp::Subscription<EmptyMsgs>::SharedPtr restart_signal_sub_;
  image_transport::Publisher img_pub_, noise_img_pub_, raw_img_pub_, rs_all_cloud_img_pub_;
  rclcpp::Publisher<PointCloud2Msgs>::SharedPtr pubLaserCloudFullResRGB_, pubLaserCloudFullRes_,
      pubVisualCloud_, pubSubVisualCloud_, pubLaserCloudEffect_,
      pubLaserCloudMap_;
  rclcpp::Publisher<nav_msgs::msg::Odometry>::SharedPtr pubOdomAftMapped_;
  rclcpp::Publisher<nav_msgs::msg::Path>::SharedPtr pubPath_;
  std::shared_ptr<tf2_ros::TransformBroadcaster> br_ptr_;
#endif
  PathMsgs path_;

  std::function<void(const LIVOResult&)> full_result_func_;
  std::function<void(const cv::Mat&)> rgb_img_func_;
  std::function<void(const cv::Mat&)> noise_img_func_;
  std::function<void(const cv::Mat&)> rs_raw_img_func_;
  std::function<void(const cv::Mat&)> rs_all_cloud_img_func_;
  std::function<void(const pcl::PointCloud<pcl::PointXYZINormal>::Ptr &msg_ptr)> cloud_register_func_;
  std::function<void(const pcl::PointCloud<pcl::PointXYZINormal>::Ptr &msg_ptr)> visual_sub_map_func_;
  std::function<void(const pcl::PointCloud<pcl::PointXYZINormal>::Ptr &msg_ptr)> cloud_effected_func_;
  std::function<void(const pcl::PointCloud<pcl::PointXYZINormal>::Ptr &msg_ptr)> laser_map_func_;
  std::function<void(const Pose &msg_ptr)> odom_aft_mapped_func_;
  std::function<void(const Pose &msg_ptr)> path_func_;

  // statics in previous version
  double LidarCallback_last_sys_t_ = 0;
  double LidarCallback_last_header_t_ = 0;
  double ImuCallback_last_sys_t_ = 0;
  double ImuCallback_last_header_t_ = 0;
  bool imu_init_done_ = false;
  void resetStatics()
  {
    LidarCallback_last_sys_t_ = 0;
    LidarCallback_last_header_t_ = 0;
    ImuCallback_last_sys_t_ = 0;
    ImuCallback_last_header_t_ = 0;
    imu_init_done_ = false;
    path_ = PathMsgs();
  }

#ifdef USE_ROS2
//   PointCloud2MsgsConstPtr ConvertZCPoints(const ZeroCopyPointCloud2MsgsConstPtr msg) {
//     PointCloud2MsgsConstPtr msg_ptr(new PointCloud2Msgs());
//     // header
//     std_msgs::msg::Header header;
//     std::string frame_id;
//     for(char c : msg->header.frame_id){
//         if(c == '\0') break;
//         frame_id.push_back(c);
//     }
//     header.frame_id = frame_id;
//     header.stamp = rclcpp::Time(msg->header.stamp);
//     msg_ptr->header = header;
//     // other
//     msg_ptr->is_bigendian = msg->is_bigendian;
//     msg_ptr->point_step = msg->point_step;
//     msg_ptr->row_step = msg->row_step;
//     msg_ptr->height = msg->height;
//     msg_ptr->width = msg->width;
//     msg_ptr->is_dense = msg->is_dense;
//     // fields
//     msg_ptr->fields.resize(msg->fields.size());
//     for (const auto& field : msg->fields) {
//       sensor_msgs::msg::PointField point_field;
//       std::string name;
//       for (char c : field.name) {
//         if (c == '\0') break;
//         name.push_back(c);
//       }
//       point_field.name = name;
//       point_field.offset = field.offset;
//       point_field.datatype = field.datatype;
//       point_field.count = field.count;
//       msg_ptr->fields.push_back(point_field);
//     }
//     // data
//     msg_ptr->data.resize(msg->row_step);
//     // copy data
//     memcpy(msg_ptr->data.data(), msg->data.data(), msg->row_step);
//     return msg_ptr;
//   }
#endif
};

} // namespace slam
} // namespace robosense
