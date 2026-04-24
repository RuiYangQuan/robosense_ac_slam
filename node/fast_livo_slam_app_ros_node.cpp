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

/**
 * @file fast_livo_slam_app_ros_node.cpp
 * @brief Fast-LIVO SLAM ROS2 节点实现
 *
 * 该文件实现了 FastLivoSlamApp 类，该类是 Fast-LIVO SLAM 系统的 ROS2 节点封装。
 * 它负责：
 * - 初始化点云预处理模块和 SLAM 核心模块
 * - 订阅激光雷达、IMU 和相机数据
 * - 发布 SLAM 结果（位姿、点云地图、路径等）
 * - 处理重启信号
 *
 * 支持多种激光雷达类型（Ouster、Velodyne、XT32 等）和 ROS1/ROS2 兼容。
 */

#include "fast_livo_slam_app_ros_node.h"

/**
 * @namespace robosense
 * @brief RoboSense 命名空间
 */

/**
 * @namespace slam
 * @brief SLAM 相关功能命名空间
 */
namespace robosense
{
  namespace slam
  {

    /**
     * @brief FastLivoSlamApp 构造函数
     * @param cfg_path 配置文件路径，包含 RS_META.yaml 等配置文件
     *
     * 构造函数执行以下初始化步骤：
     * 1. 保存配置文件路径
     * 2. 创建并初始化点云预处理模块 (Preprocess)
     * 3. 创建并初始化 Fast-LIVO SLAM 核心模块
     * 4. 从配置文件读取传感器话题名称
     * 5. 根据编译宏 (USE_ROS1 或 USE_ROS2) 创建相应的 ROS 订阅者和发布者
     * 6. 设置各种回调函数用于处理 SLAM 结果和发布数据
     */
FastLivoSlamApp::FastLivoSlamApp(const std::string cfg_path) {

  /**************** 核心模块初始化  ****************/
  // 保存配置路径，用于后续重启等操作
  cfg_path_ = cfg_path;
  
  // 初始化点云预处理模块，负责滤波、特征提取等
  p_pre_.reset(new Preprocess(cfg_path));
  
  // 初始化 Fast-LIVO SLAM 核心模块
  slam_ptr_.reset(new slam::FastLivoSlam());
  slam_ptr_->Init(cfg_path);

      /**************** ROS 通信设置  ****************/
      // 从配置文件读取传感器数据话题名称
      std::string config_file = cfg_path + "/RS_META.yaml";
      auto cfg_node = YAML::LoadFile(config_file);
      std::string lid_topic, imu_topic, img_topic, compressed_img_topic;
      yamlRead(cfg_node["topic"], "lid_topic", lid_topic);
      yamlRead(cfg_node["topic"], "imu_topic", imu_topic);
      yamlRead(cfg_node["topic"], "img_topic", img_topic);
      yamlRead(cfg_node["topic"], "compressed_img_topic", compressed_img_topic);
      LINFO << "topic: lidar: " << lid_topic << " imu: " << imu_topic << " camera: " << img_topic << " compressed_img_topic: " << compressed_img_topic << REND;

#ifdef USE_ROS1
      // ROS1 订阅者
      lidar_sub_ = nh_.subscribe(lid_topic, 100, &FastLivoSlamApp::LidarCallback, this);
      imu_sub_ = nh_.subscribe(imu_topic, 1000, &FastLivoSlamApp::ImuCallback, this);
      image_sub_ = nh_.subscribe(img_topic, 100, &FastLivoSlamApp::ImageCallback, this);

      // 添加重启信号订阅器
      restart_signal_sub_ = nh_.subscribe("/fast_livo/restart_signal", 10, &FastLivoSlamApp::RestartSignalCallback, this);
#elif defined(USE_ROS2)
      // ROS2 节点和订阅者
      ros2_node = rclcpp::Node::make_shared("fast_livo_slam_app");
      br_ptr_ = std::make_shared<tf2_ros::TransformBroadcaster>(ros2_node);
      imu_sub_ = ros2_node->create_subscription<sensor_msgs::msg::Imu>(imu_topic, 5000, std::bind(&FastLivoSlamApp::ImuCallback, this, std::placeholders::_1));
      const char *val = std::getenv("RMW_FASTRTPS_USE_QOS_FROM_XML");
      if (val != nullptr && std::string(val) == "1")
      {
        LWARNING << "is zero copy mode" << REND;
        // rs_zerocopy_lidar_sub_ = ros2_node->create_subscription<robosense_msgs::msg::RsPointCloud>(lid_topic, 100, std::bind(&FastLivoSlamApp::ZeroCopyLidarCallback, this, std::placeholders::_1));
        // rs_zerocopy_image_sub_ = ros2_node->create_subscription<robosense_msgs::msg::RsImage>(img_topic, 300, std::bind(&FastLivoSlamApp::ZeroCopyImageCallback, this, std::placeholders::_1));
      }
      else
      {
        lidar_sub_ = ros2_node->create_subscription<sensor_msgs::msg::PointCloud2>(lid_topic, 100, std::bind(&FastLivoSlamApp::LidarCallback, this, std::placeholders::_1));
        image_sub_ = ros2_node->create_subscription<sensor_msgs::msg::Image>(img_topic, 300, std::bind(&FastLivoSlamApp::ImageCallback, this, std::placeholders::_1));
      }
      restart_signal_sub_ = ros2_node->create_subscription<EmptyMsgs>("/fast_livo/restart_signal", 10, std::bind(&FastLivoSlamApp::RestartSignalCallback, this, std::placeholders::_1));
#endif

      /**************** SLAM 结果处理回调  ****************/
      /**
       * @brief Fast-LIVO SLAM 结果回调函数
       *
       * 该回调函数处理 SLAM 核心模块输出的结果，根据传感器类型发布相应的数据：
       * - LIDAR: 发布点云、位姿、TF 等
       * - CAMERA: 发布视觉 SLAM 结果
       * - IMU: 处理 IMU 初始化状态
       */
      full_result_func_ = [this](const LIVOResult &livo_res)
      {
        // 检查 IMU 初始化状态
        // static bool imu_init_done{false};
        if (!imu_init_done_)
        {
          imu_init_done_ = livo_res.imu_process_result->init_process >= 100;
          LINFO << "IMU init process: " << livo_res.imu_process_result->init_process
                << REND;
        }

        // 根据传感器类型处理不同的 SLAM 结果
        LTITLE << "LIVO result updated. sensor type: " << int(livo_res.source) << REND;
        if (livo_res.source == SensorType::LIDAR)
        {
          // 处理激光雷达 SLAM 结果
          LINFO << "LIO ts: " << livo_res.update_ts << REND;
          Pose pose = livo_res.lio_result->lidar_pose;

          // 获取各种点云数据
          CloudPtr scan = livo_res.lio_result->scan;             // 当前扫描点云
          CloudPtr map = livo_res.lio_result->map;               // 局部地图点云
          CloudPtr kdtree_map = livo_res.lio_result->kdtree_map; // KDTree 地图
          RGBCloudPtr rgb_scan = livo_res.lio_result->rgb_scan;  // 彩色扫描点云
          RGBCloudPtr rgb_map = livo_res.lio_result->rgb_map;    // 彩色地图点云

          // 记录点云大小信息
          LINFO << "size scan: " << scan->size() << " map: " << map->size()
                << " kdtree: " << kdtree_map->size()
                << " rgb scan: " << rgb_scan->size()
                << " rgb map: " << rgb_map->size() << REND;

          // 获取视觉 SLAM 结果的图像数据
          cv::Mat img_raw = livo_res.vio_result->img_raw;             // 原始图像
          cv::Mat img_all_cloud = livo_res.vio_result->img_all_cloud; // 点云投影图像

          // publisher, impl by ROS. TODO: your own version
          {
#ifdef USE_ROS1
            static ros::Publisher test = nh_.advertise<sensor_msgs::PointCloud2>(
                "/cloud_registered_rgb", 100);
            sensor_msgs::PointCloud2 laserCloudmsg;
            pcl::toROSMsg(*rgb_scan, laserCloudmsg);
            //  pcl::toROSMsg(*rgb_map, laserCloudmsg); // 不要发布全量地图，阻塞算法
            laserCloudmsg.header.stamp = ros::Time::now();
            laserCloudmsg.header.frame_id = "camera_init";
            test.publish(laserCloudmsg);
#elif defined(USE_ROS2)
            static auto test = ros2_node->create_publisher<sensor_msgs::msg::PointCloud2>("/cloud_registered_rgb", 100);
            sensor_msgs::msg::PointCloud2 laserCloudmsg;
            pcl::toROSMsg(*rgb_scan, laserCloudmsg);
            laserCloudmsg.header.stamp = ros2_node->now();
            laserCloudmsg.header.frame_id = "camera_init";
            test->publish(laserCloudmsg);
#endif
          }
        }
        else if (livo_res.source == SensorType::CAMERA)
        {
          LINFO << "VIO ts: " << livo_res.update_ts << REND;
          Pose cam_pose = livo_res.vio_result->cam_pose;
        }
        else if (livo_res.source == SensorType::IMU)
        {
          // LINFO << "IMU init process: " << livo_res.imu_process_result->init_process
          //       << REND;
        }
        else
        {
          LERROR << "wrong source type: " << REND;
        }
      };
      this->slam_ptr_->RegisterGetLIVOResult(full_result_func_);

      /**************** 调试用发布者设置  ****************/
      /**
       * @brief 设置各种调试用的话题发布者
       *
       * 这些发布者主要用于调试和可视化，包括：
       * - 图像发布者：RGB 图像、噪声图像、原始图像等
       * - 点云发布者：注册点云、视觉子地图、效果点云、地图等
       * - 位姿和路径发布者：里程计、路径等
       *
       * 支持 ROS1 和 ROS2 两种版本
       */
#ifdef USE_ROS1
      // image publisher
      image_transport::ImageTransport it(nh_);
      img_pub_ = it.advertise("/rgb_img", 1);
      rgb_img_func_ = [this](const cv::Mat &img)
      {
        cv_bridge::CvImage out_msg;
        out_msg.header.stamp = ros::Time::now();
        out_msg.encoding = sensor_msgs::image_encodings::BGR8;
        out_msg.image = img;
        this->img_pub_.publish(out_msg.toImageMsg());
      };
      this->slam_ptr_->SetRGBImageCallback(rgb_img_func_);

      noise_img_pub_ = it.advertise("/noise_img", 1);
      noise_img_func_ = [this](const cv::Mat &img)
      {
        cv_bridge::CvImage out_msg;
        out_msg.header.stamp = ros::Time::now();
        out_msg.encoding = sensor_msgs::image_encodings::BGR8;
        out_msg.image = img;
        this->noise_img_pub_.publish(out_msg.toImageMsg());
      };
      this->slam_ptr_->SetNoiseImageCallback(noise_img_func_);

      raw_img_pub_ = it.advertise("/rs_raw_img", 1);
      rs_raw_img_func_ = [this](const cv::Mat &img)
      {
        cv_bridge::CvImage out_msg;
        out_msg.header.stamp = ros::Time::now();
        out_msg.encoding = sensor_msgs::image_encodings::BGR8;
        out_msg.image = img;
        this->raw_img_pub_.publish(out_msg.toImageMsg());
      };
      this->slam_ptr_->SetRawImageCallback(rs_raw_img_func_);

      rs_all_cloud_img_pub_ = it.advertise("/rs_all_cloud_img", 1);
      rs_all_cloud_img_func_ = [this](const cv::Mat &img)
      {
        cv_bridge::CvImage out_msg;
        out_msg.header.stamp = ros::Time::now();
        out_msg.encoding = sensor_msgs::image_encodings::BGR8;
        out_msg.image = img;
        this->rs_all_cloud_img_pub_.publish(out_msg.toImageMsg());
      };
      this->slam_ptr_->SetAllCloudImageCallback(rs_all_cloud_img_func_);

      // // cloud publisher
      // pubLaserCloudFullResRGB_ = nh_.advertise<sensor_msgs::PointCloud2>("/cloud_registered", 100); //cloud_registered_rgb
      // auto cloud_register_rgb_func = [this](const pcl::PointCloud<pcl::PointXYZRGB>::Ptr &msg_ptr) {
      //   sensor_msgs::PointCloud2 laserCloudmsg;
      //   pcl::toROSMsg(*msg_ptr, laserCloudmsg);
      //   laserCloudmsg.header.stamp = ros::Time::now();
      //   laserCloudmsg.header.frame_id = "camera_init";
      //   this->pubLaserCloudFullResRGB_.publish(laserCloudmsg);
      // };
      // slam_ptr_->SetLaserCloudFullResRGBCallback(cloud_register_rgb_func);

      pubLaserCloudFullRes_ = nh_.advertise<sensor_msgs::PointCloud2>("/cloud_registered", 100);
      cloud_register_func_ = [this](const pcl::PointCloud<pcl::PointXYZINormal>::Ptr &msg_ptr)
      {
        sensor_msgs::PointCloud2 laserCloudmsg;
        pcl::toROSMsg(*msg_ptr, laserCloudmsg);
        laserCloudmsg.header.stamp = ros::Time::now();
        laserCloudmsg.header.frame_id = "camera_init";
        this->pubLaserCloudFullRes_.publish(laserCloudmsg);
      };
      slam_ptr_->SetLaserCloudFullResCallback(cloud_register_func_); // 原始点云

      pubSubVisualCloud_ = nh_.advertise<sensor_msgs::PointCloud2>("/cloud_visual_sub_map", 100);
      visual_sub_map_func_ = [this](const pcl::PointCloud<pcl::PointXYZINormal>::Ptr &msg_ptr)
      {
        sensor_msgs::PointCloud2 laserCloudmsg;
        pcl::toROSMsg(*msg_ptr, laserCloudmsg);
        laserCloudmsg.header.stamp = ros::Time::now();
        laserCloudmsg.header.frame_id = "camera_init";
        this->pubSubVisualCloud_.publish(laserCloudmsg);
      };
      slam_ptr_->SetSubVisualCloudCallback(visual_sub_map_func_); // rgb点云地图

      pubLaserCloudEffect_ = nh_.advertise<sensor_msgs::PointCloud2>("/cloud_effected", 100);
      cloud_effected_func_ = [this](const pcl::PointCloud<pcl::PointXYZINormal>::Ptr &msg_ptr)
      {
        sensor_msgs::PointCloud2 laserCloudmsg;
        pcl::toROSMsg(*msg_ptr, laserCloudmsg);
        laserCloudmsg.header.stamp = ros::Time::now();
        laserCloudmsg.header.frame_id = "camera_init";
        this->pubLaserCloudEffect_.publish(laserCloudmsg);
      };
      slam_ptr_->SetLaserCloudEffectCallback(cloud_effected_func_); // 降采样后的点云

      pubLaserCloudMap_ = nh_.advertise<sensor_msgs::PointCloud2>("/Laser_map", 100);
      laser_map_func_ = [this](const pcl::PointCloud<pcl::PointXYZINormal>::Ptr &msg_ptr)
      {
        sensor_msgs::PointCloud2 laserCloudmsg;
        pcl::toROSMsg(*msg_ptr, laserCloudmsg);
        laserCloudmsg.header.stamp = ros::Time::now();
        laserCloudmsg.header.frame_id = "camera_init";
        this->pubLaserCloudMap_.publish(laserCloudmsg);
      };
      slam_ptr_->SetLaserCloudMapCallback(laser_map_func_);

      // odom publisher
      pubOdomAftMapped_ = nh_.advertise<nav_msgs::Odometry>("/aft_mapped_to_init", 10);
      odom_aft_mapped_func_ = [this](const Pose &msg_ptr)
      {
        nav_msgs::Odometry odomAftMapped;
        odomAftMapped.header.frame_id = "camera_init";
        odomAftMapped.child_frame_id = "aft_mapped";
        odomAftMapped.header.stamp = GetTimeNow();
        odomAftMapped.pose.pose.position.x = msg_ptr.xyz.x();
        odomAftMapped.pose.pose.position.y = msg_ptr.xyz.y();
        odomAftMapped.pose.pose.position.z = msg_ptr.xyz.z();

        odomAftMapped.pose.pose.orientation.x = msg_ptr.q.x();
        odomAftMapped.pose.pose.orientation.y = msg_ptr.q.y();
        odomAftMapped.pose.pose.orientation.z = msg_ptr.q.z();
        odomAftMapped.pose.pose.orientation.w = msg_ptr.q.w();

        // tf
        tf::Transform transform;
        transform.setOrigin(
            tf::Vector3(msg_ptr.xyz.x(), msg_ptr.xyz.y(), msg_ptr.xyz.z()));
        tf::Quaternion q;
        q.setW(msg_ptr.q.w());
        q.setX(msg_ptr.q.x());
        q.setY(msg_ptr.q.y());
        q.setZ(msg_ptr.q.z());
        transform.setRotation(q);
        br_.sendTransform(tf::StampedTransform(transform, odomAftMapped.header.stamp,
                                               "camera_init", "aft_mapped"));
        this->pubOdomAftMapped_.publish(odomAftMapped);
      };
      slam_ptr_->SetOdomAftMappedCallback(odom_aft_mapped_func_); // LIO位姿

      // path publisher
      pubPath_ = nh_.advertise<nav_msgs::Path>("/path", 10);
      path_func_ = [this](const Pose &msg_ptr)
      {
        geometry_msgs::PoseStamped msg_body_pose;
        msg_body_pose.pose.position.x = msg_ptr.xyz.x();
        msg_body_pose.pose.position.y = msg_ptr.xyz.y();
        msg_body_pose.pose.position.z = msg_ptr.xyz.z();

        msg_body_pose.pose.orientation.x = msg_ptr.q.x();
        msg_body_pose.pose.orientation.y = msg_ptr.q.y();
        msg_body_pose.pose.orientation.z = msg_ptr.q.z();
        msg_body_pose.pose.orientation.w = msg_ptr.q.w();
        path_.poses.emplace_back(msg_body_pose);
        path_.header.frame_id = "camera_init";
        this->pubPath_.publish(path_);
      };
      slam_ptr_->SetPathCallback(path_func_); // LIO位姿路径
#elif defined(USE_ROS2)
      // image publisher
      img_pub_ = image_transport::create_publisher(ros2_node.get(), "/rgb_img");
      rgb_img_func_ = [this](const cv::Mat &img)
      {
        auto out_msg = cv_bridge::CvImage(std_msgs::msg::Header(), "bgr8", img).toImageMsg();
        out_msg->header.stamp = ros2_node->now();
        this->img_pub_.publish(out_msg);
      };
      this->slam_ptr_->SetRGBImageCallback(rgb_img_func_);

      noise_img_pub_ = image_transport::create_publisher(ros2_node.get(), "/noise_img");
      noise_img_func_ = [this](const cv::Mat &img)
      {
        auto out_msg = cv_bridge::CvImage(std_msgs::msg::Header(), "bgr8", img).toImageMsg();
        out_msg->header.stamp = ros2_node->now();
        this->noise_img_pub_.publish(out_msg);
      };
      this->slam_ptr_->SetNoiseImageCallback(noise_img_func_);

      raw_img_pub_ = image_transport::create_publisher(ros2_node.get(), "/rs_raw_img");
      rs_raw_img_func_ = [this](const cv::Mat &img)
      {
        auto out_msg = cv_bridge::CvImage(std_msgs::msg::Header(), "bgr8", img).toImageMsg();
        out_msg->header.stamp = ros2_node->now();
        this->raw_img_pub_.publish(out_msg);
      };
      this->slam_ptr_->SetRawImageCallback(rs_raw_img_func_);

      rs_all_cloud_img_pub_ = image_transport::create_publisher(ros2_node.get(), "/rs_all_cloud_img");
      rs_all_cloud_img_func_ = [this](const cv::Mat &img)
      {
        auto out_msg = cv_bridge::CvImage(std_msgs::msg::Header(), "bgr8", img).toImageMsg();
        out_msg->header.stamp = ros2_node->now();
        this->rs_all_cloud_img_pub_.publish(out_msg);
      };
      this->slam_ptr_->SetAllCloudImageCallback(rs_all_cloud_img_func_);

      //  // cloud publisher
       pubLaserCloudFullRes_ = ros2_node->create_publisher<sensor_msgs::msg::PointCloud2>("/cloud_registered", 100);
       cloud_register_func_ = [this](const pcl::PointCloud<pcl::PointXYZINormal>::Ptr &msg_ptr) {
         sensor_msgs::msg::PointCloud2 laserCloudmsg;
         pcl::toROSMsg(*msg_ptr, laserCloudmsg);
         laserCloudmsg.header.stamp = ros2_node->now();
         laserCloudmsg.header.frame_id = "camera_init";
         LINFO << "cloud_register_func_================" <<REND;
         this->pubLaserCloudFullRes_->publish(laserCloudmsg);
         LINFO << "cloud_register_func_1================" <<REND;
       };
       slam_ptr_->SetLaserCloudFullResCallback(cloud_register_func_);

      // cloud publisher
      pubLaserCloudFullResRGB_ = ros2_node->create_publisher<sensor_msgs::msg::PointCloud2>("/cloud_registered", 100); // cloud_registered_rgb
      auto cloud_register_rgb_func = [this](const pcl::PointCloud<pcl::PointXYZRGB>::Ptr &msg_ptr)
      {
        sensor_msgs::msg::PointCloud2 laserCloudmsg;
        pcl::toROSMsg(*msg_ptr, laserCloudmsg);
        laserCloudmsg.header.stamp = ros2_node->now();
        laserCloudmsg.header.frame_id = "camera_init";
        LINFO << "cloud_register_rgb_func================" << REND;
        this->pubLaserCloudFullResRGB_->publish(laserCloudmsg);
        LINFO << "cloud_register_rgb_func——1================" << REND;
      };
      slam_ptr_->SetLaserCloudFullResRGBCallback(cloud_register_rgb_func);//rgb点云

      pubSubVisualCloud_ = ros2_node->create_publisher<sensor_msgs::msg::PointCloud2>("/cloud_visual_sub_map", 100);
      visual_sub_map_func_ = [this](const pcl::PointCloud<pcl::PointXYZINormal>::Ptr &msg_ptr)
      {
        sensor_msgs::msg::PointCloud2 laserCloudmsg;
        pcl::toROSMsg(*msg_ptr, laserCloudmsg);
        laserCloudmsg.header.stamp = ros2_node->now();
        laserCloudmsg.header.frame_id = "camera_init";
        this->pubSubVisualCloud_->publish(laserCloudmsg);
      };
      slam_ptr_->SetSubVisualCloudCallback(visual_sub_map_func_);//视觉子图

      pubLaserCloudEffect_ = ros2_node->create_publisher<sensor_msgs::msg::PointCloud2>("/cloud_effected", 100);
      cloud_effected_func_ = [this](const pcl::PointCloud<pcl::PointXYZINormal>::Ptr &msg_ptr)
      {
        sensor_msgs::msg::PointCloud2 laserCloudmsg;
        pcl::toROSMsg(*msg_ptr, laserCloudmsg);
        laserCloudmsg.header.stamp = ros2_node->now();
        laserCloudmsg.header.frame_id = "camera_init";
        this->pubLaserCloudEffect_->publish(laserCloudmsg);
      };
      slam_ptr_->SetLaserCloudEffectCallback(cloud_effected_func_);//降采样后的点云

      pubLaserCloudMap_ = ros2_node->create_publisher<sensor_msgs::msg::PointCloud2>("/Laser_map", 100);
      laser_map_func_ = [this](const pcl::PointCloud<pcl::PointXYZINormal>::Ptr &msg_ptr)
      {
        sensor_msgs::msg::PointCloud2 laserCloudmsg;
        pcl::toROSMsg(*msg_ptr, laserCloudmsg);
        laserCloudmsg.header.stamp = ros2_node->now();
        laserCloudmsg.header.frame_id = "camera_init";
        this->pubLaserCloudMap_->publish(laserCloudmsg);
      };
      slam_ptr_->SetLaserCloudMapCallback(laser_map_func_);//lio局部地图

      // odom publisher
      pubOdomAftMapped_ = ros2_node->create_publisher<nav_msgs::msg::Odometry>("/aft_mapped_to_init", 10);
      odom_aft_mapped_func_ = [this](const Pose &msg_ptr)
      {
        auto odomAftMapped = std::make_shared<nav_msgs::msg::Odometry>();
        odomAftMapped->header.frame_id = "camera_init";
        odomAftMapped->child_frame_id = "aft_mapped";
        odomAftMapped->header.stamp = ros2_node->now();
        odomAftMapped->pose.pose.position.x = msg_ptr.xyz.x();
        odomAftMapped->pose.pose.position.y = msg_ptr.xyz.y();
        odomAftMapped->pose.pose.position.z = msg_ptr.xyz.z();

        odomAftMapped->pose.pose.orientation.x = msg_ptr.q.x();
        odomAftMapped->pose.pose.orientation.y = msg_ptr.q.y();
        odomAftMapped->pose.pose.orientation.z = msg_ptr.q.z();
        odomAftMapped->pose.pose.orientation.w = msg_ptr.q.w();

        // tf
        geometry_msgs::msg::TransformStamped transform;
        transform.header.stamp = ros2_node->now();
        transform.header.frame_id = "camera_init";
        transform.child_frame_id = "aft_mapped";
        transform.transform.translation.x = msg_ptr.xyz.x();
        transform.transform.translation.y = msg_ptr.xyz.y();
        transform.transform.translation.z = msg_ptr.xyz.z();
        transform.transform.rotation.x = msg_ptr.q.x();
        transform.transform.rotation.y = msg_ptr.q.y();
        transform.transform.rotation.z = msg_ptr.q.z();
        transform.transform.rotation.w = msg_ptr.q.w();
        br_ptr_->sendTransform(transform);

        this->pubOdomAftMapped_->publish(*odomAftMapped);
      };
      slam_ptr_->SetOdomAftMappedCallback(odom_aft_mapped_func_);//lio位姿

      // path publisher
      pubPath_ = ros2_node->create_publisher<nav_msgs::msg::Path>("/path", 10);
      path_func_ = [this](const Pose &msg_ptr)
      {
        geometry_msgs::msg::PoseStamped msg_body_pose;
        msg_body_pose.header.frame_id = "camera_init";
        msg_body_pose.pose.position.x = msg_ptr.xyz.x();
        msg_body_pose.pose.position.y = msg_ptr.xyz.y();
        msg_body_pose.pose.position.z = msg_ptr.xyz.z();

        msg_body_pose.pose.orientation.x = msg_ptr.q.x();
        msg_body_pose.pose.orientation.y = msg_ptr.q.y();
        msg_body_pose.pose.orientation.z = msg_ptr.q.z();
        msg_body_pose.pose.orientation.w = msg_ptr.q.w();
        path_.poses.emplace_back(msg_body_pose);
        path_.header.frame_id = "camera_init";
        this->pubPath_->publish(path_);
      };
      slam_ptr_->SetPathCallback(path_func_);
#endif
    }

    slam::Imu::Ptr FastLivoSlamApp::ImuRosToCommon(const ImuMsgsConstPtr &imu_msg_ptr)
    {
      if (imu_msg_ptr == nullptr)
        throw std::runtime_error("imu_msg_ptr == nulptr");
      slam::Imu::Ptr imu_ptr(new slam::Imu);
      imu_ptr->header.timestamp = HeaderToNanoSec(imu_msg_ptr->header);
      imu_ptr->header.frame_id = imu_msg_ptr->header.frame_id;
      //  imu_ptr->header.seq_id = imu_msg_ptr->header.seq;
      imu_ptr->orientation.x = imu_msg_ptr->orientation.x;
      imu_ptr->orientation.y = imu_msg_ptr->orientation.y;
      imu_ptr->orientation.z = imu_msg_ptr->orientation.z; 
      imu_ptr->orientation.w = imu_msg_ptr->orientation.w;
      imu_ptr->angular_velocity.x = imu_msg_ptr->angular_velocity.x;
      imu_ptr->angular_velocity.y = imu_msg_ptr->angular_velocity.y;
      imu_ptr->angular_velocity.z = imu_msg_ptr->angular_velocity.z;
      imu_ptr->linear_acceleration.x = imu_msg_ptr->linear_acceleration.x;
      imu_ptr->linear_acceleration.y = imu_msg_ptr->linear_acceleration.y;
      imu_ptr->linear_acceleration.z = imu_msg_ptr->linear_acceleration.z;
      for (size_t i = 0; i < imu_ptr->angular_velocity_covariance.size(); ++i)
      {
        imu_ptr->angular_velocity_covariance[i] = imu_msg_ptr->angular_velocity_covariance[i];
      }
      for (size_t i = 0; i < imu_ptr->orientation_covariance.size(); ++i)
      {
        imu_ptr->orientation_covariance[i] = imu_msg_ptr->orientation_covariance[i];
      }
      for (size_t i = 0; i < imu_ptr->linear_acceleration_covariance.size(); ++i)
      {
        imu_ptr->linear_acceleration_covariance[i] = imu_msg_ptr->linear_acceleration_covariance[i];
      }
      return imu_ptr;
    }

    void FastLivoSlamApp::CloudRosToCommon(const PointCloud2MsgsConstPtr &msg,
                                           CloudPtr &ptr, double &cloud_abs_ts)
    {
      switch (p_pre_->lidar_type)
      {
      case RS_META:
      {
        break;
      }
      case ARIY:
      {
        // 1. 使用我们之前在 preprocess.h 中定义的速腾专用点云结构体
        pcl::PointCloud<robosense_ros::Point> pl_orig;
        
        // 2. 将 ROS 2 消息解包为 PCL 点云
        pcl::fromROSMsg(*msg, pl_orig);
        
        // 安全校验：防止收到空帧导致程序崩溃
        if (pl_orig.points.empty()) {
            RCLCPP_WARN(rclcpp::get_logger("rclcpp"), "Received empty Robosense cloud!");
            break;
        }

        // 3. 提取这一帧点云的基准时间戳（模仿 ac1 的逻辑，取最后一个点的时间）
        cloud_abs_ts = pl_orig.points.back().timestamp;

        // 4. 调用对应的处理函数，将解包好的点云和时间戳传进去
        p_pre_->robosense_handler(pl_orig, cloud_abs_ts);
        break;
      }

      case OUST64:
      {
        pcl::PointCloud<ouster_ros::Point> pl_orig;
        pcl::fromROSMsg(*msg, pl_orig);
        p_pre_->oust64_handler(pl_orig);
        break;
      }

      case VELO16:
      {
#if RS_POINT
        pcl::PointCloud<robosense::Point> pl_orig;
#else
        pcl::PointCloud<velodyne_ros::Point> pl_orig;
#endif
        pcl::fromROSMsg(*msg, pl_orig);
        cloud_abs_ts = pl_orig.points.back().timestamp;
        p_pre_->velodyne_handler(pl_orig, cloud_abs_ts);
        break;
      }

      case XT32:
      {
        pcl::PointCloud<xt32_ros::Point> pl_orig;
        pcl::fromROSMsg(*msg, pl_orig);
        p_pre_->xt32_handler(pl_orig);
        break;
      }

      default:
        printf("Error LiDAR Type");
        break;
      }
      ptr = p_pre_->pcl_out;
    }

    std::ofstream f_sensor_buf(std::string(PROJECT_PATH) + "/Log/raw_sensor.txt", ios::out);
    std::mutex mtx_cb;

    /**
     * @brief 激光雷达数据回调函数
     * @param msg 点云消息
     */
    void FastLivoSlamApp::LidarCallback(const PointCloud2MsgsConstPtr msg)
    {
      // static double last_sys_t = 0;
      // static double last_header_t = 0;
      double sys_t = GetTimeNowInSecond();
      double header_ts = HeaderToSec(msg->header);

      // mtx_cb.lock();
      // f_sensor_buf << std::fixed << "sys: " << sys_t
      //              << " sys_dif: " << sys_t - last_sys_t << " lid " << header_ts
      //              << " dif: " << header_ts - last_header_t << std::endl;
      // mtx_cb.unlock();

      LidarCallback_last_sys_t_ = sys_t;
      LidarCallback_last_header_t_ = header_ts;

      CloudPtr ptr(new PointCloudXYZI());
      double cloud_abs_ts;

      double b_t = omp_get_wtime();
      CloudRosToCommon(msg, ptr, cloud_abs_ts);
      double e_t = omp_get_wtime();
      printf("[ INPUT ] preprocess cloud done, header_ts: %.6f cloud_ts: %.6f "
             "size: %d cost(ms): %f.\n",
             header_ts, cloud_abs_ts, int(ptr->points.size()), (e_t - b_t) * 1000);

      slam_ptr_->AddData(ptr, cloud_abs_ts);
    }

    void FastLivoSlamApp::ImuCallback(const ImuMsgsConstPtr imu_msg_ptr)
    {
      // mtx_cb.lock();
      // static int cnt{0};
      // if(cnt++ % 1 == 0)
      //   f_sensor_buf << std::fixed<< "sys: "<<GetTimeNow() << "   imu " << HeaderToSec(imu_msg_ptr->header)
      //                << std::endl;
      // mtx_cb.unlock();

      const auto &imu_ptr = ImuRosToCommon(imu_msg_ptr);
      slam_ptr_->AddData(imu_ptr);
    }

    void FastLivoSlamApp::ImageCallback(const ImageMsgsConstPtr image_ptr)
    {
      // static double last_sys_t = 0;
      // static double last_header_t = 0;
      double sys_t = GetTimeNowInSecond();
      double header_ts = HeaderToSec(image_ptr->header);

      // mtx_cb.lock();
      // f_sensor_buf << std::fixed << "sys: " << sys_t
      //              << " sys_dif: " << sys_t - last_sys_t << "   img " << header_ts
      //              << " dif: " << header_ts - last_header_t << std::endl;
      // mtx_cb.unlock();

      ImuCallback_last_sys_t_ = sys_t;
      ImuCallback_last_header_t_ = header_ts;

      std::shared_ptr<cv::Mat> cv_img_ptr(new cv::Mat);
      *cv_img_ptr = cv_bridge::toCvCopy(image_ptr, "bgr8")->image;
      slam_ptr_->AddData(cv_img_ptr, header_ts);
    }

#ifdef USE_ROS2
// void FastLivoSlamApp::ZeroCopyLidarCallback(const ZeroCopyPointCloud2MsgsConstPtr zc_msg) {
//   try {
//     PointCloud2MsgsConstPtr msg = ConvertZCPoints(zc_msg);
//     double header_ts = static_cast<double>(zc_msg->header.stamp.sec) + static_cast<double>(zc_msg->header.stamp.nanosec) / 1e9;

//     CloudPtr ptr(new PointCloudXYZI());
//     double cloud_abs_ts;

//     double b_t = omp_get_wtime();
//     CloudRosToCommon(msg, ptr, cloud_abs_ts);
//     double e_t = omp_get_wtime();
//     printf("[ INPUT ] preprocess cloud done, header_ts: %.6f cloud_ts: %.6f "
//           "size: %d cost(ms): %f.\n",
//           header_ts, cloud_abs_ts, int(ptr->points.size()), (e_t - b_t) * 1000);

//     slam_ptr_->AddData(ptr, cloud_abs_ts);
//   } catch (std::exception &e) {
//     std::cout << e.what();
//   }
// }

// void FastLivoSlamApp::ZeroCopyImageCallback(const ZeroCopyImageMsgsConstPtr image_ptr) {
//   try {
//     double header_ts = static_cast<double>(image_ptr->header.stamp.sec) + static_cast<double>(image_ptr->header.stamp.nanosec) / 1e9;
//     std::shared_ptr<cv::Mat> cv_img_ptr(new cv::Mat);
//     cv::Mat image(cv::Size(image_ptr->width, image_ptr->height), CV_8UC3, reinterpret_cast<void*>(const_cast<unsigned char*>(image_ptr->data.data())));
//     *cv_img_ptr = image.clone();
//     cvtColor(*cv_img_ptr, *cv_img_ptr, cv::COLOR_RGB2BGR);
//     slam_ptr_->AddData(cv_img_ptr, header_ts);
//   } catch (std::exception &e) {
//     std::cout << e.what();
//   }
// }
#endif

    /**
     * @brief 重启信号回调函数
     * @param msg 空消息
     */
    void FastLivoSlamApp::RestartSignalCallback(const EmptyMsgsConstPtr &msg)
    {
      LINFO << "Received restart signal!" << REND;
      Restart(cfg_path_);
    }

  } // namespace slam
} // namespace robosense

// bool flg_exit_ = false;
// void SigHandle(int sig) { flg_exit_ = true; }

int main(int argc, char **argv)
{
#ifdef USE_ROS1
  ros::init(argc, argv, "fast_livo_slam_app");
#elif defined(USE_ROS2)
  rclcpp::init(argc, argv);
#endif
  //  signal(SIGINT, SigHandle);

  std::string cfg_path = std::string(PROJECT_PATH) + "/config/";
  robosense::slam::FastLivoSlamApp app(cfg_path);
  app.Start();

  // RosRate loop_rate(1000);
  // while (IsRosOK() && !flg_exit_) {
  //   RosSpinOnce();
  //   loop_rate.sleep();
  // }
  // app.Stop();
  // RosShutDown();
  RosSpin();
  RosShutDown();
  return 0;
}
