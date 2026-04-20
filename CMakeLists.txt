cmake_minimum_required(VERSION 3.5)
project(slam)

set(CMAKE_BUILD_TYPE "Release")

set(PYTHON_EXECUTABLE /usr/bin/python3)

find_package(catkin QUIET) # ROS1 核心包
find_package(ament_cmake QUIET) # ROS2 核心包

if(catkin_FOUND)
	set(ROS_VERSION "ROS1")
elseif(ament_cmake_FOUND)
	set(ROS_VERSION "ROS2")
endif()

# if(ROS_VERSION STREQUAL "ROS1")
#   configure_file(
#           package.xml.ros1.in
#           ${CMAKE_CURRENT_SOURCE_DIR}/package.xml
#           @ONLY
#   )
# elseif(ROS_VERSION STREQUAL "ROS2")
#   configure_file(
#           package.xml.ros2.in
#           ${CMAKE_CURRENT_SOURCE_DIR}/package.xml
#           @ONLY
#   )
# endif()

if(WIN32)
	add_compile_options(/W4 /std:c++17 /EHsc)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /std:c++14 /O2")
	# 关键配置：指定vcpkg工具链路径
	set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake")
else()
	add_compile_options(-std=c++17)
	set(CMAKE_CXX_FLAGS "-std=c++17 -O3 -fexceptions")
	add_compile_options(-Wno-class-memaccess)
	add_compile_options(-Wall -Wextra)
	add_compile_options(-Wno-unused-parameter)
endif()

add_definitions(-DPROJECT_PATH="${PROJECT_SOURCE_DIR}")
add_definitions(-DNOMINMAX) # 所有目标生效

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fexceptions")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
# set(CMAKE_CXX_FLAGS
# 	"${CMAKE_CXX_FLAGS} -std=c++14 -pthread -std=c++0x -std=c++14 -fexceptions"
# )
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -fexceptions")

#========================
# OpenMp part
#========================
message("Current CPU archtecture: ${CMAKE_SYSTEM_PROCESSOR}")
if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
	include(ProcessorCount)
	ProcessorCount(N)
	message("Processer number:  ${N}")
	if(N GREATER 9)
		add_definitions(-DMP_EN)
		add_definitions(-DMP_PROC_NUM=8)
		message("core for MP:  ${PROC_NUM}/${N}")
	elseif(N GREATER 7)
		add_definitions(-DMP_EN)
		add_definitions(-DMP_PROC_NUM=6)
		message("core for MP:  ${PROC_NUM}/${N}")
	elseif(N GREATER 5)
		add_definitions(-DMP_EN)
		add_definitions(-DMP_PROC_NUM=4)
		message("core for MP:  4")
	elseif(N GREATER 3)
		math(EXPR PROC_NUM "${N} - 2")
		add_definitions(-DMP_EN)
		add_definitions(-DMP_PROC_NUM="${PROC_NUM}")
		message("core for MP:  ${PROC_NUM}")
	else()
		add_definitions(-DMP_EN)
		add_definitions(-DMP_PROC_NUM=4)
	endif()
else()
	add_definitions(-DMP_EN)
	add_definitions(-DMP_PROC_NUM=4)
endif()
find_package(OpenMP QUIET)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}   ${OpenMP_C_FLAGS}")

# 查找 Ceres Solver
find_package(Ceres REQUIRED)

#========================
# ROS lib
#========================
find_package(catkin QUIET COMPONENTS roscpp)

if(ROS_VERSION STREQUAL "ROS1")
	message(STATUS "Building with ROS1")
	add_definitions(-DUSE_ROS1) # 定义ROS1宏
	find_package(catkin REQUIRED
		COMPONENTS
			geometry_msgs
			nav_msgs
			sensor_msgs
			roscpp
			std_msgs
			pcl_ros
			pcl_conversions
			tf
			message_generation
			eigen_conversions
			cv_bridge
			image_transport
	)
	include_directories(${catkin_INCLUDE_DIRS})
	catkin_package(
		CATKIN_DEPENDS
		nav_msgs
		roscpp
		std_msgs
		message_runtime
		cv_bridge
		image_transport
		INCLUDE_DIRS
		include
	)
elseif(ROS_VERSION STREQUAL "ROS2")
	find_package(ament_cmake REQUIRED)
	find_package(rclcpp REQUIRED)
	find_package(cv_bridge REQUIRED)
	find_package(sensor_msgs REQUIRED)
	find_package(robosense_msgs REQUIRED)
	find_package(nav_msgs REQUIRED)
	find_package(image_transport REQUIRED)
	find_package(pcl_conversions REQUIRED)
	find_package(tf2_ros REQUIRED) # 添加 tf2_ros 依赖
	# 寻找 PCL
	#find_package(PCL REQUIRED)
	# 显式请求 common 和 registration 等核心组件，防止遗漏
	find_package(PCL REQUIRED
		COMPONENTS
			common
			registration
			filters
			kdtree
			search
			io
			features
	)
	include_directories(include ${PCL_INCLUDE_DIRS})
	link_directories(${PCL_LIBRARY_DIRS})
	# 寻找 GTSAM
	find_package(GTSAM REQUIRED QUIET)

	message(STATUS "Building with ROS2")
	add_definitions(-DUSE_ROS2) # 定义ROS2宏
endif()

#========================
# third_party lib
#========================
add_subdirectory(third_party/Sophus)
add_subdirectory(third_party/vikit_common)

#========================
# system lib
#========================
find_package(Eigen3 REQUIRED)
# message(Eigen: ${EIGEN3_INCLUDE_DIR})
find_package(PCL REQUIRED)
find_package(cv_bridge REQUIRED)
find_package(OpenCV REQUIRED)
find_package(Boost REQUIRED COMPONENTS thread)
find_package(yaml-cpp REQUIRED)

# Include dirs
# include_directories(${EIGEN3_INCLUDE_DIR}
# 	${PCL_INCLUDE_DIRS}
# 	${OpenCV_INCLUDE_DIRS}
# 	${GTSAM_INCLUDE_DIR}
# 	include
# )
include_directories(${Eigen3_INCLUDE_DIRS}
	${EIGEN3_INCLUDE_DIRS}
	/usr/include/eigen3
	# Ubuntu 22.04 物理兜底
	${PCL_INCLUDE_DIRS}
	/usr/include/pcl-1.12
	# Ubuntu 22.04 物理兜底
	${OpenCV_INCLUDE_DIRS}
	${GTSAM_INCLUDE_DIR}
	include
)

#========================
# fast_livo lib
#========================
add_library(livo_initial
	src/livo_initial/initializer.cpp
	src/livo_initial/pre_integration.cpp
	src/livo_initial/global_sfm.cpp
	src/livo_initial/feature_tracker.cpp
)
target_link_libraries(livo_initial
	${PCL_LIBRARIES}
	${OpenCV_LIBS}
	${CERES_LIBRARIES}
	Sophus
)

#========================
# Scancontext lib
#========================
add_library(scancontext_lib src/scancontext/Scancontext.cpp)
# target_link_libraries(scancontext_lib
# ${PCL_LIBRARIES}
# ${OpenCV_LIBRARIES})
target_include_directories(scancontext_lib
	PUBLIC
		include
		${OpenCV_INCLUDE_DIRS}
		${PCL_INCLUDE_DIRS}
		/usr/include/pcl-1.12
		${Eigen3_INCLUDE_DIRS}
		/usr/include/eigen3
)

target_link_libraries(scancontext_lib
	${PCL_LIBRARIES}
	${OpenCV_LIBRARIES}
)
#========================
# fast_livo lib
#========================
add_library(fast_livo
	include/ikd-Tree/ikd_Tree.cpp
	src/lidar_selection.cpp
	src/frame.cpp
	src/point.cpp
	src/feature.cpp
	src/map.cpp
	src/IMU_Processing.cpp
	src/preprocess.cpp
	src/laserMapping.cpp
)
target_include_directories(fast_livo
	PUBLIC include
)
# target_include_directories(scancontext_lib PUBLIC
#   include
#   ${OpenCV_INCLUDE_DIRS}
#   ${PCL_INCLUDE_DIRS}
# )
target_link_libraries(fast_livo
	PUBLIC
		${PCL_LIBRARIES}
		vikit_common
		Sophus
		livo_initial
		scancontext_lib
		gtsam
)

if(WIN32)
	find_package(Threads REQUIRED)
	target_link_libraries(fast_livo PRIVATE Threads::Threads)
endif()

if(ROS_VERSION STREQUAL "ROS1")
	target_link_libraries(fast_livo
		PUBLIC ${catkin_LIBRARIES}
	)
elseif(ROS_VERSION STREQUAL "ROS2")
	ament_target_dependencies(
		fast_livo
		PUBLIC
		rclcpp
		cv_bridge
		pcl_conversions
		sensor_msgs
		nav_msgs
		image_transport
		tf2_ros
		robosense_msgs
	)
	ament_target_dependencies(
		scancontext_lib
		rclcpp
		sensor_msgs
		cv_bridge
		pcl_conversions
	)
endif()

#========================
# ROS node
#========================
add_executable(slam_node
	node/fast_livo_slam_app_ros_node.cpp
)
target_link_libraries(slam_node
	fast_livo
	${YAML_CPP_LIBRARIES}
)

# ================= 修复：后端节点的完整配置 =================
# 编译后端节点
add_executable(back_node src/back_node.cpp)
target_compile_definitions(back_node PRIVATE PCL_NO_PRECOMPILE)

# 2. 显式添加 PCL 和 GTSAM 的头文件路径
target_include_directories(back_node
	PUBLIC
		include
		${PCL_INCLUDE_DIRS}
		${GTSAM_INCLUDE_DIR}
)

# 3. 为后端节点注入 ROS2 的环境依赖 (解决找不到 rclcpp 和消息的问题)
ament_target_dependencies(
	back_node
	rclcpp
	sensor_msgs
	nav_msgs
	pcl_conversions
)

# 1. 链接所需的库 (引入 ScanContext, GTSAM 和 PCL)
target_link_libraries(back_node
	scancontext_lib
	gtsam
	${PCL_LIBRARIES}
	pcl_common # 强制指定目标名
	pcl_registration # 强制指定 SVD 所在的配准模块
	flann_cpp # 补充 FLANN 依赖
	lz4
	/usr/lib/x86_64-linux-gnu/libpcl_common.so # Ubuntu 22.04 物理路径终极兜底
	/usr/lib/x86_64-linux-gnu/libpcl_registration.so
)
if(ROS_VERSION STREQUAL "ROS1")
	# do nothing
elseif(ROS_VERSION STREQUAL "ROS2")
	ament_export_libraries(fast_livo)
	ament_export_dependencies(vikit_common)

	install(
		DIRECTORY include/
		DESTINATION include
	)
	install(TARGETS fast_livo)
	install(TARGETS vikit_common)

	# 注意：安装路径统一使用 ${PROJECT_NAME} 更加规范
	install(TARGETS slam_node DESTINATION lib/${PROJECT_NAME})
	install(TARGETS back_node DESTINATION lib/${PROJECT_NAME})

	# 4. 关键：ament_package() 必须是整个 CMake 的最后一行
	ament_package()
endif()