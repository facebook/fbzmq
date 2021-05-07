#
# Copyright (c) 2014-present, Facebook, Inc.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
#

cmake_minimum_required(VERSION 3.2)

if (NOT DEFINED PACKAGE_VERSION)
  set(PACKAGE_VERSION 1.0.0)
endif()

if (NOT DEFINED SO_VERSION)
  set(SO_VERSION 1)
endif()

project(fbzmq)

if (UNIX AND (APPLE OR ${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD"))
   add_definitions(-DIS_BSD)
endif()

set(INCLUDE_INSTALL_DIR include CACHE STRING
    "The subdirectory where header files should be installed")
set(LIB_INSTALL_DIR lib CACHE STRING
  "The subdirectory where libraries should be installed")

if (NOT DEFINED CPACK_GENERATOR)
  set(CPACK_GENERATOR "RPM")
endif()
include(CPack)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "-Wall -Wextra -Werror ${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "-Wno-noexcept-type ${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "-Wno-deprecated-declarations ${CMAKE_CXX_FLAGS}")

include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_BINARY_DIR})

set(
  CMAKE_MODULE_PATH
  "${CMAKE_SOURCE_DIR}/build/fbcode_builder/CMake"
  ${CMAKE_MODULE_PATH}
)
include(FBThriftCppLibrary)

# External third-party libraries
find_library(DOUBLECONV double-conversion)
find_package(Gflags REQUIRED)
find_package(Glog REQUIRED)
find_library(PTHREAD pthread)
find_library(ZMQ zmq)
find_package(Threads REQUIRED)
find_package(Boost 1.67.0 MODULE
  COMPONENTS
    context
  REQUIRED
)

find_path(ZMQ_INCLUDE_DIR zmq.h)

include_directories(${Boost_INCLUDE_DIRS} ${ZMQ_INCLUDE_DIR})


# Facebook specific libraries
find_package(folly CONFIG REQUIRED)
find_library(ZSTD zstd)
find_package(fizz CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(wangle CONFIG REQUIRED)
find_package(FBThrift CONFIG REQUIRED)

# Include Thrift
include_directories(${FBTHRIFT_INCLUDE_DIR})

add_subdirectory(fbzmq)
