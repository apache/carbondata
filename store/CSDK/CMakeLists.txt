# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 2.8)
project(CSDK)
set(CMAKE_BUILD_TYPE Debug)
SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH true)

#GoogleTest LIBRARY is Optional only test/main_ft.cpp (Functional Test) Need GoogleTest lib.
#Refer csdk-guide for compile/build methods.
find_package(JNI REQUIRED)
find_package(GTest)

include_directories(${JNI_INCLUDE_DIRS})


set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(SOURCE_FILES src/CarbonReader.cpp src/CarbonReader.h src/CarbonRow.h
        src/CarbonRow.cpp src/CarbonWriter.h  src/CarbonWriter.cpp src/CarbonSchemaReader.h
        src/CarbonSchemaReader.cpp src/Schema.h src/Schema.cpp src/CarbonProperties.cpp src/CarbonProperties.h src/Configuration.cpp src/Configuration.h)
		
IF(GTEST_FOUND)
message(${GTEST_INCLUDE_DIRS})
message(${GTEST_LIBRARY})
include_directories(${GTEST_INCLUDE_DIRS})
set(SOURCE_FILES ${SOURCE_FILES} test/main_ft.cpp)
else()
set(SOURCE_FILES ${SOURCE_FILES} test/main.cpp)
endif()


add_executable(CSDK ${SOURCE_FILES})
get_filename_component(JAVA_JVM_LIBRARY_DIR ${JAVA_JVM_LIBRARY} DIRECTORY)
message(${JAVA_JVM_LIBRARY_DIR})
target_link_libraries(CSDK ${JAVA_JVM_LIBRARY})
target_link_libraries(CSDK ${GTEST_BOTH_LIBRARIES})
target_link_libraries(CSDK -lpthread -lm)

