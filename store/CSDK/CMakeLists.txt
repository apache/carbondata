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

find_package(JNI REQUIRED)

include_directories(${JNI_INCLUDE_DIRS})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(SOURCE_FILES src/CarbonReader.cpp src/CarbonReader.h test/main.cpp src/CarbonRow.h
        src/CarbonRow.cpp src/CarbonWriter.h  src/CarbonWriter.cpp src/CarbonSchemaReader.h
        src/CarbonSchemaReader.cpp src/Schema.h src/Schema.cpp src/CarbonProperties.cpp src/CarbonProperties.h src/Configuration.cpp src/Configuration.h)

add_executable(CSDK ${SOURCE_FILES})
get_filename_component(JAVA_JVM_LIBRARY_DIR ${JAVA_JVM_LIBRARY} DIRECTORY)
message(${JAVA_JVM_LIBRARY_DIR})
target_link_libraries(CSDK ${JAVA_JVM_LIBRARY})

