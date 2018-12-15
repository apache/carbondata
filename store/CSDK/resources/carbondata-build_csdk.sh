#!/usr/bin/env bash


#
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
#

# This script tests the CSDK.
# Arguments:
#   BUILD_PATH - Location where build application will be placed.
#   REPORT_PATH - Location where Report will be placed.


echo "JAVA HOME IS ${JAVA_HOME}"
BUILD_PATH="$1"
REPORT_PATH="$2"
echo "${BUILD_PATH} and ${REPORT_PATH}"
mkdir -p ${BUILD_PATH}
cd ${BUILD_PATH}
cmake ../
if [ $? -eq 0 ]; then
    echo "-------CSDK CMAKE (linking) is PASS-----"
else
    echo "-------CSDK CMAKE (linking) is FAIL,Existing-----"
    exit 1
fi
make
if [ $? -eq 0 ]; then
    echo "-------CSDK Compile/Build is PASS-----"
else
    echo "-------CSDK Compile/Build is FAIL,Existing-----"
    exit 1
fi
echo "Running CSDK TestCases"
./CSDK --gtest_output="xml:${REPORT_PATH}/CSDK_Report.xml"
rm -r ${BUILD_PATH}
