
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

export CFLAGS=`pkg-config --cflags thrift`" -g3 -O0 -std=c++11"
export LDFLAGS=`pkg-config --libs-only-L thrift`
export LIBS=`pkg-config --libs-only-l thrift`

export FORMAT_FILE_PATH=../../../format/src/main/thrift
export TARGET_PATH=../target

mkdir -p $TARGET_PATH

thrift --gen cpp -o $TARGET_PATH $FORMAT_FILE_PATH/carbondata_index_merge.thrift 
thrift --gen cpp -o $TARGET_PATH $FORMAT_FILE_PATH/carbondata_index.thrift
thrift --gen cpp -o $TARGET_PATH $FORMAT_FILE_PATH/carbondata.thrift
thrift --gen cpp -o $TARGET_PATH $FORMAT_FILE_PATH/dictionary_metadata.thrift
thrift --gen cpp -o $TARGET_PATH $FORMAT_FILE_PATH/dictionary.thrift
thrift --gen cpp -o $TARGET_PATH $FORMAT_FILE_PATH/schema.thrift
thrift --gen cpp -o $TARGET_PATH $FORMAT_FILE_PATH/sort_index.thrift

g++ $TARGET_PATH/gen-cpp/carbondata_constants.cpp -c $CFLAGS -o $TARGET_PATH/carbondata_constants.o
g++ $TARGET_PATH/gen-cpp/carbondata_index_constants.cpp -c $CFLAGS -o $TARGET_PATH/carbondata_index_constants.o
g++ $TARGET_PATH/gen-cpp/carbondata_index_merge_constants.cpp -c $CFLAGS -o $TARGET_PATH/carbondata_index_merge_constants.o
g++ $TARGET_PATH/gen-cpp/carbondata_index_merge_types.cpp -c $CFLAGS -o $TARGET_PATH/carbondata_index_merge_types.o
g++ $TARGET_PATH/gen-cpp/carbondata_index_types.cpp -c $CFLAGS -o $TARGET_PATH/carbondata_index_types.o
g++ $TARGET_PATH/gen-cpp/carbondata_types.cpp -c $CFLAGS -o $TARGET_PATH/carbondata_types.o
g++ $TARGET_PATH/gen-cpp/dictionary_constants.cpp -c $CFLAGS -o $TARGET_PATH/dictionary_constants.o
g++ $TARGET_PATH/gen-cpp/dictionary_metadata_constants.cpp -c $CFLAGS -o $TARGET_PATH/dictionary_metadata_constants.o
g++ $TARGET_PATH/gen-cpp/dictionary_metadata_types.cpp -c $CFLAGS -o $TARGET_PATH/dictionary_metadata_types.o
g++ $TARGET_PATH/gen-cpp/dictionary_types.cpp -c $CFLAGS -o $TARGET_PATH/dictionary_types.o
g++ $TARGET_PATH/gen-cpp/schema_constants.cpp -c $CFLAGS -o $TARGET_PATH/schema_constants.o
g++ $TARGET_PATH/gen-cpp/schema_types.cpp -c $CFLAGS -o $TARGET_PATH/schema_types.o
g++ $TARGET_PATH/gen-cpp/sort_index_constants.cpp -c $CFLAGS -o $TARGET_PATH/sort_index_constants.o
g++ $TARGET_PATH/gen-cpp/sort_index_types.cpp -c $CFLAGS -o $TARGET_PATH/sort_index_types.o

g++ carbondata_test.cpp -c $CFLAGS -I$TARGET_PATH/gen-cpp/ -o $TARGET_PATH/carbondata_test.o

g++ $TARGET_PATH/carbondata_constants.o $TARGET_PATH/carbondata_index_constants.o $TARGET_PATH/carbondata_index_merge_constants.o \
$TARGET_PATH/carbondata_index_merge_types.o $TARGET_PATH/carbondata_index_types.o $TARGET_PATH/carbondata_test.o $TARGET_PATH/carbondata_types.o \
$TARGET_PATH/dictionary_constants.o $TARGET_PATH/dictionary_metadata_constants.o $TARGET_PATH/dictionary_metadata_types.o $TARGET_PATH/dictionary_types.o \
$TARGET_PATH/schema_constants.o $TARGET_PATH/schema_types.o $TARGET_PATH/sort_index_constants.o $TARGET_PATH/sort_index_types.o -o $TARGET_PATH/carbondata_test \
$LDFLAGS $LIBS

echo build $TARGET_PATH/carbondata_test completed.
