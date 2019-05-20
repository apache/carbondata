#  Copyright (c) 2018-2019 Huawei Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse

import jnius_config

from examples import DEFAULT_CARBONSDK_PATH
from pycarbon.pysdk.CarbonReader import CarbonReader
import sys
import time

from pycarbon.tests import S3_DATA_PATH1, S3_DATA_PATH2


def readCarbon(args, path, label):
  builder = CarbonReader() \
    .builder()

  from jnius import autoclass
  java_list_class = autoclass('java.util.ArrayList')
  projection_list = java_list_class()
  projection_list.add("name")

  reader = builder.withBatch(780) \
    .withFolder(path) \
    .withHadoopConf("fs.s3a.access.key", args.access_key) \
    .withHadoopConf("fs.s3a.secret.key", args.secret_key) \
    .withHadoopConf("fs.s3a.endpoint", args.end_point) \
    .projection(projection_list) \
    .filterEqual("name", label) \
    .build()

  num = 0
  build = time.time()
  print("build time:" + str(build - start))
  list = []
  while (reader.hasNext()):
    rows = reader.readNextBatchRow()

    for row in rows:
      num = num + 1
      list.append(row)
      if (0 == (num % 1000)):
        print(num)
  print(num)
  reader.close()
  return list


def main():
  print("Start")
  path1 = "s3a://sdk/binary/sub1/"
  path2 = "s3a://sdk/binary/sub2/"

  parser = argparse.ArgumentParser(description='test Read Carbon From Local')
  parser.add_argument('-c', '--carbon-sdk-path', type=str, default=DEFAULT_CARBONSDK_PATH,
                      help='carbon sdk path')
  parser.add_argument('-d1', '--data-path1', type=str, default=S3_DATA_PATH1,
                      help='carbon sdk path')
  parser.add_argument('-d2', '--data-path2', type=str, default=S3_DATA_PATH2,
                      help='carbon sdk path')
  parser.add_argument('-ak', '--access_key', type=str, required=True,
                      help='access_key of obs')
  parser.add_argument('-sk', '--secret_key', type=str, required=True,
                      help='secret_key of obs')
  parser.add_argument('-endpoint', '--end_point', type=str, required=True,
                      help='end_point of obs')

  args = parser.parse_args()

  jnius_config.set_classpath(args.carbon_sdk_path)

  start = time.time()
  list1 = readCarbon(args, path1, "robot0")
  assert 1 == len(list1)
  end = time.time()
  print("list1 total time is " + str(end - start))

  start = time.time()
  list2 = readCarbon(args, path2, "robot1")
  assert 2 == len(list2)
  end = time.time()
  print("list2 total time is " + str(end - start))

  list1.extend(list2)
  print(len(list1))
  assert 3 == len(list1)
  end = time.time()
  print("total time:" + str(end - start))
  print("Finish")


if __name__ == '__main__':
  start = time.time()
  main()
  end = time.time()
  print("total time is " + str(end - start))
