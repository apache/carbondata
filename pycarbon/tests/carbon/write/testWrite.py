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

from examples import DEFAULT_CARBONSDK_PATH
from pycarbon.pysdk.CarbonWriter import CarbonWriter
from pycarbon.pysdk.CarbonReader import CarbonReader
import jnius_config
import sys
import time

from pycarbon.tests import S3_DATA_PATH


def main(argv):
  print("Start")
  start = time.time()
  print(argv)

  import jnius_config

  parser = argparse.ArgumentParser(description='test Read Carbon From Local')
  parser.add_argument('-c', '--carbon-sdk-path', type=str, default=DEFAULT_CARBONSDK_PATH,
                      help='carbon sdk path')
  parser.add_argument('-d', '--data-path', type=str, default=S3_DATA_PATH,
                      help='carbon sdk path')
  parser.add_argument('-ak', '--access_key', type=str, required=True,
                      help='access_key of obs')
  parser.add_argument('-sk', '--secret_key', type=str, required=True,
                      help='secret_key of obs')
  parser.add_argument('-endpoint', '--end_point', type=str, required=True,
                      help='end_point of obs')

  args = parser.parse_args()

  jnius_config.set_classpath(args.carbon_sdk_path)

  jsonSchema = "[{stringField:string},{shortField:short},{intField:int}]";
  path = "../data/writeCarbon"
  writer = CarbonWriter() \
    .builder() \
    .outputPath(path) \
    .withCsvInput(jsonSchema) \
    .writtenBy("pycarbon") \
    .build()

  for i in range(0, 10):
    from jnius import autoclass
    arrayListClass = autoclass("java.util.ArrayList");
    list = arrayListClass()
    list.add("pycarbon")
    list.add(str(i))
    list.add(str(i * 10))
    writer.write(list.toArray())
  writer.close()

  reader = CarbonReader() \
    .builder() \
    .withFolder(path) \
    .withBatch(1000) \
    .build()

  i = 0
  while (reader.hasNext()):
    rows = reader.readNextBatchRow()
    for row in rows:
      i = i + 1
      print()
      if 1 == i % 10:
        print(str(i) + " to " + str(i + 9) + ":")
      for column in row:
        print(column, end="\t")

  print()
  print("number of rows by read: " + str(i))
  reader.close()

  end = time.time()
  print("all time is " + str(end - start))
  print("Finish")


if __name__ == '__main__':
  main(sys.argv)
