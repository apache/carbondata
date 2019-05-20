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
from pycarbon.pysdk.CarbonReader import CarbonReader
import time
from obs import *

from pycarbon.tests import S3_DATA_PATH


def list_obs_files(obs_client, bucket_name, prefix):
  files = []

  pageSize = 1000
  index = 1
  nextMarker = None
  while True:
    resp = obs_client.listObjects(bucket_name, prefix=prefix, max_keys=pageSize, marker=nextMarker)
    for content in resp.body.contents:
      files.append(content.key)
    if not resp.body.is_truncated:
      break
    nextMarker = resp.body.next_marker
    index += 1

  return files


def read_obs_files(access_key, secret_key, end_point, bucket_name, prefix):
  obsClient = ObsClient(
    access_key_id=access_key,
    secret_access_key=secret_key,
    server=end_point,
    long_conn_mode=True
  )
  files = list_obs_files(obsClient, bucket_name, prefix)
  numOfFiles = len(files)
  print(numOfFiles)
  num = 0
  for file in files:
    num = num + 1
    # obsClient.l
    obsClient.getObject(bucket_name, file, downloadPath='/tmp/carbonbinary/' + file)
    # resp.body.buffer
    if (0 == num % (numOfFiles / 10)):
      print(str(num) + ":" + file)

  obsClient.close()


def readCarbon(path):
  reader = CarbonReader() \
    .builder() \
    .withBatch(1000) \
    .withFolder(path) \
    .build()

  num = 0
  build = time.time()
  print("build time:" + str(build - start))
  while (reader.hasNext()):
    rows = reader.readNextBatchRow()

    for row in rows:
      num = num + 1
      if (0 == (num % 1000)):
        print(num)
      for column in row:
        column
  print(num)
  assert 30 == num
  reader.close()
  end = time.time()
  print("total time:" + str(end - start))
  print("Finish")


def main():
  print("Start")
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

  jnius_config.set_options('-Xms14g')
  read_obs_files(args.access_key, args.secret_key, args.end_point,
                 'sdk', 'binary')

  readCarbon("/tmp/carbonbinary/")


if __name__ == '__main__':
  start = time.time()
  main()
  end = time.time()
  print("total time is " + str(end - start))
