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

from __future__ import division

import pytest

from pycarbon.pysdk.CarbonReader import CarbonReader
from pycarbon.tests import LOCAL_DATA_PATH
from pycarbon.tests import S3_DATA_PATH, S3_DATA_PATH1, S3_DATA_PATH2

from multiprocessing.dummy import Pool as ThreadPool
from obs import ObsClient

import shutil
import os
import jnius_config

jnius_config.set_classpath(pytest.config.getoption("--carbon-sdk-path"))

if pytest.config.getoption("--pyspark-python") is not None and \
    pytest.config.getoption("--pyspark-driver-python") is not None:
  os.environ['PYSPARK_PYTHON'] = pytest.config.getoption("--pyspark-python")
  os.environ['PYSPARK_DRIVER_PYTHON'] = pytest.config.getoption("--pyspark-driver-python")
elif 'PYSPARK_PYTHON' in os.environ.keys() and 'PYSPARK_DRIVER_PYTHON' in os.environ.keys():
  pass
else:
  raise ValueError("please set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON variables, "
                   "using cmd line "
                   "--pyspark-python=PYSPARK_PYTHON_PATH --pyspark-driver-python=PYSPARK_DRIVER_PYTHON_PATH "
                   "or set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON in system env")


def test_run_read_carbon_from_local():
  reader = CarbonReader() \
    .builder() \
    .withBatch(780) \
    .withFolder(LOCAL_DATA_PATH) \
    .build()

  num = 0
  while reader.hasNext():
    rows = reader.readNextBatchRow()
    num += len(rows)

  assert 30 == num
  reader.close()


# TODO: fix bug in CI
@pytest.mark.skip("local filter and obs filter run simultaneously, raise errors")
def test_run_read_carbon_from_local_for_filter():
  reader = CarbonReader() \
    .builder() \
    .withBatch(10) \
    .withFolder(LOCAL_DATA_PATH) \
    .filterEqual("name", "robot0") \
    .build()

  num = 0
  while reader.hasNext():
    rows = reader.readNextBatchRow()
    num += len(rows)

  assert 3 == num
  reader.close()


def test_run_read_carbon_from_local_for_projection():
  from jnius import autoclass
  java_list_class = autoclass('java.util.ArrayList')
  projection_list = java_list_class()
  projection_list.add("name")
  projection_list.add("age")
  projection_list.add("image1")
  projection_list.add("image2")
  projection_list.add("image3")

  reader = CarbonReader() \
    .builder() \
    .withBatch(100) \
    .withFolder(LOCAL_DATA_PATH) \
    .projection(projection_list) \
    .build()

  num = 0
  while reader.hasNext():
    rows = reader.readNextBatchRow()
    num += len(rows)

  assert 30 == num
  reader.close()


def test_run_read_carbon_by_file():
  reader = CarbonReader() \
    .builder() \
    .withFile(LOCAL_DATA_PATH + "/sub1/part-0-1196034485149392_batchno0-0-null-1196033673787967.carbondata") \
    .withBatch(1000) \
    .build()

  num = 0
  while reader.hasNext():
    rows = reader.readNextBatchRow()
    num += len(rows)

  assert 10 == num
  reader.close()


def test_run_read_carbon_by_file_lists():
  from jnius import autoclass
  java_list_class = autoclass('java.util.ArrayList')

  java_list = java_list_class()
  java_list.add(LOCAL_DATA_PATH + "/sub1/part-0-1196034485149392_batchno0-0-null-1196033673787967.carbondata")
  java_list.add(LOCAL_DATA_PATH + "/sub2/part-0-1196034758543568_batchno0-0-null-1196034721553227.carbondata")

  projection_list = java_list_class()
  projection_list.add("name")
  projection_list.add("age")
  projection_list.add("image1")
  projection_list.add("image2")
  projection_list.add("image3")

  reader = CarbonReader() \
    .builder() \
    .withFileLists(java_list) \
    .withBatch(1000) \
    .projection(projection_list) \
    .build()

  num = 0
  while reader.hasNext():
    rows = reader.readNextBatchRow()
    num += len(rows)

  assert 20 == num
  reader.close()


def test_run_read_carbon_from_obs():
  reader = CarbonReader() \
    .builder() \
    .withBatch(1000) \
    .withFolder(S3_DATA_PATH) \
    .withHadoopConf("fs.s3a.access.key", pytest.config.getoption("--access_key")) \
    .withHadoopConf("fs.s3a.secret.key", pytest.config.getoption("--secret_key")) \
    .withHadoopConf("fs.s3a.endpoint", pytest.config.getoption("--end_point")) \
    .build()

  num = 0
  while reader.hasNext():
    rows = reader.readNextBatchRow()
    num += len(rows)

  assert 30 == num
  reader.close()


def test_download_carbon_from_obs_and_read():
  key = pytest.config.getoption("--access_key")
  secret = pytest.config.getoption("--secret_key")
  end_point = pytest.config.getoption("--end_point")

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

  def read_obs_files(key, secret, end_point, bucket_name, prefix, downloadPath):
    obsClient = ObsClient(
      access_key_id=key,
      secret_access_key=secret,
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
      obsClient.getObject(bucket_name, file, downloadPath=downloadPath + file)
      # resp.body.buffer
      if 0 == num % (numOfFiles / 10):
        print(str(num) + ":" + file)

    obsClient.close()

  downloadPath = '/tmp/carbonbinary/'

  if os.path.exists(downloadPath):
    shutil.rmtree(downloadPath)

  read_obs_files(key, secret, end_point, 'sdk', 'binary', downloadPath)

  reader = CarbonReader() \
    .builder() \
    .withBatch(1000) \
    .withFolder(downloadPath) \
    .build()

  num = 0
  while reader.hasNext():
    rows = reader.readNextBatchRow()
    num += len(rows)

  assert 30 == num
  reader.close()

  shutil.rmtree(downloadPath)


def test_run_read_carbon_from_obs_for_filter():
  from jnius import autoclass
  java_list_class = autoclass('java.util.ArrayList')
  projection_list = java_list_class()
  projection_list.add("name")

  reader = CarbonReader() \
    .builder() \
    .withBatch(780) \
    .withFolder(S3_DATA_PATH) \
    .withHadoopConf("fs.s3a.access.key", pytest.config.getoption("--access_key")) \
    .withHadoopConf("fs.s3a.secret.key", pytest.config.getoption("--secret_key")) \
    .withHadoopConf("fs.s3a.endpoint", pytest.config.getoption("--end_point")) \
    .projection(projection_list) \
    .filterEqual("name", "robot0") \
    .build()

  num = 0
  while reader.hasNext():
    rows = reader.readNextBatchRow()
    num += len(rows)

  assert 3 == num
  reader.close()


def test_run_read_carbon_from_obs_for_union():
  key = pytest.config.getoption("--access_key")
  secret = pytest.config.getoption("--secret_key")
  end_point = pytest.config.getoption("--end_point")

  def readCarbon(key, secret, end_point, path, label):
    from jnius import autoclass
    java_list_class = autoclass('java.util.ArrayList')
    projection_list = java_list_class()
    projection_list.add("name")

    reader = CarbonReader() \
      .builder() \
      .withBatch(780) \
      .withFolder(path) \
      .withHadoopConf("fs.s3a.access.key", key) \
      .withHadoopConf("fs.s3a.secret.key", secret) \
      .withHadoopConf("fs.s3a.endpoint", end_point) \
      .projection(projection_list) \
      .filterEqual("name", label) \
      .build()

    data_list = []
    while reader.hasNext():
      rows = reader.readNextBatchRow()
      for row in rows:
        data_list.append(row)

    reader.close()
    return data_list

  list_1 = readCarbon(key, secret, end_point, S3_DATA_PATH1, "robot0")
  assert 1 == len(list_1)

  list_2 = readCarbon(key, secret, end_point, S3_DATA_PATH2, "robot1")
  assert 2 == len(list_2)

  list_1.extend(list_2)
  assert 3 == len(list_1)


# TODO: to be supported
@pytest.mark.skip("read folder concurrently to be supported")
def test_read_carbon_from_local_by_folder_concurrently():
  reader = CarbonReader() \
    .builder() \
    .withFolder(LOCAL_DATA_PATH) \
    .withBatch(1000) \
    .build()

  readers = reader.splitAsArray(int(3))
  pool = ThreadPool(len(readers))

  def readLogic(carbonReader):
    i = 0
    while carbonReader.hasNext():
      rows = carbonReader.readNextBatchRow()
      i += len(rows)

    carbonReader.close()

  pool.map(readLogic, readers)
  pool.close()
