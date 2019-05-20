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

from pycarbon.pysdk.CarbonReader import CarbonReader
import time

from pycarbon.tests import DEFAULT_CARBONSDK_PATH, DATA_PATH

import unittest


class ReadCarbonDataTest(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    import jnius_config

    jnius_config.set_classpath(DEFAULT_CARBONSDK_PATH)

    print("TestCase  start running ")

  def test_1_run_read_carbon_from_local(self):
    print("Start")
    start = time.time()

    reader = CarbonReader() \
      .builder() \
      .withBatch(780) \
      .withFolder(DATA_PATH) \
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

  def test_2_run_read_carbon_from_local_for_filter(self):
    print("Start")
    start = time.time()
    builder = CarbonReader().builder()

    reader = builder.withBatch(10) \
      .withFolder(DATA_PATH) \
      .filterEqual("name", "robot0") \
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
    assert 3 == num
    reader.close()
    end = time.time()
    print("total time:" + str(end - start))
    print("Finish")

  def test_3_run_read_carbon_from_local_for_projection(self):
    print("Start")
    start = time.time()
    builder = CarbonReader() \
      .builder()

    from jnius import autoclass
    java_list_class = autoclass('java.util.ArrayList')
    projection_list = java_list_class()
    projection_list.add("name")
    projection_list.add("age")
    projection_list.add("image1")
    projection_list.add("image2")
    projection_list.add("image3")

    reader = builder.withBatch(100) \
      .withFolder(DATA_PATH) \
      .projection(projection_list) \
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

  def test_4_run_read_carbon_by_file(self):

    start = time.time()
    reader = CarbonReader() \
      .builder() \
      .withFile(DATA_PATH + "/sub1/part-0-1196034485149392_batchno0-0-null-1196033673787967.carbondata") \
      .withBatch(1000) \
      .build()

    i = 0
    while (reader.hasNext()):
      rows = reader.readNextBatchRow()
      for row in rows:
        i = i + 1
        if 0 == i % 1000:
          print(i)
        for column in row:
          column

    print(i)
    assert 10 == i
    reader.close()

    end = time.time()
    print("all time is " + str(end - start))
    print("Finish")

  def test_5_run_read_carbon_by_file_lists(self):

    from jnius import autoclass

    java_list_class = autoclass('java.util.ArrayList')

    java_list = java_list_class()
    java_list.add(DATA_PATH + "/sub1/part-0-1196034485149392_batchno0-0-null-1196033673787967.carbondata")
    java_list.add(DATA_PATH + "/sub2/part-0-1196034758543568_batchno0-0-null-1196034721553227.carbondata")

    start = time.time()

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

    i = 0
    while (reader.hasNext()):
      rows = reader.readNextBatchRow()
      for row in rows:
        i = i + 1
        if 0 == i % 1000:
          print(i)
        for column in row:
          column

    print(i)
    assert 20 == i
    reader.close()

    end = time.time()
    print("all time is " + str(end - start))
    print("Finish")
