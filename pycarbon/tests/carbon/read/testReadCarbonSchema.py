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

from pycarbon.pysdk.CarbonSchemaReader import CarbonSchemaReader
from pycarbon.tests import DEFAULT_CARBONSDK_PATH, DATA_PATH

import unittest


class ReadCarbonSchemaTest(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    import jnius_config

    jnius_config.set_classpath(DEFAULT_CARBONSDK_PATH)

    print("TestCase  start running ")

  def test_1_run_read_carbon_from_folder(self):
    print("Start")
    carbonSchemaReader = CarbonSchemaReader()
    schema = carbonSchemaReader.readSchema(DATA_PATH + "/sub1")
    print(schema.getFieldsLength())
    assert 5 == schema.getFieldsLength()
    for each in schema.getFields():
      print(each.getFieldName(), end="\t")
      print(each.getDataType().toString(), end="\t")
      print(each.getSchemaOrdinal())

  def test_2_run_read_carbon_from_folder_check(self):
    print("Start")
    carbonSchemaReader = CarbonSchemaReader()
    schema = carbonSchemaReader.readSchema(getAsBuffer=False, path=DATA_PATH + "/sub2", validateSchema=True)
    print(schema.getFieldsLength())
    assert 5 == schema.getFieldsLength()
    for each in schema.getFields():
      print(each.getFieldName(), end="\t")
      print(each.getDataType().toString(), end="\t")
      print(each.getSchemaOrdinal())

  def test_3_run_read_carbon_from_index_file(self):
    print("Start")
    carbonSchemaReader = CarbonSchemaReader()
    schema = carbonSchemaReader.readSchema(
      DATA_PATH + "/sub1/1196034485149392_batchno0-0-null-1196033673787967.carbonindex")
    print(schema.getFieldsLength())
    assert 5 == schema.getFieldsLength()
    for each in schema.getFields():
      print(each.getFieldName(), end="\t")
      print(each.getDataType().toString(), end="\t")
      print(each.getSchemaOrdinal())

  def test_4_run_read_carbon_from_data_file(self):
    print("Start")
    carbonSchemaReader = CarbonSchemaReader()
    schema = carbonSchemaReader.readSchema(
      DATA_PATH + "/sub1/part-0-1196034485149392_batchno0-0-null-1196033673787967.carbondata")
    print(schema.getFieldsLength())
    assert 5 == schema.getFieldsLength()
    for each in schema.getFields():
      print(each.getFieldName(), end="\t")
      print(each.getDataType().toString(), end="\t")
      print(each.getSchemaOrdinal())
