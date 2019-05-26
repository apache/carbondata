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

import pytest

from pycarbon.pysdk.CarbonSchemaReader import CarbonSchemaReader
from pycarbon.pysdk.Configuration import Configuration

from pycarbon.tests import LOCAL_DATA_PATH
from pycarbon.tests import S3_DATA_PATH2

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


def test_run_read_carbon_schema_from_folder():
  carbonSchemaReader = CarbonSchemaReader()
  schema = carbonSchemaReader.readSchema(LOCAL_DATA_PATH + "/sub1")

  assert 5 == schema.getFieldsLength()


def test_run_read_carbon_schema_from_folder_check():
  carbonSchemaReader = CarbonSchemaReader()
  schema = carbonSchemaReader.readSchema(getAsBuffer=False, path=LOCAL_DATA_PATH + "/sub2", validateSchema=True)

  assert 5 == schema.getFieldsLength()


def test_run_read_carbon_schema_from_index_file():
  carbonSchemaReader = CarbonSchemaReader()
  schema = carbonSchemaReader.readSchema(
    LOCAL_DATA_PATH + "/sub1/1196034485149392_batchno0-0-null-1196033673787967.carbonindex")

  assert 5 == schema.getFieldsLength()


def test_run_read_carbon_schema_from_data_file():
  carbonSchemaReader = CarbonSchemaReader()
  schema = carbonSchemaReader.readSchema(
    LOCAL_DATA_PATH + "/sub1/part-0-1196034485149392_batchno0-0-null-1196033673787967.carbondata")

  assert 5 == schema.getFieldsLength()


def test_run_read_carbon_schema_from_obs():
  configuration = Configuration()

  key = pytest.config.getoption("--access_key")
  secret = pytest.config.getoption("--secret_key")
  endpoint = pytest.config.getoption("--end_point")

  configuration.set("fs.s3a.access.key", key)
  configuration.set("fs.s3a.secret.key", secret)
  configuration.set("fs.s3a.endpoint", endpoint)

  carbonSchemaReader = CarbonSchemaReader()
  schema = carbonSchemaReader.readSchema(
    S3_DATA_PATH2, False, True, configuration.conf)

  assert 5 == schema.getFieldsLength()
