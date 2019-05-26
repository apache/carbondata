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

from pycarbon.Constants import LOCAL_FILE_PREFIX
from pycarbon.carbon_reader import make_batch_carbon_reader
from pycarbon.tests import EXAMPLES_MANIFEST_PATH

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


def test_manifest_not_exist():
  # local
  dataset_url = LOCAL_FILE_PREFIX + "/tmp/not_exist.manifest"
  with pytest.raises(FileNotFoundError):
    make_batch_carbon_reader(dataset_url)

  # obs
  key = pytest.config.getoption("--access_key")
  secret = pytest.config.getoption("--secret_key")
  endpoint = pytest.config.getoption("--end_point")

  dataset_url = "s3a://manifest/carbon/manifestcarbon/not_exist.manifest"
  with pytest.raises(Exception):
    make_batch_carbon_reader(dataset_url, key=key, secret=secret, endpoint=endpoint)


def test_manifest_exist_but_no_content():
  # local
  dataset_url = LOCAL_FILE_PREFIX + EXAMPLES_MANIFEST_PATH + "no_content.manifest"
  with pytest.raises(Exception):
    make_batch_carbon_reader(dataset_url)

  # obs
  key = pytest.config.getoption("--access_key")
  secret = pytest.config.getoption("--secret_key")
  endpoint = pytest.config.getoption("--end_point")

  dataset_url = "s3a://manifest/carbon/manifestcarbon/no_content.manifest"
  with pytest.raises(Exception):
    make_batch_carbon_reader(dataset_url, key=key, secret=secret, endpoint=endpoint)


def test_manifest_normal_but_record_not_exist():
  # local
  dataset_url = LOCAL_FILE_PREFIX + EXAMPLES_MANIFEST_PATH + "binary1558365345315.manifest"
  with pytest.raises(Exception):
    make_batch_carbon_reader(dataset_url)

  # obs
  key = pytest.config.getoption("--access_key")
  secret = pytest.config.getoption("--secret_key")
  endpoint = pytest.config.getoption("--end_point")

  dataset_url = "s3a://manifest/carbon/manifestcarbon/obsbinary1557717977531_record_not_exist.manifest"
  with pytest.raises(Exception):
    make_batch_carbon_reader(dataset_url, key=key, secret=secret, endpoint=endpoint)


def test_manifest_normal_but_record_part_exist_1():
  # local
  dataset_url = LOCAL_FILE_PREFIX + EXAMPLES_MANIFEST_PATH + "binary1558365345315_record_part_exist_1.manifest"
  with pytest.raises(Exception):
    make_batch_carbon_reader(dataset_url)

  # TODO: fix in CI
  # obs
  # key = pytest.config.getoption("--access_key")
  # secret = pytest.config.getoption("--secret_key")
  # endpoint = pytest.config.getoption("--end_point")
  #
  # dataset_url = "s3a://manifest/carbon/manifestcarbon/obsbinary1557717977531_record_part_exist_1.manifest"
  # with pytest.raises(Exception):
  #   make_batch_carbon_reader(dataset_url, key=key, secret=secret, endpoint=endpoint)


def test_manifest_normal_but_record_part_exist_2():
  # local
  dataset_url = LOCAL_FILE_PREFIX + EXAMPLES_MANIFEST_PATH + "binary1558365345315_record_part_exist_2.manifest"
  with pytest.raises(Exception):
    make_batch_carbon_reader(dataset_url)

  # TODO: fix in CI
  # obs
  # key = pytest.config.getoption("--access_key")
  # secret = pytest.config.getoption("--secret_key")
  # endpoint = pytest.config.getoption("--end_point")
  #
  # dataset_url = "s3a://manifest/carbon/manifestcarbon/obsbinary1557717977531_record_part_exist_2.manifest"
  # with pytest.raises(Exception):
  #   make_batch_carbon_reader(dataset_url, key=key, secret=secret, endpoint=endpoint)


def test_manifest_correct():
  # local
  dataset_url = LOCAL_FILE_PREFIX + EXAMPLES_MANIFEST_PATH + "binary1558365345315_record_exist.manifest"
  for num_epochs in [1, 2, 4]:
    with make_batch_carbon_reader(dataset_url, num_epochs=num_epochs) as train_reader:
      i = 0
      for schema_view in train_reader:
        i += len(schema_view.name)

      print(i)
      assert 20 * num_epochs == i

  # obs
  key = pytest.config.getoption("--access_key")
  secret = pytest.config.getoption("--secret_key")
  endpoint = pytest.config.getoption("--end_point")

  dataset_url = "s3a://manifest/carbon/manifestcarbon/obsbinary1557717977531.manifest"

  for num_epochs in [1, 2, 4]:
    with make_batch_carbon_reader(dataset_url, key=key, secret=secret, endpoint=endpoint,
                                  num_epochs=num_epochs) as train_reader:
      i = 0
      for schema_view in train_reader:
        i += len(schema_view.name)

      print(i)
      assert 20 * num_epochs == i
