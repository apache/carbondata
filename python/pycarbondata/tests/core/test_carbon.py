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


import pytest

from pycarbon.core.Constants import LOCAL_FILE_PREFIX
from pycarbon.core.carbon import CarbonDataset
from pycarbon.core.carbon import CarbonDatasetPiece

from pycarbon.sdk.ArrowCarbonReader import ArrowCarbonReader
from pycarbon.sdk.Configuration import Configuration
from pycarbon.sdk.CarbonSchemaReader import CarbonSchemaReader

import os
import jnius_config

from pycarbon.tests import proxy

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


def test_invalid_carbondataset_obs_parameters(carbon_obs_dataset):
  with pytest.raises(ValueError):
    CarbonDataset(carbon_obs_dataset.url)

  with pytest.raises(ValueError):
    CarbonDataset(carbon_obs_dataset.url,
                  key=pytest.config.getoption("--access_key"),
                  secret=pytest.config.getoption("--secret_key"),
                  endpoint=pytest.config.getoption("--end_point"),
                  proxy=proxy)

  with pytest.raises(ValueError):
    CarbonDataset(carbon_obs_dataset.url,
                  key=pytest.config.getoption("--access_key"),
                  secret=pytest.config.getoption("--secret_key"),
                  endpoint=pytest.config.getoption("--end_point"),
                  proxy_port="8080")


def test_create_carbondataset_obs(carbon_obs_dataset):
  carbondataset_1 = CarbonDataset(carbon_obs_dataset.url,
                                  key=pytest.config.getoption("--access_key"),
                                  secret=pytest.config.getoption("--secret_key"),
                                  endpoint=pytest.config.getoption("--end_point"))

  carbondataset_2 = CarbonDataset(carbon_obs_dataset.url,
                                  key=pytest.config.getoption("--access_key"),
                                  secret=pytest.config.getoption("--secret_key"),
                                  endpoint=pytest.config.getoption("--end_point"),
                                  proxy=proxy,
                                  proxy_port="8080")

  assert len(carbondataset_1.pieces) == len(carbondataset_2.pieces)
  assert carbondataset_1.pieces


def test_create_carbondataset_local(carbon_synthetic_dataset):
  carbondataset = CarbonDataset(carbon_synthetic_dataset.url)
  assert len(carbondataset.pieces) == 2


def test_invalid_carbondatasetpiece_obs_parameters(carbon_obs_dataset):
  key = pytest.config.getoption("--access_key")
  secret = pytest.config.getoption("--secret_key")
  endpoint = pytest.config.getoption("--end_point")

  carbon_splits = ArrowCarbonReader().builder(carbon_obs_dataset.url)\
    .withHadoopConf("fs.s3a.access.key", key)\
    .withHadoopConf("fs.s3a.secret.key", secret)\
    .withHadoopConf("fs.s3a.endpoint", endpoint)\
    .getSplits(True)

  configuration = Configuration()
  configuration.set("fs.s3a.access.key", key)
  configuration.set("fs.s3a.secret.key", secret)
  configuration.set("fs.s3a.endpoint", endpoint)

  assert carbon_splits

  carbon_schema = CarbonSchemaReader().readSchema(carbon_obs_dataset.url, configuration.conf)

  with pytest.raises(ValueError):
    CarbonDatasetPiece(carbon_obs_dataset.url, carbon_schema, carbon_splits[0])

  with pytest.raises(ValueError):
    CarbonDatasetPiece(carbon_obs_dataset.url, carbon_schema, carbon_splits[0],
                       key=key, secret=secret, endpoint=endpoint,
                       proxy=proxy)

  with pytest.raises(ValueError):
    CarbonDatasetPiece(carbon_obs_dataset.url, carbon_schema, carbon_splits[0],
                       key=key, secret=secret, endpoint=endpoint,
                       proxy_port="8080")


def test_create_carbondatasetpiece_obs(carbon_obs_dataset):
  key = pytest.config.getoption("--access_key")
  secret = pytest.config.getoption("--secret_key")
  endpoint = pytest.config.getoption("--end_point")

  carbon_splits = ArrowCarbonReader().builder(carbon_obs_dataset.url)\
    .withHadoopConf("fs.s3a.access.key", key)\
    .withHadoopConf("fs.s3a.secret.key", secret)\
    .withHadoopConf("fs.s3a.endpoint", endpoint)\
    .getSplits(True)

  configuration = Configuration()
  configuration.set("fs.s3a.access.key", key)
  configuration.set("fs.s3a.secret.key", secret)
  configuration.set("fs.s3a.endpoint", endpoint)

  assert carbon_splits

  carbon_schema = CarbonSchemaReader().readSchema(carbon_obs_dataset.url, configuration.conf)

  carbondatasetpiece_1 = CarbonDatasetPiece(carbon_obs_dataset.url, carbon_schema, carbon_splits[0],
                                            key=key, secret=secret, endpoint=endpoint)

  carbondatasetpiece_2 = CarbonDatasetPiece(carbon_obs_dataset.url, carbon_schema, carbon_splits[0],
                                            key=key, secret=secret, endpoint=endpoint,
                                            proxy=proxy,
                                            proxy_port="8080")

  num_rows_1 = len(carbondatasetpiece_1.read_all(columns=None))
  num_rows_2 = len(carbondatasetpiece_2.read_all(columns=None))

  assert num_rows_1 != 0
  assert num_rows_1 == num_rows_2


def test_carbondataset_dataset_url_not_exist(carbon_obs_dataset):
  # local
  with pytest.raises(Exception):
    CarbonDataset(LOCAL_FILE_PREFIX + "/not_exist_dir")

  # obs
  with pytest.raises(Exception):
    CarbonDataset(carbon_obs_dataset.not_exist_url,
                  key=pytest.config.getoption("--access_key"),
                  secret=pytest.config.getoption("--secret_key"),
                  endpoint=pytest.config.getoption("--end_point"))
