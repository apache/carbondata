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


from time import sleep

import pytest

from pycarbon.core.carbon_reader import make_carbon_reader, make_batch_carbon_reader
from pycarbon.core.carbon_reader import CarbonDataReader
from pycarbon.core.carbon import CarbonDataset

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

# pylint: disable=unnecessary-lambda
READER_FACTORIES = [
  make_carbon_reader,
  lambda url, **kwargs: make_carbon_reader(url, **kwargs),
  make_batch_carbon_reader,
  lambda url, **kwargs: make_batch_carbon_reader(url, **kwargs),
]


@pytest.mark.parametrize('reader_factory', READER_FACTORIES)
def test_dataset_url_must_be_string(reader_factory):
  with pytest.raises(ValueError):
    reader_factory(None)

  with pytest.raises(ValueError):
    reader_factory(123)

  with pytest.raises(ValueError):
    reader_factory([])


def test_diagnostics_reader_v1(carbon_synthetic_dataset):
  with make_carbon_reader(carbon_synthetic_dataset.url) as reader:
    next(reader)
    diags = reader.diagnostics
    # # Hard to make a meaningful assert on the content of the diags without potentially introducing a race
    assert 'output_queue_size' in diags


def test_normalize_shuffle_partitions(carbon_synthetic_dataset):
  dataset = CarbonDataset(carbon_synthetic_dataset.path)
  row_drop_partitions = CarbonDataReader._normalize_shuffle_options(1, dataset)
  assert row_drop_partitions == 1

  row_drop_partitions = CarbonDataReader._normalize_shuffle_options(100, dataset)
  assert row_drop_partitions == 100


def test_bound_size_of_output_queue_size_reader(carbon_synthetic_dataset):
  """This test is timing sensitive so it might become flaky"""
  TIME_TO_GET_TO_STATIONARY_STATE = 0.5

  with make_carbon_reader(carbon_synthetic_dataset.url) as reader:
    next(reader)
    sleep(TIME_TO_GET_TO_STATIONARY_STATE)
    assert reader.diagnostics['output_queue_size'] > 0


@pytest.mark.parametrize('reader_factory', READER_FACTORIES)
def test_invalid_cache_type(carbon_synthetic_dataset, reader_factory):
  with pytest.raises(ValueError, match='Unknown cache_type'):
    reader_factory(carbon_synthetic_dataset.url, cache_type='bogus_cache_type')


@pytest.mark.parametrize('reader_factory', READER_FACTORIES)
def test_invalid_reader_pool_type(carbon_synthetic_dataset, reader_factory):
  with pytest.raises(ValueError, match='Unknown reader_pool_type'):
    reader_factory(carbon_synthetic_dataset.url, reader_pool_type='bogus_pool_type')


@pytest.mark.parametrize('reader_factory', READER_FACTORIES)
def test_unsupported_reader_pool_type(carbon_synthetic_dataset, reader_factory):
  with pytest.raises(NotImplementedError):
    reader_factory(carbon_synthetic_dataset.url, reader_pool_type='process')

  with pytest.raises(NotImplementedError):
    reader_factory(carbon_synthetic_dataset.url, reader_pool_type='dummy')


def test_invalid_reader_engine(carbon_synthetic_dataset):
  with pytest.raises(ValueError, match='Supported reader_engine values'):
    make_carbon_reader(carbon_synthetic_dataset.url, reader_engine='bogus reader engine')


def test_reader_engine_v2_not_supported(carbon_synthetic_dataset):
  with pytest.raises(NotImplementedError):
    make_carbon_reader(carbon_synthetic_dataset.url, reader_engine='experimental_reader_v2')


@pytest.mark.parametrize('reader_factory', READER_FACTORIES)
def test_invalid_obs_parameters(carbon_obs_dataset, reader_factory):
  with pytest.raises(ValueError):
    reader_factory(carbon_obs_dataset.url)

  with pytest.raises(ValueError):
    reader_factory(carbon_obs_dataset.wrong_url)
