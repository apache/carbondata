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


import numpy as np
import pytest

from pycarbon.core.carbon_reader import make_batch_carbon_reader

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
_TP = [
  lambda url, **kwargs: make_batch_carbon_reader(url, reader_pool_type='thread', **kwargs),
]


def _check_simple_reader(reader, expected_data):
  # Read a bunch of entries from the dataset and compare the data to reference
  expected_field_names = expected_data[0].keys()
  count = 0
  for row in reader:
    actual = row._asdict()

    # Compare value of each entry in the batch
    for i, id_value in enumerate(actual['id']):
      expected = next(d for d in expected_data if d['id'] == id_value)
      for field in expected_field_names:
        expected_value = expected[field]
        actual_value = actual[field][i, ...]
        np.testing.assert_equal(actual_value, expected_value)

    count += len(actual['id'])

  assert count == len(expected_data)


@pytest.mark.parametrize('reader_factory', _TP)
def test_simple_read(carbon_scalar_dataset, reader_factory):
  """Just a bunch of read and compares of all values to the expected values using the different reader pools"""
  with reader_factory(carbon_scalar_dataset.url) as reader:
    _check_simple_reader(reader, carbon_scalar_dataset.data)


@pytest.mark.parametrize('reader_factory', _TP)
def test_specify_columns_to_read(carbon_scalar_dataset, reader_factory):
  """Just a bunch of read and compares of all values to the expected values using the different reader pools"""
  with reader_factory(carbon_scalar_dataset.url, schema_fields=['id', 'float.*$']) as reader:
    sample = next(reader)
    assert set(sample._asdict().keys()) == {'id', 'float64'}
    assert sample.float64.size > 0


@pytest.mark.parametrize('reader_factory', _TP)
def test_many_columns_non_unischema_dataset(carbon_many_columns_non_unischema_dataset, reader_factory):
  """Check if we can read a dataset with huge number of columns (1000 in this case)"""
  with reader_factory(carbon_many_columns_non_unischema_dataset.url) as reader:
    sample = next(reader)
    assert set(sample._fields) == set(carbon_many_columns_non_unischema_dataset.data[0].keys())
