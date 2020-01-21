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

from pycarbon import make_carbon_reader, make_batch_carbon_reader
from pycarbon.core.carbon_local_memory_cache import LocalMemoryCache

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


def test_invalid_cache_parameter(carbon_synthetic_dataset, carbon_scalar_dataset):
  with make_carbon_reader(carbon_synthetic_dataset.url,
                          cache_type='memory-cache',
                          shuffle_row_drop_partitions=5) as reader:
    with pytest.raises(RuntimeError):
      next(reader)

  with make_batch_carbon_reader(carbon_scalar_dataset.url,
                                cache_type='memory-cache',
                                shuffle_row_drop_partitions=5) as reader:
    with pytest.raises(RuntimeError):
      next(reader)


def test_simple_scalar_cache():
  """Testing trivial NullCache: should trigger value generating function on each run"""
  cache = LocalMemoryCache(100)
  assert 42 == cache.get('some_key', lambda: 42)
  assert 42 == cache.get('some_key', lambda: 43)


def test_cache_cleanup():
  cache = LocalMemoryCache(100)
  assert 42 == cache.get('some_key', lambda: 42)
  cache.cleanup()

  assert 0 == cache.size()
