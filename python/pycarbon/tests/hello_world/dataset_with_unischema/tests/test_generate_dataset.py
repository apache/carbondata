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


import os

import pytest

from pycarbon.tests.hello_world.dataset_with_unischema.generate_pycarbon_dataset import generate_pycarbon_dataset
from pycarbon.tests.hello_world.dataset_with_unischema.pyspark_hello_world_carbon import pyspark_hello_world
from pycarbon.tests.hello_world.dataset_with_unischema.python_hello_world_carbon import python_hello_world
from pycarbon.tests.hello_world.dataset_with_unischema.tensorflow_hello_world_carbon import tensorflow_hello_world
from petastorm.tests.conftest import SyntheticDataset

from pycarbon.core.Constants import LOCAL_FILE_PREFIX

from pycarbon.reader import make_reader

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
                   "using cmd line --pyspark-python=PYSPARK_PYTHON_PATH --pyspark-driver-python=PYSPARK_DRIVER_PYTHON_PATH, "
                   "or set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON in system env")


@pytest.fixture(scope="session")
def pycarbon_dataset(tmpdir_factory):
  path = tmpdir_factory.mktemp("data").strpath
  url = LOCAL_FILE_PREFIX + path

  generate_pycarbon_dataset(url)

  dataset = SyntheticDataset(url=url, path=path, data=None)

  # Generate a dataset
  assert os.path.exists(os.path.join(path, '_SUCCESS'))

  return dataset


def test_generate(pycarbon_dataset):
  # Read from it using a plain reader
  with make_reader(pycarbon_dataset.url) as reader:
    all_samples = list(reader)
  assert all_samples

  with make_reader(pycarbon_dataset.url, is_batch=False) as reader:
    all_samples = list(reader)
  assert all_samples


def test_pyspark_hello_world_pycarbon_dataset_example(pycarbon_dataset):
  pyspark_hello_world(pycarbon_dataset.url)


def test_python_hello_world_pycarbon_dataset_example(pycarbon_dataset):
  python_hello_world(pycarbon_dataset.url)


def test_tensorflow_hello_world_pycarbon_dataset_example(pycarbon_dataset):
  tensorflow_hello_world(pycarbon_dataset.url)
