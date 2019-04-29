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

from pycarbon.tests.hello_world.dataset_with_normal_schema.generate_dataset_carbon import generate_dataset_with_normal_schema
from pycarbon.tests.hello_world.dataset_with_normal_schema.python_hello_world_carbon import python_hello_world
from pycarbon.tests.hello_world.dataset_with_normal_schema.tensorflow_hello_world_carbon import tensorflow_hello_world
from petastorm.tests.conftest import SyntheticDataset

from pycarbon.core.Constants import LOCAL_FILE_PREFIX
from pycarbon.core.carbon_reader import make_batch_carbon_reader

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
def dataset(tmpdir_factory):
  path = tmpdir_factory.mktemp("data").strpath
  url = LOCAL_FILE_PREFIX + path

  generate_dataset_with_normal_schema(url)

  dataset = SyntheticDataset(url=url, path=path, data=None)

  # Generate a dataset
  assert os.path.exists(os.path.join(path, '_SUCCESS'))

  return dataset


def test_generate(dataset):
  # Read from it using a plain reader
  with make_reader(dataset.url) as reader:
    all_samples = list(reader)
  assert all_samples

def test_python_hello_world_external_dataset_example(dataset):
  python_hello_world(dataset.url)


def test_tensorflow_hello_world_external_dataset_example(dataset):
  tensorflow_hello_world(dataset.url)
