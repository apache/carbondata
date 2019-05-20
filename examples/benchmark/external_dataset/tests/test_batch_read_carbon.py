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

from examples.benchmark.external_dataset import carbon_batch_read_from_local
from examples.benchmark.external_dataset import carbon_batch_read_with_projection
from examples.benchmark.external_dataset import carbon_batch_read_with_shuffle
from examples.benchmark.external_dataset import generate_benchmark_external_dataset

from examples.benchmark.external_dataset.generate_benchmark_external_dataset import ROW_COUNT

import os
import pytest
import jnius_config

jnius_config.set_classpath(pytest.config.getoption("--carbon-sdk-path"))

jnius_config.add_options('-Xrs', '-Xmx6g')

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


generate_benchmark_external_dataset.generate_benchmark_dataset()


def test_carbon_batch_read():
    num_1 = carbon_batch_read_from_local.just_read_batch()
    num_2 = carbon_batch_read_from_local.just_read_batch()

    assert num_1 == num_2
    assert num_1 == ROW_COUNT
    assert num_2 == ROW_COUNT


def test_carbon_batch_read_with_projection():
    num_1 = carbon_batch_read_with_projection.just_read_batch()
    num_2 = carbon_batch_read_with_projection.just_read_batch()

    assert num_1 == num_2
    assert num_1 == ROW_COUNT
    assert num_2 == ROW_COUNT


def test_carbon_batch_read_with_shuffle():
    list_1 = carbon_batch_read_with_shuffle.just_read_batch()
    list_2 = carbon_batch_read_with_shuffle.just_read_batch()

    assert len(list_1)
    assert len(list_2)
    assert list_1 != list_2
