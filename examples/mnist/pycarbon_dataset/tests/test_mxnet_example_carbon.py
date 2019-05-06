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

import os
from examples.mnist.pycarbon_dataset import mxnet_example_carbon as mxnet_example
from examples.mnist.pycarbon_dataset.generate_pycarbon_mnist import mnist_data_to_pycarbon_dataset

import pytest
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


def test_full_mxnet_example(large_mock_mnist_data, tmpdir):
  # First, generate mock dataset
  dataset_url = 'file://{}'.format(tmpdir)
  mnist_data_to_pycarbon_dataset(tmpdir, dataset_url, mnist_data=large_mock_mnist_data,
                                 spark_master='local[1]', carbon_files_count=1)

  mxnet_example.mnist_iter_test(
    dataset_path=dataset_url + "/train",
    num_epoch=1,
    batch_size=100
  )
