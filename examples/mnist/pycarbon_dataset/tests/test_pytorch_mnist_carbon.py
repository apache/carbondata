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

import logging
import os

import torch

import pytest

import examples.mnist.pycarbon_dataset.pytorch_example_carbon as pytorch_example
import examples.mnist.pycarbon_dataset.pytorch_example_carbon_unified_api as pytorch_example_unified
from examples.mnist.pycarbon_dataset.generate_pycarbon_mnist import mnist_data_to_pycarbon_dataset, download_mnist_data
from examples.mnist.pycarbon_dataset.tests.conftest import SMALL_MOCK_IMAGE_COUNT

from petastorm import TransformSpec

from pycarbon.core.carbon_reader import make_carbon_reader
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
                   "using cmd line "
                   "--pyspark-python=PYSPARK_PYTHON_PATH --pyspark-driver-python=PYSPARK_DRIVER_PYTHON_PATH "
                   "or set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON in system env")


logging.basicConfig(level=logging.INFO)


# Set test image sizes and number of mock nouns/variants

@pytest.fixture(scope="session")
def generate_mnist_dataset(small_mock_mnist_data, tmpdir_factory):
  # Using carbon_files_count to speed up the test
  path = tmpdir_factory.mktemp('data').strpath
  dataset_url = 'file://{}'.format(path)
  mnist_data_to_pycarbon_dataset(path, dataset_url, mnist_data=small_mock_mnist_data,
                                 spark_master='local[1]', carbon_files_count=1)
  return path


def test_full_pytorch_example(large_mock_mnist_data, tmpdir):
  # First, generate mock dataset
  dataset_url = 'file://{}'.format(tmpdir)
  mnist_data_to_pycarbon_dataset(tmpdir, dataset_url, mnist_data=large_mock_mnist_data,
                                 spark_master='local[1]', carbon_files_count=1)

  # Next, run a round of training using the pytorce adapting data loader
  from petastorm.pytorch import DataLoader

  torch.manual_seed(1)
  device = torch.device('cpu')
  model = pytorch_example.Net().to(device)
  optimizer = torch.optim.SGD(model.parameters(), lr=0.01, momentum=0.5)

  transform = TransformSpec(pytorch_example._transform_row, removed_fields=['idx'])

  with DataLoader(make_carbon_reader('{}/train'.format(dataset_url), reader_pool_type='thread', num_epochs=1,
                                     transform_spec=transform),
                  batch_size=32) as train_loader:
    pytorch_example.train(model, device, train_loader, 10, optimizer, 1)

  with DataLoader(make_carbon_reader('{}/test'.format(dataset_url), reader_pool_type='thread', num_epochs=1,
                                     transform_spec=transform),
                  batch_size=100) as test_loader:
    pytorch_example.evaluation(model, device, test_loader)


def test_full_pytorch_example_unified(large_mock_mnist_data, tmpdir):
  # First, generate mock dataset
  dataset_url = 'file://{}'.format(tmpdir)
  mnist_data_to_pycarbon_dataset(tmpdir, dataset_url, mnist_data=large_mock_mnist_data,
                                 spark_master='local[1]', carbon_files_count=1)

  # Next, run a round of training using the pytorce adapting data loader
  from pycarbon.reader import make_data_loader

  torch.manual_seed(1)
  device = torch.device('cpu')
  model = pytorch_example.Net().to(device)
  optimizer = torch.optim.SGD(model.parameters(), lr=0.01, momentum=0.5)

  transform = TransformSpec(pytorch_example._transform_row, removed_fields=['idx'])

  with make_data_loader(make_reader('{}/train'.format(dataset_url), is_batch=False,
                                    reader_pool_type='thread', num_epochs=1,
                                    transform_spec=transform),
                        batch_size=32) as train_loader:
    pytorch_example_unified.train(model, device, train_loader, 10, optimizer, 1)

  with make_data_loader(make_reader('{}/test'.format(dataset_url), is_batch=False,
                                    reader_pool_type='thread', num_epochs=1,
                                    transform_spec=transform),
                        batch_size=100) as test_loader:
    pytorch_example_unified.evaluation(model, device, test_loader)


def test_mnist_download(tmpdir):
  """ Demonstrates that MNIST download works, using only the 'test' data. Assumes data does not change often. """
  o = download_mnist_data(tmpdir, train=False)
  assert 10000 == len(o)
  assert o[0][1] == 7
  assert o[len(o) - 1][1] == 6


def test_generate_mnist_dataset(generate_mnist_dataset):
  train_path = os.path.join(generate_mnist_dataset, 'train')
  assert os.path.exists(train_path)
  assert os.path.exists(os.path.join(train_path, '_common_metadata'))

  test_path = os.path.join(generate_mnist_dataset, 'test')
  assert os.path.exists(test_path)
  assert os.path.exists(os.path.join(test_path, '_common_metadata'))


def test_read_mnist_dataset(generate_mnist_dataset):
  # Verify both datasets via a reader
  for dset in SMALL_MOCK_IMAGE_COUNT.keys():
    with make_carbon_reader('file://{}/{}'.format(generate_mnist_dataset, dset),
                            reader_pool_type='thread', num_epochs=1) as reader:
      assert sum(1 for _ in reader) == SMALL_MOCK_IMAGE_COUNT[dset]


def test_unified_read_mnist_dataset(generate_mnist_dataset):
  # Verify both datasets via a reader
  for dset in SMALL_MOCK_IMAGE_COUNT.keys():
    with make_reader('file://{}/{}'.format(generate_mnist_dataset, dset), is_batch=False,
                     reader_pool_type='thread', num_epochs=1) as reader:
      assert sum(1 for _ in reader) == SMALL_MOCK_IMAGE_COUNT[dset]
