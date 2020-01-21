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
from pycarbon.tests import DEFAULT_CARBONSDK_PATH


MOCK_IMAGE_SIZE = (28, 28)
MOCK_IMAGE_3DIM_SIZE = (28, 28, 1)
SMALL_MOCK_IMAGE_COUNT = {
  'train': 30,
  'test': 5
}
LARGE_MOCK_IMAGE_COUNT = {
  'train': 600,
  'test': 100
}

class MockDataObj(object):
  """ Wraps a mock image array and provide a needed getdata() interface function. """

  def __init__(self, a):
    self.a = a

  def getdata(self):
    return self.a


def _mock_mnist_data(mock_spec):
  """
  Creates a mock data dictionary with train and test sets, each containing 5 mock pairs:

      ``(random images, random digit)``.
  """
  bogus_data = {
    'train': [],
    'test': []
  }

  for dset, data in bogus_data.items():
    for _ in range(mock_spec[dset]):
      pair = (MockDataObj(np.random.randint(0, 255, size=MOCK_IMAGE_SIZE, dtype=np.uint8)),
              np.random.randint(0, 9))
      data.append(pair)

  return bogus_data


@pytest.fixture(scope="session")
def small_mock_mnist_data():
  return _mock_mnist_data(SMALL_MOCK_IMAGE_COUNT)


@pytest.fixture(scope="session")
def large_mock_mnist_data():
  return _mock_mnist_data(LARGE_MOCK_IMAGE_COUNT)
