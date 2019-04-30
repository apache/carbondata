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

import pytest
import os
import shutil
import tempfile
import unittest

import cv2
import numpy as np

from examples.imagenet.generate_pycarbon_imagenet import download_nouns_mapping, \
  imagenet_directory_to_pycarbon_dataset

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


# Set test image sizes and number of mock nouns/variants
MOCK_IMAGE_SIZE = (64, 32, 3)
MOCK_NOUNS_COUNT = 5
MOCK_VARIANTS_COUNT = 3


def _mock_imagenet_dir(temp_dir):
  """Creates a mock directory with 5 noun-id directores and 3 variants of the noun images. Random images are used."""
  noun_id_to_text = dict()
  for i in range(MOCK_NOUNS_COUNT):
    # Make noun-id directory (e.g. n00000001 format)
    noun_id = 'n0000000{}'.format(i)
    noun_id_to_text[noun_id] = 'text for {}'.format(noun_id)
    noun_dir = os.path.join(temp_dir, noun_id)
    os.mkdir(noun_dir)

    # Create 3 noun image variants (e.g n00000001_0001.JPEG)
    for variant_id in range(MOCK_VARIANTS_COUNT):
      jpeg_path = os.path.join(noun_dir, '{}_000{}.JPEG'.format(noun_id, variant_id))
      dummy_image = np.random.randint(0, 255, size=MOCK_IMAGE_SIZE, dtype=np.uint8)
      cv2.imwrite(jpeg_path, dummy_image)
  return noun_id_to_text


class TestGenerate(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    cls.mock_imagenet_dir = tempfile.mkdtemp()
    cls.mock_output_dir = tempfile.mkdtemp()
    cls.noun_id_to_text = _mock_imagenet_dir(cls.mock_imagenet_dir)

  @classmethod
  def tearDownClass(cls):
    if os.path.exists(cls.mock_imagenet_dir):
      shutil.rmtree(cls.mock_imagenet_dir)
    if os.path.exists(cls.mock_output_dir):
      shutil.rmtree(cls.mock_output_dir)

  @unittest.skip('')
  def test_get_labels(self):
    a = download_nouns_mapping()
    self.assertEqual(1000, len(a))
    self.assertEqual(a['n03887697'], 'paper_towel')

  def test_generate(self):
    # Use carbon_files_count to speed up the test
    imagenet_directory_to_pycarbon_dataset(TestGenerate.mock_imagenet_dir,
                                           'file://' + TestGenerate.mock_output_dir,
                                           spark_master='local[3]', carbon_files_count=3,
                                           noun_id_to_text=TestGenerate.noun_id_to_text)


if __name__ == '__main__':
  unittest.main()
