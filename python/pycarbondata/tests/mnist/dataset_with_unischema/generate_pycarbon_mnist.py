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


"""
This utility converts the MNIST standard dataset (http://yann.lecun.com/exdb/mnist/) into
a pycarbon dataset (Carbon format).  The resulting dataset can then be used by main.py
to demonstrate pycarbon usage with pytorch.

The script can run locally (use '--master=local[*]' command line argument), or submitted to a spark cluster.

Schema defined in examples.mnist.schema.MnistSchema will be used.

NOTE: MNIST train and test data will be downloaded automatically.
"""

import argparse

import jnius_config

import numpy as np
import os
import shutil
import tempfile
from six.moves.urllib.parse import urlparse

from pyspark.sql import SparkSession

from pycarbon.tests.mnist.dataset_with_unischema import DEFAULT_MNIST_DATA_PATH
from pycarbon.tests import DEFAULT_CARBONSDK_PATH
from pycarbon.tests.mnist.dataset_with_unischema.schema import MnistSchema
from petastorm.unischema import dict_to_spark_row

from pycarbon.core.carbon_dataset_metadata import materialize_dataset_carbon


def _arg_parser():
  parser = argparse.ArgumentParser(description=__doc__, add_help=True,
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('-d', '--download-dir', type=str, required=False, default=None,
                      help='Directory to where the MNIST data will be downloaded; '
                           'default to a tempdir that gets wiped after generation.')
  parser.add_argument('-o', '--output-url', type=str, required=False,
                      default='file://{}'.format(DEFAULT_MNIST_DATA_PATH),
                      help='hdfs://... or file:/// url where the carbon dataset will be written to.')
  parser.add_argument('-m', '--master', type=str, required=False, default='local[*]',
                      help='Spark master; default is local[*] to run locally.')
  parser.add_argument('-pp', '--pyspark-python', type=str, default=None,
                      help='pyspark python env variable')
  parser.add_argument('-pdp', '--pyspark-driver-python', type=str, default=None,
                      help='pyspark driver python env variable')
  parser.add_argument('-c', '--carbon-sdk-path', type=str, default=DEFAULT_CARBONSDK_PATH,
                      help='carbon sdk path')

  return parser


def download_mnist_data(download_dir, train=True):
  """
  Downloads the dataset files and returns the torch Dataset object, which
  represents the data as an array of (img, label) pairs.

  Each image is a PIL.Image of black-and-white 28x28 pixels.
  Each label is a long integer representing the digit 0..9.
  """
  # This is the only function requiring torch in this module.

  from torchvision import datasets

  return datasets.MNIST('{}/{}'.format(download_dir, 'data'), train=train, download=True)


def mnist_data_to_pycarbon_dataset(download_dir, output_url, spark_master=None, carbon_files_count=1,
                                   mnist_data=None):
  """Converts a directory with MNIST data into a pycarbon dataset.

  Data files are as specified in http://yann.lecun.com/exdb/mnist/:
      * train-images-idx3-ubyte.gz:  training set images (9912422 bytes)
      * train-labels-idx1-ubyte.gz:  training set labels (28881 bytes)
      * t10k-images-idx3-ubyte.gz:   test set images (1648877 bytes)
      * t10k-labels-idx1-ubyte.gz:   test set labels (4542 bytes)

  The images and labels and stored in the IDX file format for vectors and multidimensional matrices of
  various numerical types, as defined in the same URL.

  :param download_dir: the path to where the MNIST data will be downloaded.
  :param output_url: the location where your dataset will be written to. Should be a url: either
    file://... or hdfs://...
  :param spark_master: A master parameter used by spark session builder. Use default value (None) to use system
    environment configured spark cluster. Use 'local[*]' to run on a local box.
  :param mnist_data: A dictionary of MNIST data, with name of dataset as key, and the dataset object as value;
    if None is suplied, download it.
  :return: None
  """
  session_builder = SparkSession \
    .builder \
    .appName('MNIST Dataset Creation')
  if spark_master:
    session_builder.master(spark_master)

  spark = session_builder.getOrCreate()

  # Get training and test data
  if mnist_data is None:
    mnist_data = {
      'train': download_mnist_data(download_dir, train=True),
      'test': download_mnist_data(download_dir, train=False)
    }

  url_path = urlparse(output_url)
  if os.path.exists(url_path.path):
    shutil.rmtree(url_path.path)

  # The MNIST data is small enough to do everything here in Python
  for dset, data in mnist_data.items():
    dset_output_url = '{}/{}'.format(output_url, dset)
    with materialize_dataset_carbon(spark, dset_output_url, MnistSchema):
      # List of [(idx, image, digit), ...]
      # where image is shaped as a 28x28 numpy matrix
      idx_image_digit_list = map(lambda idx_image_digit: {
        MnistSchema.idx.name: idx_image_digit[0],
        MnistSchema.digit.name: idx_image_digit[1][1],
        MnistSchema.image.name: np.array(list(idx_image_digit[1][0].getdata()), dtype=np.uint8).reshape(28, 28)
      }, enumerate(data))

      # Convert to pyspark.sql.Row
      sql_rows = map(lambda r: dict_to_spark_row(MnistSchema, r), idx_image_digit_list)

      # Write out the result
      spark.createDataFrame(sql_rows, MnistSchema.as_spark_schema()) \
        .coalesce(carbon_files_count) \
        .write \
        .save(path=dset_output_url, format='carbon')


if __name__ == '__main__':
  args = _arg_parser().parse_args()
  if args.download_dir is None:
    # Make a temp dir that we'll clean up afterward
    download_dir = tempfile.mkdtemp()
  else:
    download_dir = args.download_dir

  jnius_config.set_classpath(args.carbon_sdk_path)

  if args.pyspark_python is not None and args.pyspark_driver_python is not None:
    os.environ['PYSPARK_PYTHON'] = args.pyspark_python
    os.environ['PYSPARK_DRIVER_PYTHON'] = args.pyspark_driver_python
  elif 'PYSPARK_PYTHON' in os.environ.keys() and 'PYSPARK_DRIVER_PYTHON' in os.environ.keys():
    pass
  else:
    raise ValueError("please set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON variables, "
                     "using cmd line -pp PYSPARK_PYTHON_PATH -pdp PYSPARK_DRIVER_PYTHON_PATH, "
                     "or set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON in system env")

  mnist_data_to_pycarbon_dataset(download_dir, args.output_url)

  if args.download_dir is None:
    if os.path.exists(download_dir):
      shutil.rmtree(download_dir)
