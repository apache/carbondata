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


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import os
import shutil
import tempfile
import jnius_config
from six.moves.urllib.parse import urlparse

import tensorflow as tf

from tensorflow.contrib.learn.python.learn.datasets import mnist

from pycarbon.tests.mnist.dataset_with_normal_schema import DEFAULT_MNIST_DATA_PATH
from pycarbon.tests import DEFAULT_CARBONSDK_PATH

from pyspark.sql import SparkSession, Row


def _arg_parser():
  parser = argparse.ArgumentParser(description=__doc__, add_help=True,
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('-d', '--download-dir', type=str, required=False, default=None,
                      help='Directory to where the MNIST data will be downloaded; '
                           'default to a tempdir that gets wiped after generation.')
  parser.add_argument('-v', '--validation-size', type=int, required=False, default=0,
                      help='validation_size, default is 0 ')
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


def main(download_dir, validation_size, output_url, carbon_files_count=1):
  # Get the data
  data_sets = mnist.read_data_sets(download_dir,
                                   dtype=tf.uint8,
                                   reshape=False,
                                   validation_size=validation_size,
                                   source_url="http://yann.lecun.com/exdb/mnist/")
  url_path = urlparse(output_url)
  if os.path.exists(url_path.path):
    shutil.rmtree(url_path.path)

  spark = SparkSession.builder \
    .master('local') \
    .getOrCreate()

  mnist_data = {
    'train': data_sets.train,
    'test': data_sets.test
  }

  for dset, data in mnist_data.items():
    output_dir = os.path.join(output_url, dset)

    images = data.images
    labels = data.labels
    num_examples = data.num_examples

    if images.shape[0] != num_examples:
      raise ValueError('Image size %d does not match label size %d.' %
                       (images.shape[0], num_examples))

    rows = map(lambda x: Row(idx=x, digit=(int(labels[x])), image=bytearray(images[x].tobytes())), range(num_examples))

    spark.createDataFrame(rows) \
      .coalesce(carbon_files_count) \
      .write \
      .save(path=output_dir, format='carbon')


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

  main(download_dir=download_dir, validation_size=args.validation_size, output_url=args.output_url)

  if args.download_dir is None:
    if os.path.exists(download_dir):
      shutil.rmtree(download_dir)
