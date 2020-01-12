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
This is a minimal example of how to generate a pycarbon dataset. Generates a
sample dataset with some random data.
"""

import os
import argparse

import jnius_config

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

from petastorm.codecs import ScalarCodec, CompressedImageCodec, NdarrayCodec
from petastorm.unischema import dict_to_spark_row, Unischema, UnischemaField

from pycarbon.core.carbon_dataset_metadata import materialize_dataset_carbon

from pycarbon.tests import DEFAULT_CARBONSDK_PATH

# The schema defines how the dataset schema looks like
HelloWorldSchema = Unischema('HelloWorldSchema', [
  UnischemaField('id', np.int_, (), ScalarCodec(IntegerType()), False),
  UnischemaField('image1', np.uint8, (128, 256, 3), CompressedImageCodec('png'), False),
  UnischemaField('array_4d', np.uint8, (None, 128, 30, None), NdarrayCodec(), False),
])


def row_generator(x):
  """Returns a single entry in the generated dataset. Return a bunch of random values as an example."""
  return {'id': x,
          'image1': np.random.randint(0, 255, dtype=np.uint8, size=(128, 256, 3)),
          'array_4d': np.random.randint(0, 255, dtype=np.uint8, size=(4, 128, 30, 3))}


def generate_pycarbon_dataset(output_url='file:///tmp/carbon_pycarbon_dataset'):
  blocklet_size_mb = 256

  spark = SparkSession.builder.config('spark.driver.memory', '2g').master('local[2]').getOrCreate()
  sc = spark.sparkContext

  # Wrap dataset materialization portion. Will take care of setting up spark environment variables as
  # well as save pycarbon specific metadata
  rows_count = 10
  with materialize_dataset_carbon(spark, output_url, HelloWorldSchema, blocklet_size_mb):
    rows_rdd = sc.parallelize(range(rows_count)) \
      .map(row_generator) \
      .map(lambda x: dict_to_spark_row(HelloWorldSchema, x))

    spark.createDataFrame(rows_rdd, HelloWorldSchema.as_spark_schema()) \
      .coalesce(10) \
      .write \
      .mode('overwrite') \
      .save(path=output_url, format='carbon')


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Pycarbon Example II')
  parser.add_argument('-pp', '--pyspark-python', type=str, default=None,
                      help='pyspark python env variable')
  parser.add_argument('-pdp', '--pyspark-driver-python', type=str, default=None,
                      help='pyspark driver python env variable')
  parser.add_argument('-c', '--carbon-sdk-path', type=str, default=DEFAULT_CARBONSDK_PATH,
                      help='carbon sdk path')

  args = parser.parse_args()
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

  generate_pycarbon_dataset()
