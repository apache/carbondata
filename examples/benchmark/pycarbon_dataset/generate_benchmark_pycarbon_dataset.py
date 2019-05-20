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

"""
This is part of a minimal example of how to use pycarbon to read a dataset not created
with pycarbon. Generates a sample dataset from random data.
"""

import os
import argparse
import numpy as np
import jnius_config

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

from petastorm.codecs import ScalarCodec
from petastorm.unischema import dict_to_spark_row, Unischema, UnischemaField

from pycarbon.etl.carbon_dataset_metadata import materialize_dataset_carbon

from examples import DEFAULT_CARBONSDK_PATH

ROW_COUNT = 100

BenchmarkSchema = Unischema('BenchmarkSchema', [
  UnischemaField('id', np.int_, (), ScalarCodec(IntegerType()), False),
  UnischemaField('value1', np.int_, (), ScalarCodec(IntegerType()), False),
  UnischemaField('value2', np.int_, (), ScalarCodec(IntegerType()), False),
])


def row_generator(x):
  """Returns a single entry in the generated dataset. Return a bunch of random values as an example."""
  return {'id': x,
          'value1': np.random.randint(-255, 255),
          'value2': np.random.randint(-255, 255)}


def generate_benchmark_dataset(output_url='file:///tmp/benchmark_dataset'):
  # """Creates an example dataset at output_url in Carbon format"""
  blocklet_size_mb = 256

  spark = SparkSession.builder.config('spark.driver.memory', '2g').master('local[2]').getOrCreate()
  sc = spark.sparkContext

  rows_count = ROW_COUNT

  with materialize_dataset_carbon(spark, output_url, BenchmarkSchema, blocklet_size_mb):
    rows_rdd = sc.parallelize(range(rows_count)) \
      .map(row_generator) \
      .map(lambda x: dict_to_spark_row(BenchmarkSchema, x))

    spark.createDataFrame(rows_rdd, BenchmarkSchema.as_spark_schema()) \
      .coalesce(10) \
      .write \
      .mode('overwrite') \
      .save(path=output_url, format='carbon')


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Pycarbon Benchmark Example')
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

  generate_benchmark_dataset()
