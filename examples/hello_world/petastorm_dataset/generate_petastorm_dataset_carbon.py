#  Copyright (c) 2017-2018 Uber Technologies, Inc.
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
This is a minimal example of how to generate a petastorm dataset. Generates a
sample dataset with some random data.
"""
import os

import jnius_config
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

from petastorm.codecs import ScalarCodec, CompressedImageCodec, NdarrayCodec
from petastorm.etl.dataset_metadata import materialize_dataset, materialize_dataset_carbon
from petastorm.unischema import dict_to_spark_row, Unischema, UnischemaField

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

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


def generate_petastorm_dataset(output_url='file:///tmp/hello_world_dataset_carbon'):
  rowgroup_size_mb = 256

  spark = SparkSession.builder.config('spark.driver.memory', '2g').master('local[2]').getOrCreate()
  sc = spark.sparkContext

  # Wrap dataset materialization portion. Will take care of setting up spark environment variables as
  # well as save petastorm specific metadata
  rows_count = 10
  with materialize_dataset_carbon(spark, output_url, HelloWorldSchema, rowgroup_size_mb):
    rows_rdd = sc.parallelize(range(rows_count)) \
      .map(row_generator) \
      .map(lambda x: dict_to_spark_row(HelloWorldSchema, x))

    spark.createDataFrame(rows_rdd, HelloWorldSchema.as_spark_schema()) \
      .coalesce(10) \
      .write \
      .mode('overwrite') \
      .save(path=output_url, format='carbon')


if __name__ == '__main__':
  jnius_config.set_classpath(
    "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/carbondata-sdk.jar")
  generate_petastorm_dataset()
