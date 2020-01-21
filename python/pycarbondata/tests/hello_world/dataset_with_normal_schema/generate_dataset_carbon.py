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
This is part of a minimal example of how to use pycarbon to read a dataset not created
with pycarbon. Generates a sample dataset from random data.
"""

import os
import argparse
import random
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType

NON_PYCARBON_SCHEMA = StructType([
  StructField("id", IntegerType(), True),
  StructField("value1", IntegerType(), True),
  StructField("value2", IntegerType(), True)
])


def row_generator(x):
  """Returns a single entry in the generated dataset. Return a bunch of random values as an example."""
  return Row(id=x, value1=random.randint(-255, 255), value2=random.randint(-255, 255))


def generate_dataset_with_normal_schema(output_url='file:///tmp/carbon_external_dataset'):
  # """Creates an example dataset at output_url in Carbon format"""
  spark = SparkSession.builder \
    .master('local') \
    .getOrCreate()
  sc = spark.sparkContext
  sc.setLogLevel('INFO')

  rows_count = 10
  rows_rdd = sc.parallelize(range(rows_count)) \
    .map(row_generator)

  spark.createDataFrame(rows_rdd) \
    .write \
    .mode('overwrite') \
    .save(path=output_url, format='carbon')


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Pycarbon Hello World Example I')
  parser.add_argument('-pp', '--pyspark-python', type=str, default=None,
                      help='pyspark python env variable')
  parser.add_argument('-pdp', '--pyspark-driver-python', type=str, default=None,
                      help='pyspark driver python env variable')

  args = parser.parse_args()

  if args.pyspark_python is not None and args.pyspark_driver_python is not None:
    os.environ['PYSPARK_PYTHON'] = args.pyspark_python
    os.environ['PYSPARK_DRIVER_PYTHON'] = args.pyspark_driver_python
  elif 'PYSPARK_PYTHON' in os.environ.keys() and 'PYSPARK_DRIVER_PYTHON' in os.environ.keys():
    pass
  else:
    raise ValueError("please set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON variables, "
                     "using cmd line -pp PYSPARK_PYTHON_PATH -pdp PYSPARK_DRIVER_PYTHON_PATH, "
                     "or set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON in system env")

  generate_dataset_with_normal_schema()
