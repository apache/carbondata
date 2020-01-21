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


import pytest

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

from petastorm.codecs import ScalarCodec
from petastorm.predicates import in_lambda
from petastorm.unischema import dict_to_spark_row, Unischema, UnischemaField

from pycarbon.core.carbon_reader import make_carbon_reader, make_batch_carbon_reader
from pycarbon.core.carbon_dataset_metadata import materialize_dataset_carbon
from pycarbon.tests.core.test_carbon_common import TestSchema

import os
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


def test_invalid_carbon_reader_predicate_parameters(carbon_synthetic_dataset):
  with make_carbon_reader(carbon_synthetic_dataset.url,
                          cache_type="memory-cache",
                          predicate=in_lambda(['id2'], lambda id2: True)) as reader:
    with pytest.raises(RuntimeError):
      next(reader)

  with make_carbon_reader(carbon_synthetic_dataset.url,
                          predicate=in_lambda([], lambda x: False)) as reader:
    with pytest.raises(ValueError):
      next(reader)

  with make_carbon_reader(carbon_synthetic_dataset.url,
                          predicate=in_lambda(['not_exist_col'], lambda x: False)) as reader:
    with pytest.raises(ValueError):
      next(reader)


def test_invalid_batch_carbon_reader_predicate_parameters(carbon_scalar_dataset):
  with make_batch_carbon_reader(carbon_scalar_dataset.url,
                                cache_type="memory-cache",
                                predicate=in_lambda(['id2'], lambda id2: True)) as reader:
    with pytest.raises(RuntimeError):
      next(reader)

  with make_batch_carbon_reader(carbon_scalar_dataset.url,
                                predicate=in_lambda([], lambda x: False)) as reader:
    with pytest.raises(ValueError):
      next(reader)

  with make_batch_carbon_reader(carbon_scalar_dataset.url,
                                predicate=in_lambda(['not_exist_col'], lambda x: False)) as reader:
    with pytest.raises(ValueError):
      next(reader)


def test_predicate_on_single_column(carbon_synthetic_dataset):
  reader = make_carbon_reader(carbon_synthetic_dataset.url,
                              schema_fields=[TestSchema.id2],
                              predicate=in_lambda(['id2'], lambda id2: True),
                              reader_pool_type='thread')
  counter = 0
  for row in reader:
    counter += 1
    actual = dict(row._asdict())
    assert actual['id2'] < 2
  assert counter == len(carbon_synthetic_dataset.data)


def test_predicate_on_dataset(tmpdir):
  TestSchema = Unischema('TestSchema', [
    UnischemaField('id', np.int32, (), ScalarCodec(IntegerType()), False),
    UnischemaField('test_field', np.int32, (), ScalarCodec(IntegerType()), False),
  ])

  def test_row_generator(x):
    """Returns a single entry in the generated dataset."""
    return {'id': x,
            'test_field': x * x}

  blocklet_size_mb = 256
  dataset_url = "file://{0}/partitioned_test_dataset".format(tmpdir)

  spark = SparkSession.builder.config('spark.driver.memory', '2g').master('local[2]').getOrCreate()
  sc = spark.sparkContext

  rows_count = 10
  with materialize_dataset_carbon(spark, dataset_url, TestSchema, blocklet_size_mb):
    rows_rdd = sc.parallelize(range(rows_count)) \
      .map(test_row_generator) \
      .map(lambda x: dict_to_spark_row(TestSchema, x))

    spark.createDataFrame(rows_rdd, TestSchema.as_spark_schema()) \
      .write \
      .save(path=dataset_url, format='carbon')

  with make_carbon_reader(dataset_url, predicate=in_lambda(['id'], lambda x: x == 3)) as reader:
    assert next(reader).id == 3
  with make_carbon_reader(dataset_url, predicate=in_lambda(['id'], lambda x: x == '3')) as reader:
    with pytest.raises(StopIteration):
      # Predicate should have selected none, so a StopIteration should be raised.
      next(reader)
