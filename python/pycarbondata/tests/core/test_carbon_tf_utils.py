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


from __future__ import division

import datetime
from calendar import timegm
from collections import namedtuple
from contextlib import contextmanager
from decimal import Decimal

import os
import numpy as np
import pytest
import tensorflow as tf

from petastorm.ngram import NGram
from petastorm.unischema import Unischema, UnischemaField

from pycarbon.core.carbon_reader import make_carbon_reader, make_batch_carbon_reader
from pycarbon.tests.core.test_carbon_common import TestSchema
from pycarbon.core.carbon_tf_utils import _sanitize_field_tf_types, _numpy_to_tf_dtypes, \
  _schema_to_tf_dtypes, tf_tensors

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

NON_NULLABLE_FIELDS = set(TestSchema.fields.values())


@contextmanager
def _tf_session():
  with tf.Session() as sess:
    sess.run([tf.global_variables_initializer(), tf.local_variables_initializer()])

    coord = tf.train.Coordinator()
    threads = tf.train.start_queue_runners(coord=coord, start=True)

    yield sess

    coord.request_stop()
    coord.join(threads)


def test_sanitize_field_tf_types():
  expected_datetime_array = [datetime.date(1970, 1, 1), datetime.date(2015, 9, 29)]
  expected_datetime_ns_from_epoch = [timegm(dt.timetuple()) * 1000000000 for dt in expected_datetime_array]
  assert expected_datetime_ns_from_epoch[0] == 0

  sample_input_dict = {
    'int32': np.asarray([-2 ** 31, 0, 100, 2 ** 31 - 1], dtype=np.int32),
    'uint16': np.asarray([0, 2, 2 ** 16 - 1], dtype=np.uint16),
    'Decimal': Decimal(1234) / Decimal(10),
    'array_of_datetime_date': np.asarray(expected_datetime_array),
    'array_of_np_datetime_64': np.asarray(expected_datetime_array).astype(np.datetime64),
  }

  TestNamedTuple = namedtuple('TestNamedTuple', sample_input_dict.keys())
  sample_input_tuple = TestNamedTuple(**sample_input_dict)
  sanitized_tuple = _sanitize_field_tf_types(sample_input_tuple)

  np.testing.assert_equal(sanitized_tuple.int32.dtype, np.int32)
  np.testing.assert_equal(sanitized_tuple.uint16.dtype, np.int32)
  assert isinstance(sanitized_tuple.Decimal, str)

  np.testing.assert_equal(sanitized_tuple.int32, sample_input_dict['int32'])
  np.testing.assert_equal(sanitized_tuple.uint16, sample_input_dict['uint16'])
  np.testing.assert_equal(str(sanitized_tuple.Decimal), str(sample_input_dict['Decimal'].normalize()))

  np.testing.assert_equal(sanitized_tuple.array_of_datetime_date, expected_datetime_ns_from_epoch)
  np.testing.assert_equal(sanitized_tuple.array_of_np_datetime_64, expected_datetime_ns_from_epoch)


def test_invalid_sanitize_field_tf_types():
  sample_input_dict = {
    'int32': None,
  }

  TestNamedTuple = namedtuple('TestNamedTuple', sample_input_dict.keys())
  sample_input_tuple = TestNamedTuple(**sample_input_dict)
  with pytest.raises(RuntimeError):
    _sanitize_field_tf_types(sample_input_tuple)


def test_decimal_conversion():
  assert _numpy_to_tf_dtypes(Decimal) == tf.string


def test_uint16_promotion_to_int32():
  assert _numpy_to_tf_dtypes(np.uint16) == tf.int32


def test_unknown_type():
  with pytest.raises(ValueError):
    _numpy_to_tf_dtypes(np.uint64)


def test_schema_to_dtype_list():
  TestSchema = Unischema('TestSchema', [
    UnischemaField('int32', np.int32, (), None, False),
    UnischemaField('uint8', np.uint8, (), None, False),
    UnischemaField('uint16', np.uint16, (), None, False),
    UnischemaField('Decimal', Decimal, (), None, False),
  ])

  actual_tf_dtype_list = _schema_to_tf_dtypes(TestSchema)
  # Note that the order of the fields is defined by alphabetical order of keys and always sorted by Unischema
  # to avoid ambiguity
  #  [Decimal,   int32,    uint16,   uint8] <- alphabetical order
  #  [tf.string, tf.int32, tf.int32, tf.uint8]
  np.testing.assert_equal(actual_tf_dtype_list, [tf.string, tf.int32, tf.int32, tf.uint8])


def _read_from_tf_tensors(synthetic_dataset, count, shuffling_queue_capacity, min_after_dequeue, ngram,
                          workers_count=10):
  """Used by several test cases. Reads a 'count' rows using reader.

  The reader is configured without row-group shuffling and guarantees deterministic order of rows up to the
  results queue TF shuffling which is controlled by 'shuffling_queue_capacity', 'min_after_dequeue' arguments.

  The function returns a tuple with: (actual data read from the dataset, a TF tensor returned by the reader)
  """

  schema_fields = (NON_NULLABLE_FIELDS if ngram is None else ngram)

  with make_carbon_reader(schema_fields=schema_fields, dataset_url=synthetic_dataset.url, reader_pool_type='thread',
                          shuffle_blocklets=False, workers_count=workers_count) as reader:
    row_tensors = tf_tensors(reader, shuffling_queue_capacity=shuffling_queue_capacity,
                             min_after_dequeue=min_after_dequeue)

    with _tf_session() as sess:
      rows_data = [sess.run(row_tensors) for _ in range(count)]

  return rows_data, row_tensors


def _assert_all_tensors_have_shape(row_tensors):
  """Asserts that all elements in row_tensors list/tuple have static shape."""
  for column in row_tensors:
    assert column.get_shape().dims is not None


def _assert_fields_eq(actual, desired):
  if isinstance(desired, Decimal) or isinstance(actual, bytes):
    # Tensorflow returns all strings as bytes in python3. So we will need to decode it
    actual = actual.decode()
  elif isinstance(desired, np.ndarray) and desired.dtype.type == np.unicode_:
    actual = np.array([item.decode() for item in actual])

  if isinstance(desired, Decimal):
    np.testing.assert_equal(Decimal(actual), desired)
  elif issubclass(desired.dtype.type, np.datetime64):
    # tf_utils will convert timestamps to ns from epoch int64 value.
    assert desired.astype('<M8[ns]').astype(np.int64) == actual
  else:
    np.testing.assert_equal(actual, desired)


def _assert_expected_rows_data(expected_data, rows_data):
  """Asserts all elements of rows_data list of rows match reference data used to create the dataset"""
  for row_tuple in rows_data:

    # It is easier to work with dict as we will be indexing column names using strings
    row = row_tuple._asdict()

    # Find corresponding row in the reference data
    expected = next(d for d in expected_data if d['id'] == row['id'])

    # Check equivalence of all values between a checked row and a row from reference data
    for column_name, actual in row.items():
      expected_val = expected[column_name]
      _assert_fields_eq(actual, expected_val)


@pytest.mark.forked
def test_simple_read_tensorflow(carbon_synthetic_dataset):
  """Read couple of rows. Make sure all tensors have static shape sizes assigned and the data matches reference
  data"""
  with make_carbon_reader(schema_fields=NON_NULLABLE_FIELDS, dataset_url=carbon_synthetic_dataset.url) as reader:
    row_tensors = tf_tensors(reader)
    with _tf_session() as sess:
      rows_data = [sess.run(row_tensors) for _ in range(30)]

  # Make sure we have static shape info for all fields
  _assert_all_tensors_have_shape(row_tensors)
  _assert_expected_rows_data(carbon_synthetic_dataset.data, rows_data)


@pytest.mark.forked
def test_shuffling_queue(carbon_synthetic_dataset):
  """Read data without tensorflow shuffling queue and with it. Check the the order is deterministic within
  unshuffled read and is random with shuffled read"""
  unshuffled_1, _ = _read_from_tf_tensors(carbon_synthetic_dataset, 30, shuffling_queue_capacity=0, min_after_dequeue=0,
                                          ngram=None, workers_count=1)
  unshuffled_2, _ = _read_from_tf_tensors(carbon_synthetic_dataset, 30, shuffling_queue_capacity=0, min_after_dequeue=0,
                                          ngram=None, workers_count=1)

  shuffled_1, shuffled_1_row_tensors = \
    _read_from_tf_tensors(carbon_synthetic_dataset, 30, shuffling_queue_capacity=10, min_after_dequeue=9, ngram=None)
  shuffled_2, _ = \
    _read_from_tf_tensors(carbon_synthetic_dataset, 30, shuffling_queue_capacity=10, min_after_dequeue=9, ngram=None)

  # Make sure we have static shapes and the data matches reference data (important since a different code path
  # is executed within tf_tensors when shuffling is specified
  _assert_all_tensors_have_shape(shuffled_1_row_tensors)
  _assert_expected_rows_data(carbon_synthetic_dataset.data, shuffled_1)

  assert [f.id for f in unshuffled_1] == [f.id for f in unshuffled_2]
  assert [f.id for f in unshuffled_1] != [f.id for f in shuffled_2]
  assert [f.id for f in shuffled_1] != [f.id for f in shuffled_2]


@pytest.mark.forked
def test_simple_ngram_read_tensorflow(carbon_synthetic_dataset):
  """Read a single ngram. Make sure all shapes are set and the data read matches reference data"""
  fields = {
    0: [TestSchema.id],
    1: [TestSchema.id],
    2: [TestSchema.id]
  }

  # Expecting delta between ids to be 1. Setting 1.5 as upper bound
  ngram = NGram(fields=fields, delta_threshold=1.5, timestamp_field=TestSchema.id)

  ngrams, row_tensors_seq = \
    _read_from_tf_tensors(carbon_synthetic_dataset, 30, shuffling_queue_capacity=0, min_after_dequeue=0, ngram=ngram)

  for row_tensors in row_tensors_seq.values():
    _assert_all_tensors_have_shape(row_tensors)

  for one_ngram_dict in ngrams:
    _assert_expected_rows_data(carbon_synthetic_dataset.data, one_ngram_dict.values())


@pytest.mark.forked
def test_shuffling_queue_with_ngrams(carbon_synthetic_dataset):
  """Read data without tensorflow shuffling queue and with it (no rowgroup shuffling). Read ngrams
  Check the the order is deterministic within unshuffled read and is random with shuffled read"""
  fields = {
    0: [TestSchema.id],
    1: [TestSchema.id],
    2: [TestSchema.id]
  }

  # Expecting delta between ids to be 1. Setting 1.5 as upper bound
  ngram = NGram(fields=fields, delta_threshold=1.5, timestamp_field=TestSchema.id)
  unshuffled_1, _ = _read_from_tf_tensors(carbon_synthetic_dataset, 30, shuffling_queue_capacity=0, min_after_dequeue=0,
                                          ngram=ngram, workers_count=1)
  unshuffled_2, _ = _read_from_tf_tensors(carbon_synthetic_dataset, 30, shuffling_queue_capacity=0, min_after_dequeue=0,
                                          ngram=ngram, workers_count=1)

  shuffled_1, shuffled_1_ngram = \
    _read_from_tf_tensors(carbon_synthetic_dataset, 20, shuffling_queue_capacity=30, min_after_dequeue=29, ngram=ngram)
  shuffled_2, _ = \
    _read_from_tf_tensors(carbon_synthetic_dataset, 20, shuffling_queue_capacity=30, min_after_dequeue=29, ngram=ngram)

  # shuffled_1_ngram is a dictionary of named tuple indexed by time:
  # {0: (tensor, tensor, tensor, ...),
  #  1: (tensor, tensor, tensor, ...),
  #  ...}
  for row_tensor in shuffled_1_ngram.values():
    _assert_all_tensors_have_shape(row_tensor)

  # shuffled_1 is a list of dictionaries of named tuples indexed by time:
  # [{0: (tensor, tensor, tensor, ...),
  #  1: (tensor, tensor, tensor, ...),
  #  ...}
  # {0: (tensor, tensor, tensor, ...),
  #  1: (tensor, tensor, tensor, ...),
  #  ...},...
  # ]
  for one_ngram_dict in shuffled_1:
    _assert_expected_rows_data(carbon_synthetic_dataset.data, one_ngram_dict.values())

  def flatten(list_of_ngrams):
    return [row for seq in list_of_ngrams for row in seq.values()]

  assert [f.id for f in flatten(unshuffled_1)] == [f.id for f in flatten(unshuffled_2)]

  assert [f.id for f in flatten(unshuffled_1)] != [f.id for f in flatten(shuffled_2)]
  assert [f.id for f in flatten(shuffled_1)] != [f.id for f in flatten(shuffled_2)]


@pytest.mark.forked
def test_simple_read_tensorflow_with_carbon_dataset(carbon_scalar_dataset):
  """Read couple of rows. Make sure all tensors have static shape sizes assigned and the data matches reference
  data"""
  with make_batch_carbon_reader(dataset_url=carbon_scalar_dataset.url) as reader:
    row_tensors = tf_tensors(reader)
    # Make sure we have static shape info for all fields
    for column in row_tensors:
      assert column.get_shape().as_list() == [None]

    with _tf_session() as sess:
      for _ in range(2):
        batch = sess.run(row_tensors)._asdict()
        for i, id_value in enumerate(batch['id']):
          expected_row = next(d for d in carbon_scalar_dataset.data if d['id'] == id_value)
          for field_name in expected_row.keys():
            _assert_fields_eq(batch[field_name][i], expected_row[field_name])


@pytest.mark.forked
def test_simple_read_tensorflow_with_non_unischema_many_columns_dataset(carbon_many_columns_non_unischema_dataset):
  """Read couple of rows. Make sure all tensors have static shape sizes assigned and the data matches reference
  data"""
  with make_batch_carbon_reader(dataset_url=carbon_many_columns_non_unischema_dataset.url) as reader:
    row_tensors = tf_tensors(reader)
    # Make sure we have static shape info for all fields
    for column in row_tensors:
      assert column.get_shape().as_list() == [None]

    with _tf_session() as sess:
      batch = sess.run(row_tensors)._asdict()
      assert set(batch.keys()) == set(carbon_many_columns_non_unischema_dataset.data[0].keys())


def test_shuffling_queue_with_make_batch_carbon_reader(carbon_scalar_dataset):
  with make_batch_carbon_reader(dataset_url=carbon_scalar_dataset.url) as reader:
    with pytest.raises(ValueError):
      tf_tensors(reader, 100, 90)
