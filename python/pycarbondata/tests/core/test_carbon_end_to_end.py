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


from concurrent.futures import ThreadPoolExecutor
from shutil import rmtree, copytree

import numpy as np
import pytest

from petastorm.codecs import ScalarCodec

from pycarbon import make_carbon_reader, make_batch_carbon_reader
from pycarbon.reader import make_reader
from pycarbon.tests.core.test_carbon_common import TestSchema

from pycarbon.tests.conftest import _ROWS_COUNT

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

# pylint: disable=unnecessary-lambda
ALL_READER_FLAVOR_FACTORIES = [
  lambda url, **kwargs: make_carbon_reader(url, reader_pool_type='thread', **kwargs),
]

SCALAR_FIELDS = [f for f in TestSchema.fields.values() if isinstance(f.codec, ScalarCodec)]

SCALAR_ONLY_READER_FACTORIES = [
  lambda url, **kwargs: make_batch_carbon_reader(url, reader_pool_type='thread', **kwargs),
]


def _check_simple_reader(reader, expected_data, expected_rows_count=None, check_types=True, limit_checked_rows=None):
  # Read a bunch of entries from the dataset and compare the data to reference
  def _type(v):
    if isinstance(v, np.ndarray):
      if v.dtype.str.startswith('|S'):
        return '|S'
      else:
        return v.dtype
    else:
      return type(v)

  expected_rows_count = expected_rows_count or len(expected_data)
  count = 0

  for i, row in enumerate(reader):
    if limit_checked_rows and i >= limit_checked_rows:
      break

    actual = row._asdict()
    expected = next(d for d in expected_data if d['id'] == actual['id'])
    np.testing.assert_equal(actual, expected)
    actual_types = {k: _type(v) for k, v in actual.items()}
    expected_types = {k: _type(v) for k, v in expected.items()}
    assert not check_types or actual_types == expected_types
    count += 1

  if limit_checked_rows:
    assert count == min(expected_rows_count, limit_checked_rows)
  else:
    assert count == expected_rows_count


def _readout_all_ids(reader, limit=None):
  ids = []
  for i, row in enumerate(reader):
    if limit is not None and i >= limit:
      break
    ids.append(row.id)

  # Flatten ids if reader returns batches (make_batch_reader)
  if isinstance(ids[0], np.ndarray):
    ids = [i for arr in ids for i in arr]

  return ids


@pytest.mark.parametrize('reader_factory', ALL_READER_FLAVOR_FACTORIES)
def test_simple_read(carbon_synthetic_dataset, reader_factory):
  """Just a bunch of read and compares of all values to the expected values using the different reader pools"""
  with reader_factory(carbon_synthetic_dataset.url) as reader:
    _check_simple_reader(reader, carbon_synthetic_dataset.data)


@pytest.mark.parametrize('reader_factory', ALL_READER_FLAVOR_FACTORIES + SCALAR_ONLY_READER_FACTORIES)
@pytest.mark.forked
def test_simple_read_with_disk_cache(carbon_synthetic_dataset, reader_factory, tmpdir):
  """Try using the Reader with LocalDiskCache using different flavors of pools"""
  CACHE_SIZE = 10 * 2 ** 30  # 20GB
  ROW_SIZE_BYTES = 100  # not really important for this test
  with reader_factory(carbon_synthetic_dataset.url, num_epochs=2,
                      cache_type='local-disk', cache_location=tmpdir.strpath,
                      cache_size_limit=CACHE_SIZE, cache_row_size_estimate=ROW_SIZE_BYTES) as reader:
    ids = _readout_all_ids(reader)
    assert 200 == len(ids)  # We read 2 epochs
    assert set(ids) == set(range(100))


@pytest.mark.parametrize('reader_factory', ALL_READER_FLAVOR_FACTORIES + SCALAR_ONLY_READER_FACTORIES)
def test_simple_read_with_added_slashes(carbon_synthetic_dataset, reader_factory):
  """Tests that using relative paths for the dataset metadata works as expected"""
  with reader_factory(carbon_synthetic_dataset.url + '///') as reader:
    next(reader)


@pytest.mark.parametrize('reader_factory', ALL_READER_FLAVOR_FACTORIES + SCALAR_ONLY_READER_FACTORIES)
def test_simple_read_moved_dataset(carbon_synthetic_dataset, tmpdir, reader_factory):
  """Tests that a dataset may be opened after being moved to a new location"""
  a_moved_path = tmpdir.join('moved').strpath
  copytree(carbon_synthetic_dataset.path, a_moved_path)

  with reader_factory('file://{}'.format(a_moved_path)) as reader:
    next(reader)

  rmtree(a_moved_path)


@pytest.mark.parametrize('reader_factory', ALL_READER_FLAVOR_FACTORIES)
def test_reading_subset_of_columns(carbon_synthetic_dataset, reader_factory):
  """Just a bunch of read and compares of all values to the expected values"""
  with reader_factory(carbon_synthetic_dataset.url, schema_fields=[TestSchema.id2, TestSchema.id]) as reader:
    # Read a bunch of entries from the dataset and compare the data to reference
    for row in reader:
      actual = dict(row._asdict())
      expected = next(d for d in carbon_synthetic_dataset.data if d['id'] == actual['id'])
      np.testing.assert_equal(expected['id2'], actual['id2'])


@pytest.mark.parametrize('reader_factory', ALL_READER_FLAVOR_FACTORIES)
def test_reading_subset_of_columns_using_regex(carbon_synthetic_dataset, reader_factory):
  """Just a bunch of read and compares of all values to the expected values"""
  with reader_factory(carbon_synthetic_dataset.url, schema_fields=['id$', 'id_.*$', 'partition_key$']) as reader:
    # Read a bunch of entries from the dataset and compare the data to reference
    for row in reader:
      actual = dict(row._asdict())
      assert set(actual.keys()) == {'id_float', 'id_odd', 'id', 'partition_key'}
      expected = next(d for d in carbon_synthetic_dataset.data if d['id'] == actual['id'])
      np.testing.assert_equal(expected['id_float'], actual['id_float'])


# TODO: fix make_batch_carbon_reader error
@pytest.mark.parametrize('reader_factory', [
  lambda url, **kwargs: make_carbon_reader(url, reader_pool_type='thread', **kwargs),
  lambda url, **kwargs: make_batch_carbon_reader(url, reader_pool_type='thread', **kwargs)])
def test_shuffle(carbon_synthetic_dataset, reader_factory):
  rows_count = len(carbon_synthetic_dataset.data)

  # Read ids twice without shuffle: assert we have the same array and all expected ids are in the array
  with reader_factory(carbon_synthetic_dataset.url, shuffle_blocklets=False, shuffle_row_drop_partitions=5,
                      workers_count=1) as reader_1:
    first_readout = _readout_all_ids(reader_1)
  with reader_factory(carbon_synthetic_dataset.url, shuffle_blocklets=False, shuffle_row_drop_partitions=5,
                      workers_count=1) as reader_2:
    second_readout = _readout_all_ids(reader_2)

  np.testing.assert_array_equal(range(rows_count), sorted(first_readout))
  np.testing.assert_array_equal(first_readout, second_readout)

  # Now read with shuffling
  with reader_factory(carbon_synthetic_dataset.url, shuffle_blocklets=True, shuffle_row_drop_partitions=5,
                      workers_count=1) as shuffled_reader:
    shuffled_readout = _readout_all_ids(shuffled_reader)
  assert np.any(np.not_equal(first_readout, shuffled_readout))


# TODO: fix make_batch_carbon_reader error
@pytest.mark.parametrize('reader_factory', [
  lambda url, **kwargs: make_carbon_reader(url, reader_pool_type='thread', **kwargs),
  lambda url, **kwargs: make_batch_carbon_reader(url, reader_pool_type='thread', **kwargs)])
def test_shuffle_drop_ratio(carbon_synthetic_dataset, reader_factory):
  # Read ids twice without shuffle: assert we have the same array and all expected ids are in the array
  with reader_factory(carbon_synthetic_dataset.url, shuffle_blocklets=False, shuffle_row_drop_partitions=1) as reader:
    first_readout = _readout_all_ids(reader)
  np.testing.assert_array_equal([r['id'] for r in carbon_synthetic_dataset.data], sorted(first_readout))

  # Test that the ids are increasingly not consecutive numbers as we increase the shuffle dropout
  prev_jumps_not_1 = 0
  for shuffle_dropout in [2, 5, 8]:
    with reader_factory(carbon_synthetic_dataset.url, shuffle_blocklets=True,
                        shuffle_row_drop_partitions=shuffle_dropout) as reader:
      readout = _readout_all_ids(reader)

    assert len(first_readout) == len(readout)
    jumps_not_1 = np.sum(np.diff(readout) != 1)
    assert jumps_not_1 > prev_jumps_not_1
    prev_jumps_not_1 = jumps_not_1


# TODO: fix test_partition_multi_node
# @pytest.mark.parametrize('reader_factory', ALL_READER_FLAVOR_FACTORIES)
# def test_partition_multi_node(carbon_synthetic_dataset, reader_factory):
#   """Tests that the reader only returns half of the expected data consistently"""
#   with reader_factory(carbon_synthetic_dataset.url, cur_shard=0, shard_count=5) as reader:
#     with reader_factory(carbon_synthetic_dataset.url, cur_shard=0, shard_count=5) as reader_2:
#       results_1 = set(_readout_all_ids(reader))
#       results_2 = set(_readout_all_ids(reader_2))
#
#       assert results_1, 'Non empty shard expected'
#
#       np.testing.assert_equal(results_1, results_2)
#
#       assert len(results_1) < len(carbon_synthetic_dataset.data)
#
#       # Test that separate partitions also have no overlap by checking ids)
#       for partition in range(1, 5):
#         with reader_factory(carbon_synthetic_dataset.url, cur_shard=partition,
#                             shard_count=5) as reader_other:
#           ids_in_other_partition = set(_readout_all_ids(reader_other))
#
#           assert not ids_in_other_partition.intersection(results_1)


# TODO: fix test_partition_value_error
# @pytest.mark.parametrize('reader_factory', ALL_READER_FLAVOR_FACTORIES)
# def test_partition_value_error(carbon_synthetic_dataset, reader_factory):
#   """Tests that the reader raises value errors when appropriate"""
#
#   # shard_count has to be greater than 0
#   with pytest.raises(ValueError):
#     reader_factory(carbon_synthetic_dataset.url, shard_count=0)
#
#   # missing cur_shard value
#   with pytest.raises(ValueError):
#     reader_factory(carbon_synthetic_dataset.url, shard_count=5)
#
#   # cur_shard is a string
#   with pytest.raises(ValueError):
#     reader_factory(carbon_synthetic_dataset.url, cur_shard='0',
#                    shard_count=5)
#
#   # shard_count is a string
#   with pytest.raises(ValueError):
#     reader_factory(carbon_synthetic_dataset.url, cur_shard=0,
#                    shard_count='5')


@pytest.mark.parametrize('reader_factory', [
  lambda url, **kwargs: make_carbon_reader(url, reader_pool_type='thread', **kwargs),
  lambda url, **kwargs: make_batch_carbon_reader(url, reader_pool_type='thread', **kwargs),
])
def test_stable_pieces_order(carbon_synthetic_dataset, reader_factory):
  """Tests that the reader raises value errors when appropriate"""

  RERUN_THE_TEST_COUNT = 4
  baseline_run = None
  for _ in range(RERUN_THE_TEST_COUNT):
    # TODO(yevgeni): factor out. Reading all ids appears multiple times in this test.
    with reader_factory(carbon_synthetic_dataset.url, shuffle_blocklets=False, workers_count=1) as reader:
      this_run = _readout_all_ids(reader)

    if baseline_run:
      assert this_run == baseline_run

    baseline_run = this_run


@pytest.mark.parametrize('reader_factory',
                         [lambda url, **kwargs: make_reader(url, reader_pool_type='thread', is_batch=False, **kwargs)])
def test_multiple_epochs(carbon_synthetic_dataset, reader_factory):
  """Tests that multiple epochs works as expected"""
  num_epochs = 5
  with reader_factory(carbon_synthetic_dataset.url, num_epochs=num_epochs) as reader:
    # Read all expected entries from the dataset and compare the data to reference
    single_epoch_id_set = [d['id'] for d in carbon_synthetic_dataset.data]
    actual_ids_in_all_epochs = _readout_all_ids(reader)
    np.testing.assert_equal(sorted(actual_ids_in_all_epochs), sorted(num_epochs * single_epoch_id_set))

    # Reset reader should reset ventilator. Should produce another `num_epochs` results
    reader.reset()
    actual_ids_in_all_epochs = _readout_all_ids(reader)
    np.testing.assert_equal(sorted(actual_ids_in_all_epochs), sorted(num_epochs * single_epoch_id_set))


@pytest.mark.parametrize('reader_factory',
                         [lambda url, **kwargs: make_reader(url, reader_pool_type='thread', is_batch=False, **kwargs)])
def test_fail_if_resetting_in_the_middle_of_epoch(carbon_synthetic_dataset, reader_factory):
  """Tests that multiple epochs works as expected"""
  num_epochs = 5
  with reader_factory(carbon_synthetic_dataset.url, num_epochs=num_epochs) as reader:
    # Read all expected entries from the dataset and compare the data to reference
    actual_ids = _readout_all_ids(reader, limit=20)
    assert len(actual_ids) == 20

    with pytest.raises(NotImplementedError):
      reader.reset()


@pytest.mark.parametrize('reader_factory', ALL_READER_FLAVOR_FACTORIES + SCALAR_ONLY_READER_FACTORIES)
def test_unlimited_epochs(carbon_synthetic_dataset, reader_factory):
  """Tests that unlimited epochs works as expected"""
  with reader_factory(carbon_synthetic_dataset.url, num_epochs=None) as reader:
    read_limit = len(carbon_synthetic_dataset.data) * 3 + 2
    actual_ids = _readout_all_ids(reader, read_limit)
    expected_ids = [d['id'] for d in carbon_synthetic_dataset.data]
    assert len(actual_ids) > len(expected_ids)
    assert set(actual_ids) == set(expected_ids)


@pytest.mark.parametrize('reader_factory', ALL_READER_FLAVOR_FACTORIES + SCALAR_ONLY_READER_FACTORIES)
def test_num_epochs_value_error(carbon_synthetic_dataset, reader_factory):
  """Tests that the reader raises value errors when appropriate"""

  # Testing only Reader v1, as the v2 uses an epoch generator. The error would raise only when the generator is
  # evaluated. Parameter validation for Reader v2 is covered by test_epoch_generator.py

  with pytest.raises(ValueError):
    reader_factory(carbon_synthetic_dataset.url, num_epochs=-10)

  with pytest.raises(ValueError):
    reader_factory(carbon_synthetic_dataset.url, num_epochs='abc')


@pytest.mark.parametrize('reader_factory', ALL_READER_FLAVOR_FACTORIES + SCALAR_ONLY_READER_FACTORIES)
def test_dataset_path_is_a_unicode(carbon_synthetic_dataset, reader_factory):
  """Just a bunch of read and compares of all values to the expected values using the different reader pools"""
  # Making sure unicode_in_p23 is a unicode both in python 2 and 3
  unicode_in_p23 = carbon_synthetic_dataset.url.encode().decode('utf-8')
  with reader_factory(unicode_in_p23) as reader:
    next(reader)


# TODO: fix test_make_carbon_reader_fails_loading_non_unischema_dataset
def test_make_carbon_reader_fails_loading_non_unischema_dataset(carbon_many_columns_non_unischema_dataset):
  with pytest.raises(RuntimeError, match='use make_batch_carbon_reader'):
    make_carbon_reader(carbon_many_columns_non_unischema_dataset.url)


def test_multithreaded_reads(carbon_synthetic_dataset):
  with make_reader(carbon_synthetic_dataset.url, is_batch=False, workers_count=5, num_epochs=1) as reader:
    with ThreadPoolExecutor(max_workers=10) as executor:
      def read_one_row():
        return next(reader)

      futures = [executor.submit(read_one_row) for _ in range(100)]
      results = [f.result() for f in futures]
      assert len(results) == len(carbon_synthetic_dataset.data)
      assert set(r.id for r in results) == set(d['id'] for d in carbon_synthetic_dataset.data)


@pytest.mark.forked
def test_make_reader(carbon_synthetic_dataset):
  with make_reader(carbon_synthetic_dataset.url, is_batch=False, num_epochs=1) as reader:
    i = 0
    for sample in reader:
      print(sample.id)
      i += 1
    assert i == _ROWS_COUNT


@pytest.mark.forked
def test_batch_carbon_reader(carbon_synthetic_dataset):
  with make_reader(carbon_synthetic_dataset.url, num_epochs=1) as reader:
    i = 0
    for sample in reader:
      for ele in sample.id:
        print(ele)
        i += 1
    assert i == _ROWS_COUNT


@pytest.mark.forked
def test_make_reader_of_obs(carbon_obs_dataset):
  with make_reader(carbon_obs_dataset.url,
                   key=pytest.config.getoption("--access_key"),
                   secret=pytest.config.getoption("--secret_key"),
                   endpoint=pytest.config.getoption("--end_point")) as reader:
    i = 0
    for sample in reader:
      i += len(sample.id)

    assert i == _ROWS_COUNT
