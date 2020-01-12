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


import collections
import logging
import warnings

import six

from petastorm.cache import NullCache
from petastorm.local_disk_cache import LocalDiskCache
from petastorm.ngram import NGram
from petastorm.transform import transform_schema
from petastorm.workers_pool.thread_pool import ThreadPool
from petastorm.workers_pool.ventilator import ConcurrentVentilator

from pycarbon.core.carbon_arrow_reader_worker import ArrowCarbonReaderWorker
from pycarbon.core.carbon_py_dict_reader_worker import PyDictCarbonReaderWorker
from pycarbon.core.carbon import CarbonDataset
from pycarbon.core import carbon_dataset_metadata
from pycarbon.core.carbon_dataset_metadata import infer_or_load_unischema_carbon
from pycarbon.core.carbon_dataset_metadata import PycarbonMetadataError
from pycarbon.core.carbon_local_memory_cache import LocalMemoryCache
from pycarbon.core.carbon_fs_utils import CarbonFilesystemResolver

logger = logging.getLogger(__name__)


# Ventilator guarantees that no more than workers + _VENTILATE_EXTRA_BLOCKLETS are processed at a moment by a
# worker pool. This guarantees that we don't run out of memory if data consumer is slower than the Reader.
_VENTILATE_EXTRA_BLOCKLETS = 2


def make_carbon_reader(dataset_url,
                       key=None,
                       secret=None,
                       endpoint=None,
                       proxy=None,
                       proxy_port=None,
                       schema_fields=None,
                       reader_pool_type='thread', workers_count=10, results_queue_size=100,
                       shuffle_blocklets=True, shuffle_row_drop_partitions=1,
                       predicate=None,
                       blocklet_selector=None,
                       num_epochs=1,
                       cur_shard=None, shard_count=None,
                       cache_type='null', cache_location=None, cache_size_limit=None,
                       cache_row_size_estimate=None, cache_extra_settings=None,
                       hdfs_driver='libhdfs3',
                       reader_engine='reader_v1', reader_engine_params=None,
                       transform_spec=None):
  """
  Creates an instance of Reader for reading Pycarbon datasets. A Pycarbon dataset is a dataset generated using
  :func:`~pycarbon.etl.carbon_dataset_metadata.materialize_dataset_carbon` context manager as explained

  See :func:`~pycarbon.make_batch_carbon_reader` to read from a Carbon store that was not generated using
  :func:`~pycarbon.etl.carbon_dataset_metadata.materialize_dataset_carbon`.

  :param dataset_url: an filepath or a url to a carbon directory,
      e.g. ``'hdfs://some_hdfs_cluster/user/yevgeni/carbon8'``, or ``'file:///tmp/mydataset'``
      or ``'s3://bucket/mydataset'``.
  :param key: access key
  :param secret: secret key
  :param endpoint: endpoint_url
  :param proxy: proxy
  :param proxy_port:  proxy_port
  :param schema_fields: Can be: a list of unischema fields and/or regex pattern strings; ``None`` to read all fields;
          an NGram object, then it will return an NGram of the specified fields.
  :param reader_pool_type: A string denoting the reader pool type. Should be one of ['thread', 'process', 'dummy']
      denoting a thread pool, process pool, or running everything in the master thread. Defaults to 'thread'
    TODO: process support
  :param workers_count: An int for the number of workers to use in the reader pool. This only is used for the
      thread or process pool. Defaults to 10
  :param results_queue_size: Size of the results queue to store prefetched rows. Currently only applicable to
      thread reader pool type.
  :param shuffle_blocklets: Whether to shuffle blocklets (the order in which full blocklets are read)
  :param shuffle_row_drop_partitions: This is is a positive integer which determines how many partitions to
      break up a blocklet into for increased shuffling in exchange for worse performance (extra reads).
      For example if you specify 2 each blocklet read will drop half of the rows within every blocklet and
      read the remaining rows in separate reads. It is recommended to keep this number below the regular row
      group size in order to not waste reads which drop all rows.
  :param predicate: instance of :class:`.PredicateBase` object to filter rows to be returned by reader. The predicate
      will be passed a single row and must return a boolean value indicating whether to include it in the results.
  :param blocklet_selector: instance of blocklet selector object to select blocklet to be read
    TODO: blocklet_selector
  :param num_epochs: An epoch is a single pass over all rows in the dataset. Setting ``num_epochs`` to
      ``None`` will result in an infinite number of epochs.
  :param cur_shard: An int denoting the current shard number. Each node reading a shard should
      pass in a unique shard number in the range [0, shard_count). shard_count must be supplied as well.
      Defaults to None
  :param shard_count: An int denoting the number of shards to break this dataset into. Defaults to None
    TODO: cur_shard & shard_count
  :param cache_type: A string denoting the cache type, if desired. Options are [None, 'null', 'local-disk'] to
      either have a null/noop cache or a cache implemented using diskcache. Caching is useful when communication
      to the main data store is either slow or expensive and the local machine has large enough storage
      to store entire dataset (or a partition of a dataset if shard_count is used). By default will be a null cache.
  :param cache_location: A string denoting the location or path of the cache.
  :param cache_size_limit: An int specifying the size limit of the cache in bytes
  :param cache_row_size_estimate: An int specifying the estimated size of a row in the dataset
  :param cache_extra_settings: A dictionary of extra settings to pass to the cache implementation,
  :param hdfs_driver: A string denoting the hdfs driver to use (if using a dataset on hdfs). Current choices are
      libhdfs (java through JNI) or libhdfs3 (C++)
  :param reader_engine: Multiple engine implementations exist ('reader_v1' and 'experimental_reader_v2'). 'reader_v1'
      (the default value) selects a stable reader implementation.
    TODO: experimental_reader_v2 for carbon
  :param reader_engine_params: For advanced usage: a dictionary with arguments passed directly to a reader
      implementation constructor chosen by ``reader_engine`` argument.  You should not use this parameter, unless you
      fine-tuning of a reader.
  :param transform_spec: An instance of :class:`~petastorm.transform.TransformSpec` object defining how a record
      is transformed after it is loaded and decoded. The transformation occurs on a worker thread/process (depends
      on the ``reader_pool_type`` value).
  :return: A :class:`Reader` object
  """

  if dataset_url is None or not isinstance(dataset_url, six.string_types):
    raise ValueError("""dataset_url must be a string""")

  dataset_url = dataset_url[:-1] if dataset_url[-1] == '/' else dataset_url
  logger.debug('dataset_url: %s', dataset_url)

  resolver = CarbonFilesystemResolver(dataset_url,
                                      key=key,
                                      secret=secret,
                                      endpoint=endpoint,
                                      proxy=proxy,
                                      proxy_port=proxy_port,
                                      hdfs_driver=hdfs_driver)
  filesystem = resolver.filesystem()

  if cache_type is None or cache_type == 'null':
    cache = NullCache()
  elif cache_type == 'local-disk':
    cache = LocalDiskCache(cache_location, cache_size_limit, cache_row_size_estimate, **cache_extra_settings or {})
  elif cache_type == 'memory-cache':
    cache = LocalMemoryCache(cache_size_limit)
  else:
    raise ValueError('Unknown cache_type: {}'.format(cache_type))

  # Fail if this is a non-pycarbon dataset. Typically, a Carbon store will have hundred thousands rows in a single
  # blocklet. Using PyDictCarbonReaderWorker or ReaderV2 implementation is very inefficient as it processes data on a
  # row by row basis. ArrowCarbonReaderWorker (used by make_batch_carbon_reader) is much more efficient in these cases.
  try:
    carbon_dataset_metadata.get_schema_from_dataset_url_carbon(dataset_url,
                                                               key=key,
                                                               secret=secret,
                                                               endpoint=endpoint,
                                                               proxy=proxy,
                                                               proxy_port=proxy_port,
                                                               filesystem=filesystem)
  except PycarbonMetadataError:
    raise RuntimeError('Currently make_carbon_reader supports reading only Pycarbon datasets(has unischema). '
                       'To read from a non-Pycarbon Carbon store use make_batch_carbon_reader')

  if reader_engine == 'reader_v1':
    if reader_pool_type == 'thread':
      reader_pool = ThreadPool(workers_count, results_queue_size)
    elif reader_pool_type == 'process':
      raise NotImplementedError('not support process reader_pool_type now.')
    elif reader_pool_type == 'dummy':
      raise NotImplementedError('not support dummy reader_pool_type now.')
    else:
      raise ValueError('Unknown reader_pool_type: {}'.format(reader_pool_type))

    # Create a dictionary with all ReaderV2 parameters, so we can merge with reader_engine_params if specified
    kwargs = {
      'key': key,
      'secret': secret,
      'endpoint': endpoint,
      'proxy': proxy,
      'proxy_port': proxy_port,
      'schema_fields': schema_fields,
      'reader_pool': reader_pool,
      'shuffle_blocklets': shuffle_blocklets,
      'shuffle_row_drop_partitions': shuffle_row_drop_partitions,
      'predicate': predicate,
      'blocklet_selector': blocklet_selector,
      'num_epochs': num_epochs,
      'cur_shard': cur_shard,
      'shard_count': shard_count,
      'cache': cache,
      'transform_spec': transform_spec,
    }

    if reader_engine_params:
      kwargs.update(reader_engine_params)

    try:
      return CarbonDataReader(filesystem, dataset_url,
                              worker_class=PyDictCarbonReaderWorker,
                              **kwargs)
    except PycarbonMetadataError as e:
      logger.error('Unexpected exception: %s', str(e))
      raise RuntimeError('make_carbon_reader has failed. If you were trying to open a Carbon store that was not '
                         'created using Pycarbon materialize_dataset_carbon and it contains only scalar columns, '
                         'you may use make_batch_reader to read it.\n'
                         'Inner exception: %s', str(e))

  elif reader_engine == 'experimental_reader_v2':
    raise NotImplementedError('not support experimental_reader_v2 reader engine now.')
  else:
    raise ValueError('Unexpected value of reader_engine argument \'%s\'. '
                     'Supported reader_engine values are \'reader_v1\' and \'experimental_reader_v2\'',
                     reader_engine)


def make_batch_carbon_reader(dataset_url,
                             key=None,
                             secret=None,
                             endpoint=None,
                             proxy=None,
                             proxy_port=None,
                             schema_fields=None,
                             reader_pool_type='thread', workers_count=10, results_queue_size=100,
                             shuffle_blocklets=True, shuffle_row_drop_partitions=1,
                             predicate=None,
                             blocklet_selector=None,
                             num_epochs=1,
                             cur_shard=None, shard_count=None,
                             cache_type='null', cache_location=None, cache_size_limit=None,
                             cache_row_size_estimate=None, cache_extra_settings=None,
                             hdfs_driver='libhdfs3',
                             transform_spec=None):
  """
  Creates an instance of Reader for reading batches out of a non-Pycarbon Carbon store.

  Currently, only stores having native scalar carbon data types are supported.
  Use :func:`~pycarbon.make_carbon_reader` to read Pycarbon Carbon stores generated with
  :func:`~pycarbon.etl.carbon_dataset_metadata.materialize_dataset_carbon`.

  NOTE: only scalar columns are currently supported.

  :param dataset_url: an filepath or a url to a carbon directory,
      e.g. ``'hdfs://some_hdfs_cluster/user/yevgeni/carbon8'``, or ``'file:///tmp/mydataset'``
      or ``'s3://bucket/mydataset'``.
  :param key: access key
  :param secret: secret key
  :param endpoint: endpoint_url
  :param proxy: proxy
  :param proxy_port:  proxy_port
  :param schema_fields: A list of regex pattern strings. Only columns matching at least one of the
      patterns in the list will be loaded.
  :param reader_pool_type: A string denoting the reader pool type. Should be one of ['thread', 'process', 'dummy']
      denoting a thread pool, process pool, or running everything in the master thread. Defaults to 'thread'
  :param workers_count: An int for the number of workers to use in the reader pool. This only is used for the
      thread or process pool. Defaults to 10
  :param results_queue_size: Size of the results queue to store prefetched rows. Currently only applicable to
      thread reader pool type.
  :param shuffle_blocklets: Whether to shuffle blocklets (the order in which full blocklets are read)
  :param shuffle_row_drop_partitions: This is is a positive integer which determines how many partitions to
      break up a blocklet into for increased shuffling in exchange for worse performance (extra reads).
      For example if you specify 2 each blocklet read will drop half of the rows within every blocklet and
      read the remaining rows in separate reads. It is recommended to keep this number below the regular row
      group size in order to not waste reads which drop all rows.
  :param predicate: instance of :class:`.PredicateBase` object to filter rows to be returned by reader. The predicate
      will be passed a pandas DataFrame object and must return a pandas Series with boolean values of matching
      dimensions.
  :param blocklet_selector: instance of blocklet selector object to select blocklets to be read
  :param num_epochs: An epoch is a single pass over all rows in the dataset. Setting ``num_epochs`` to
      ``None`` will result in an infinite number of epochs.
  :param cur_shard: An int denoting the current shard number. Each node reading a shard should
      pass in a unique shard number in the range [0, shard_count). shard_count must be supplied as well.
      Defaults to None
  :param shard_count: An int denoting the number of shards to break this dataset into. Defaults to None
  :param cache_type: A string denoting the cache type, if desired. Options are [None, 'null', 'local-disk'] to
      either have a null/noop cache or a cache implemented using diskcache. Caching is useful when communication
      to the main data store is either slow or expensive and the local machine has large enough storage
      to store entire dataset (or a partition of a dataset if shard_count is used). By default will be a null cache.
  :param cache_location: A string denoting the location or path of the cache.
  :param cache_size_limit: An int specifying the size limit of the cache in bytes
  :param cache_row_size_estimate: An int specifying the estimated size of a row in the dataset
  :param cache_extra_settings: A dictionary of extra settings to pass to the cache implementation,
  :param hdfs_driver: A string denoting the hdfs driver to use (if using a dataset on hdfs). Current choices are
      libhdfs (java through JNI) or libhdfs3 (C++)
  :param transform_spec: An instance of :class:`~petastorm.transform.TransformSpec` object defining how a record
      is transformed after it is loaded and decoded. The transformation occurs on a worker thread/process (depends
      on the ``reader_pool_type`` value).
  :return: A :class:`Reader` object
  """

  if dataset_url is None or not isinstance(dataset_url, six.string_types):
    raise ValueError("""dataset_url must be a string""")

  dataset_url = dataset_url[:-1] if dataset_url[-1] == '/' else dataset_url
  logger.debug('dataset_url: %s', dataset_url)

  resolver = CarbonFilesystemResolver(dataset_url,
                                      key=key,
                                      secret=secret,
                                      endpoint=endpoint,
                                      proxy=proxy,
                                      proxy_port=proxy_port,
                                      hdfs_driver=hdfs_driver)
  filesystem = resolver.filesystem()

  try:
    carbon_dataset_metadata.get_schema_from_dataset_url_carbon(dataset_url,
                                                               key=key,
                                                               secret=secret,
                                                               endpoint=endpoint,
                                                               proxy=proxy,
                                                               proxy_port=proxy_port,
                                                               filesystem=filesystem)
    warnings.warn('Please use make_carbon_reader (instead of \'make_batch_carbon_reader\' function '
                  'to read this dataset as it contains unischema file.')
  except PycarbonMetadataError:
    pass

  if cache_type is None or cache_type == 'null':
    cache = NullCache()
  elif cache_type == 'local-disk':
    cache = LocalDiskCache(cache_location, cache_size_limit, cache_row_size_estimate,
                           **cache_extra_settings or {})
  elif cache_type == 'memory-cache':
    cache = LocalMemoryCache(cache_size_limit)
  else:
    raise ValueError('Unknown cache_type: {}'.format(cache_type))

  if reader_pool_type == 'thread':
    reader_pool = ThreadPool(workers_count, results_queue_size)
  elif reader_pool_type == 'process':
    raise NotImplementedError('not support process reader_pool_type now.')
  elif reader_pool_type == 'dummy':
    raise NotImplementedError('not support dummy reader_pool_type now.')
  else:
    raise ValueError('Unknown reader_pool_type: {}'.format(reader_pool_type))

  return CarbonDataReader(filesystem, dataset_url,
                          key=key, secret=secret, endpoint=endpoint,
                          proxy=proxy, proxy_port=proxy_port,
                          schema_fields=schema_fields,
                          worker_class=ArrowCarbonReaderWorker,
                          reader_pool=reader_pool,
                          shuffle_blocklets=shuffle_blocklets,
                          shuffle_row_drop_partitions=shuffle_row_drop_partitions,
                          predicate=predicate,
                          blocklet_selector=blocklet_selector,
                          num_epochs=num_epochs,
                          cur_shard=cur_shard,
                          shard_count=shard_count,
                          cache=cache,
                          transform_spec=transform_spec)


class CarbonDataReader(object):
  """Reads a dataset from a Pycarbon dataset.

  :ivar last_row_consumed: True if the last row was already returned by the Reader.
  """

  def __init__(self, pyarrow_filesystem, dataset_path,
               key=None, secret=None, endpoint=None,
               proxy=None, proxy_port=None,
               schema_fields=None,
               shuffle_blocklets=True, shuffle_row_drop_partitions=1,
               predicate=None, blocklet_selector=None, reader_pool=None, num_epochs=1,
               cur_shard=None, shard_count=None, cache=None, worker_class=None,
               transform_spec=None):
    """Initializes a reader object.

    :param pyarrow_filesystem: An instance of ``pyarrow.FileSystem`` that will be used. If not specified,
        then a default one will be selected based on the url (only for ``hdfs://`` or ``file://``; for
        ``s3://`` support, use ``make_reader``). The default hdfs driver is ``libhdfs3``. If you want
        to to use ``libhdfs``, use
        ``pyarrow_filesystem=pyarrow.hdfs.connect('hdfs:///some/path', driver='libhdfs')``.
    :param dataset_path: filepath to a carbon directory on the specified filesystem.
        e.g. ``'/user/yevgeni/carbon8'``, or ``'/tmp/mydataset'``.
    :param key: access key
    :param secret: secret key
    :param endpoint: endpoint_url
    :param proxy: proxy
    :param proxy_port:  proxy_port
    :param schema_fields: Either list of unischema fields to subset, or ``None`` to read all fields.
        OR an NGram object, then it will return an NGram of the specified properties.
    :param shuffle_blocklets: Whether to shuffle blocklets (the order in which full blocklets are read)
    :param shuffle_row_drop_partitions: This is is a positive integer which determines how many partitions to
        break up a blocklet into for increased shuffling in exchange for worse performance (extra reads).
        For example if you specify 2 each blocklet read will drop half of the rows within every blocklet and
        read the remaining rows in separate reads. It is recommended to keep this number below the regular row
        group size in order to not waste reads which drop all rows.
    :param predicate: instance of predicate object to filter rows to be returned by reader.
    :param blocklet_selector: instance of blocklet selector object to select blocklets to be read
    :param reader_pool: parallelization pool. ``ThreadPool(10)`` (10 threads) is used by default.
        This pool is a custom implementation used to parallelize reading data from the dataset.
        Any object from workers_pool package can be used
        (e.g. :class:`petastorm.workers_pool.process_pool.ProcessPool`).
    :param num_epochs: An epoch is a single pass over all rows in the dataset. Setting ``num_epochs`` to
        ``None`` will result in an infinite number of epochs.
    :param cur_shard: An int denoting the current shard number used. Each reader instance should
        pass in a unique shard number in the range ``[0, shard_count)``.
        ``shard_count`` must be supplied as well. Defaults to None
    :param shard_count: An int denoting the number of shard partitions there are. Defaults to None
    :param cache: An object conforming to :class:`.CacheBase` interface. Before loading blocklets from a carbon
        file the Reader will attempt to load these values from cache. Caching is useful when communication
        to the main data store is either slow or expensive and the local machine has large enough storage
        to store entire dataset (or a partition of a dataset if shards are used).
        By default, use the :class:`.NullCache` implementation.

    :param worker_class: This is the class that will be instantiated on a different thread/process. It's
        responsibility is to load and filter the data.
    """

    # 1. Open the carbon storage (dataset) & Get a list of all blocklets
    # 2. Filter blocklets
    # 4. Create a blocklet ventilator object
    # 5. Start workers pool
    if not (isinstance(schema_fields, collections.Iterable) or isinstance(schema_fields, NGram)
            or schema_fields is None):
      raise ValueError("""Fields must be either None, an iterable collection of Unischema fields or an NGram
            object.""")

    self.ngram = schema_fields if isinstance(schema_fields, NGram) else None

    # By default, use original method of working with list of dictionaries and not arrow tables
    worker_class = worker_class or PyDictCarbonReaderWorker
    self._results_queue_reader = worker_class.new_results_queue_reader()

    if self.ngram and not self.ngram.timestamp_overlap and shuffle_row_drop_partitions > 1:
      raise NotImplementedError('Using timestamp_overlap=False is not implemented with'
                                ' shuffle_options.shuffle_row_drop_partitions > 1')

    self.cache = cache or NullCache()

    self._workers_pool = reader_pool or ThreadPool(10)
    # 1. Resolve dataset path (hdfs://, file://) and open the carbon storage (dataset)
    self.carbon_dataset = CarbonDataset(dataset_path,
                                        key=key,
                                        secret=secret,
                                        endpoint=endpoint,
                                        proxy=proxy,
                                        proxy_port=proxy_port,
                                        filesystem=pyarrow_filesystem)
    stored_schema = infer_or_load_unischema_carbon(self.carbon_dataset)

    # Make a schema view (a view is a Unischema containing only a subset of fields
    # Will raise an exception if invalid schema fields are in schema_fields
    fields = schema_fields if isinstance(schema_fields, collections.Iterable) else None
    storage_schema = stored_schema.create_schema_view(fields) if fields else stored_schema
    if transform_spec:
      self.schema = transform_schema(storage_schema, transform_spec)
    else:
      self.schema = storage_schema

    # 2. Filter blocklets
    filtered_blocklet_indexes = list(range(len(self.carbon_dataset.pieces)))
    worker_predicate = predicate

    # 3. Create a blocklet ventilator object
    normalized_shuffle_row_drop_partitions = \
      self._normalize_shuffle_options(shuffle_row_drop_partitions, self.carbon_dataset)
    self.ventilator = self._create_ventilator(filtered_blocklet_indexes, shuffle_blocklets,
                                              normalized_shuffle_row_drop_partitions, num_epochs, worker_predicate,
                                              self._workers_pool.workers_count + _VENTILATE_EXTRA_BLOCKLETS)

    # 4. Start workers pool
    self._workers_pool.start(worker_class, (pyarrow_filesystem, dataset_path, storage_schema, self.ngram,
                                            self.carbon_dataset.pieces, cache, transform_spec),
                             ventilator=self.ventilator)
    logger.debug('Workers pool started')

    self.last_row_consumed = False

  def reset(self):
    """Resets ``Reader`` state and allows to fetch more samples once the ``Reader`` finished reading all epochs,
    as specified by the ``num_epochs`` parameter.

    Once all samples were read from a reader, an attempt to fetch new sample (e.g. ``next(reader)`` would raise
    ``StopIterationError``. You can reset the reader to the original state and restart reading samples
    calling ``reset()``.

    We do not support calling ``reset()`` until all samples were consumed. ``NotImplementedError``
    will be raised if a user attempt to do so.

    Calling reset after ``stop()`` was called has no effect.

    :return: None
    """
    if not self.last_row_consumed:
      raise NotImplementedError('Currently do not support resetting a reader while in the middle of iteration. '
                                'You can call reset only after all samples were consumed.')
    self.last_row_consumed = False
    self.ventilator.reset()

  @property
  def batched_output(self):
    return self._results_queue_reader.batched_output

  @staticmethod
  def _normalize_shuffle_options(shuffle_row_drop_partitions, carbonSplit):
    """Checks that shuffle_options doesnt ask for more patitions than rows in a blocklet.
    This prevents sending partitions to workers which will result in not reading anything."""
    if shuffle_row_drop_partitions > 1 and carbonSplit.number_of_splits > 0:
      max_rows_in_blocklet = 1
      for i in six.moves.xrange(carbonSplit.number_of_splits):
        max_rows_in_blocklet = max(max_rows_in_blocklet, carbonSplit.pieces.__getitem__(i).num_rows)
      return min(shuffle_row_drop_partitions, max_rows_in_blocklet)
    return shuffle_row_drop_partitions

  def _create_ventilator(self, blocklet_indexes, shuffle_blocklets, shuffle_row_drop_partitions,
                         num_epochs, worker_predicate, max_ventilation_queue_size):
    items_to_ventilate = []
    for piece_index in blocklet_indexes:
      for shuffle_row_drop_partition in range(shuffle_row_drop_partitions):
        items_to_ventilate.append(
          {'piece_index': piece_index,
           'worker_predicate': worker_predicate,
           'shuffle_row_drop_partition': (shuffle_row_drop_partition,
                                          shuffle_row_drop_partitions)})

    return ConcurrentVentilator(self._workers_pool.ventilate,
                                items_to_ventilate,
                                iterations=num_epochs,
                                max_ventilation_queue_size=max_ventilation_queue_size,
                                randomize_item_order=shuffle_blocklets)

  def stop(self):
    """Stops all worker threads/processes."""
    self._workers_pool.stop()

  def join(self):
    """Joins all worker threads/processes. Will block until all worker workers have been fully terminated."""
    self._workers_pool.join()

  def cleanup(self):
    if not isinstance(self.cache, NullCache):
      self.cache.cleanup()

  def exit(self):
    self.stop()
    self.join()
    self.cleanup()

  @property
  def diagnostics(self):
    return self._workers_pool.diagnostics

  def __iter__(self):
    return self

  def __next__(self):
    try:
      return self._results_queue_reader.read_next(self._workers_pool, self.schema, self.ngram)
    except StopIteration:
      self.last_row_consumed = True
      raise

  def next(self):
    return self.__next__()

  # Functions needed to treat reader as a context manager
  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.stop()
    self.join()
    self.cleanup()
