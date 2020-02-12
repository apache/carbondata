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


from obs import ObsClient

from pycarbon.core.carbon_reader import make_carbon_reader, make_batch_carbon_reader
from pycarbon.integration.pytorch import decimal_friendly_collate, DataLoader
from pycarbon.integration.tensorflow import TensorFlow


def make_reader(dataset_url=None,
                workers_count=10,
                results_queue_size=100,
                num_epochs=1,
                obs_client=None,
                shuffle=True,
                schema_fields=None,
                is_batch=True,
                reader_pool_type='thread',
                data_format='carbon',
                cache_properties={'cache_type': None, 'cache_location': None, 'cache_size_limit': None,
                                  'cache_row_size_estimate': None, 'cache_extra_settings': None},
                **properties
                ):
  """
  an unified api for different data format dataset

  :param dataset_url: an filepath or a url to a carbon directory,
      e.g. ``'hdfs://some_hdfs_cluster/user/yevgeni/carbon8'``, or ``'file:///tmp/mydataset'``
      or ``'s3a://bucket/mydataset'``.
  :param data_format: dataset data format (default: carbon)
  :param is_batch: return single record or batch records (default: True)
  :param obs_client: obs client object
    access key
    secret key
    endpoint_url
  :param schema_fields: Can be: a list of unischema fields and/or regex pattern strings; ``None`` to read all fields;
          an NGram object, then it will return an NGram of the specified fields.
  :param reader_pool_type: A string denoting the reader pool type. Should be one of ['thread', 'process', 'dummy']
      denoting a thread pool, process pool, or running everything in the master thread. Defaults to 'thread'
    TODO: process support
  :param workers_count: An int for the number of workers to use in the reader pool. This only is used for the
      thread or process pool. Defaults to 10
  :param results_queue_size: Size of the results queue to store prefetched rows. Currently only applicable to
      thread reader pool type.
  :param shuffle: Whether to shuffle partition (the order in which full partition are read)
  :param num_epochs: An epoch is a single pass over all rows in the dataset. Setting ``num_epochs`` to
      ``None`` will result in an infinite number of epochs.
  :param cache_properties: a dict of cache parameters
    cache_type: A string denoting the cache type, if desired. Options are [None, 'null', 'local-disk', 'memory-cache']
      to either have a null/noop cache or a cache implemented using diskcache. Caching is useful when communication
      to the main data store is either slow or expensive and the local machine has large enough storage
      to store entire dataset. By default will be a null cache.
    cache_location: A string denoting the location or path of the cache.
    cache_size_limit: An int specifying the size limit of the cache in bytes
    cache_row_size_estimate: An int specifying the estimated size of a row in the dataset
    cache_extra_settings: A dictionary of extra settings to pass to the cache implementation,
  :param **properties: other parameters (using dict)
  :return: A :class:`Reader` object
  """

  if is_batch is True:
    if data_format == 'carbon':
      if isinstance(obs_client, ObsClient):
        if obs_client.is_secure is True:
          endpoint = "https://" + obs_client.server
        else:
          endpoint = "http://" + obs_client.server
        return make_batch_carbon_reader(dataset_url,
                                        key=obs_client.securityProvider.access_key_id,
                                        secret=obs_client.securityProvider.secret_access_key,
                                        endpoint=endpoint,
                                        proxy=obs_client.proxy_host,
                                        proxy_port=obs_client.proxy_port,
                                        schema_fields=schema_fields,
                                        reader_pool_type=reader_pool_type,
                                        workers_count=workers_count,
                                        results_queue_size=results_queue_size,
                                        shuffle_blocklets=shuffle,
                                        num_epochs=num_epochs,
                                        cache_type=cache_properties['cache_type'],
                                        cache_location=cache_properties['cache_location'],
                                        cache_size_limit=cache_properties['cache_size_limit'],
                                        cache_row_size_estimate=cache_properties['cache_row_size_estimate'],
                                        cache_extra_settings=cache_properties['cache_extra_settings'],
                                        **properties)
      elif obs_client is None:
        return make_batch_carbon_reader(dataset_url,
                                        schema_fields=schema_fields,
                                        reader_pool_type=reader_pool_type,
                                        workers_count=workers_count,
                                        results_queue_size=results_queue_size,
                                        shuffle_blocklets=shuffle,
                                        num_epochs=num_epochs,
                                        cache_type=cache_properties['cache_type'],
                                        cache_location=cache_properties['cache_location'],
                                        cache_size_limit=cache_properties['cache_size_limit'],
                                        cache_row_size_estimate=cache_properties['cache_row_size_estimate'],
                                        cache_extra_settings=cache_properties['cache_extra_settings'],
                                        **properties)
      else:
        raise ValueError("""obs_client should be a ObsClient object or None""")
    else:
      raise NotImplementedError("""not support other data format datset""")

  elif is_batch is False:
    if data_format == 'carbon':
      if isinstance(obs_client, ObsClient):
        if obs_client.is_secure is True:
          endpoint = "https://" + obs_client.server
        else:
          endpoint = "http://" + obs_client.server
        return make_carbon_reader(dataset_url,
                                  key=obs_client.securityProvider.access_key_id,
                                  secret=obs_client.securityProvider.secret_access_key,
                                  endpoint=endpoint,
                                  proxy=obs_client.proxy_host,
                                  proxy_port=obs_client.proxy_port,
                                  schema_fields=schema_fields,
                                  reader_pool_type=reader_pool_type,
                                  workers_count=workers_count,
                                  results_queue_size=results_queue_size,
                                  shuffle_blocklets=shuffle,
                                  num_epochs=num_epochs,
                                  cache_type=cache_properties['cache_type'],
                                  cache_location=cache_properties['cache_location'],
                                  cache_size_limit=cache_properties['cache_size_limit'],
                                  cache_row_size_estimate=cache_properties['cache_row_size_estimate'],
                                  cache_extra_settings=cache_properties['cache_extra_settings'],
                                  **properties)
      elif obs_client is None:
        return make_carbon_reader(dataset_url,
                                  schema_fields=schema_fields,
                                  reader_pool_type=reader_pool_type,
                                  workers_count=workers_count,
                                  results_queue_size=results_queue_size,
                                  shuffle_blocklets=shuffle,
                                  num_epochs=num_epochs,
                                  cache_type=cache_properties['cache_type'],
                                  cache_location=cache_properties['cache_location'],
                                  cache_size_limit=cache_properties['cache_size_limit'],
                                  cache_row_size_estimate=cache_properties['cache_row_size_estimate'],
                                  cache_extra_settings=cache_properties['cache_extra_settings'],
                                  **properties)
      else:
        raise ValueError("""obs_client should be a ObsClient object or None""")
    else:
      raise NotImplementedError("""not support other data format datset""")

  else:
    raise ValueError("""the value of is_batch is invalid, it should be set True or False""")


def make_dataset(reader):
  """Creates a `tensorflow.data.Dataset <https://www.tensorflow.org/api_docs/python/tf/data/Dataset>`_ object from

  NGrams are not yet supported by this function.

  :param reader: An instance of :class:`Reader` object that would serve as a data source.
  :return: A ``tf.data.Dataset`` instance.
  """
  tensorflow = TensorFlow()
  return tensorflow.make_dataset(reader)


def make_tensor(reader, shuffling_queue_capacity=0, min_after_dequeue=0):
  """Bridges between python-only interface of the Reader (next(Reader)) and tensorflow world.

  This function returns a named tuple of tensors from the dataset, e.g.,

  If the reader was created with ``ngram=NGram(...)`` parameter, then a dictionary of named tuples is returned
  (indexed by time):

  An optional shuffling queue is created if shuffling_queue_capacity is greater than 0.

  Note that if reading a unischema field that is unicode (``np.unicode_`` or ``np.str_``) tensorflow will
  represent it as a tf.string which will be an array of bytes. If using python3 you may need to decode
  it to convert it back to a python str type.

  :param reader: An instance of Reader object used as the data source
  :param shuffling_queue_capacity: Queue capacity is passed to the underlying :class:`tf.RandomShuffleQueue`
      instance. If set to 0, no suffling will be done.
  :param min_after_dequeue: If ``shuffling_queue_capacity > 0``, this value is passed to the underlying
      :class:`tf.RandomShuffleQueue`.
  :return: If no ngram reading is used, the function will return a named tuple with tensors that are populated
      from the underlying dataset. If ngram reading is enabled, a dictionary of named tuples of tensors is returned.
      The dictionary is indexed by time.
  """
  tensorflow = TensorFlow()
  return tensorflow.make_tensor(reader, shuffling_queue_capacity, min_after_dequeue)


def make_data_loader(reader, batch_size=1, collate_fn=decimal_friendly_collate):
  """
  Initializes a data loader object, with a default collate.

  Number of epochs is defined by the configuration of the reader argument.

  :param reader: PyCarbon Reader instance
  :param batch_size: the number of items to return per batch; factored into the len() of this reader
  :param collate_fn: an optional callable to merge a list of samples to form a mini-batch.
  """
  return DataLoader(reader, batch_size=batch_size, collate_fn=collate_fn)
