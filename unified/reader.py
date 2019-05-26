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

from obs import ObsClient

from pycarbon.carbon_reader import make_carbon_reader, make_batch_carbon_reader


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
