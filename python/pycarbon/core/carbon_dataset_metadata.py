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


import json
import logging
from contextlib import contextmanager

from six.moves import cPickle as pickle
from six.moves.urllib.parse import urlparse

from petastorm.etl.legacy import depickle_legacy_package_name_compatible
from petastorm.fs_utils import FilesystemResolver
from petastorm.unischema import Unischema
from petastorm.unischema import _numpy_and_codec_from_arrow_type
from petastorm.unischema import UnischemaField
from petastorm.etl.dataset_metadata import _init_spark, _cleanup_spark

from pycarbon.core.carbon import CarbonDataset
from pycarbon.core import carbon_utils

logger = logging.getLogger(__name__)

BLOCKLETS_PER_FILE_KEY = b'dataset-toolkit.num_blocklets_per_file.v1'
UNISCHEMA_KEY = b'dataset-toolkit.unischema.v1'


class PycarbonMetadataError(Exception):
  """
  Error to specify when the pycarbon metadata does not exist, does not contain the necessary information,
  or is corrupt/invalid.
  """

@contextmanager
def materialize_dataset_carbon(spark, dataset_url, schema, blocklet_size_mb=None, use_summary_metadata=False,
                               pyarrow_filesystem=None):
  """
  A Context Manager which handles all the initialization and finalization necessary
  to generate metadata for a pycarbon dataset. This should be used around your
  spark logic to materialize a dataset (specifically the writing of carbon output).

  Note: Any blocklet indexing should happen outside the materialize_dataset_carbon block

  Example:

  >>> spark = SparkSession.builder...
  >>> ds_url = 'hdfs:///path/to/my/dataset'
  >>> with materialize_dataset_carbon(spark, ds_url, MyUnischema, 64):
  >>>   spark.sparkContext.parallelize(range(0, 10)).
  >>>     ...
  >>>     .write.save(path=ds_url, format='carbon')

  A user may provide their own instance of pyarrow filesystem object in ``pyarrow_filesystem`` argument (otherwise,
  pycarbon will create a default one based on the url).

  The following example shows how a custom pyarrow HDFS filesystem, instantiated using ``libhdfs`` driver can be used
  during Pycarbon dataset generation:

  >>> resolver=FilesystemResolver(dataset_url, spark.sparkContext._jsc.hadoopConfiguration(),
  >>>                             hdfs_driver='libhdfs')
  >>> with materialize_dataset_carbon(..., pyarrow_filesystem=resolver.filesystem()):
  >>>     ...


  :param spark: The spark session you are using
  :param dataset_url: The dataset url to output your dataset to (e.g. ``hdfs:///path/to/dataset``)
  :param schema: The :class:`petastorm.unischema.Unischema` definition of your dataset
  :param blocklet_size_mb: The carbon blocklet size to use for your dataset
  :param use_summary_metadata: Whether to use the carbon summary metadata for blocklet indexing or a custom
    indexing method. The custom indexing method is more scalable for very large datasets.
  :param pyarrow_filesystem: A pyarrow filesystem object to be used when saving Pycarbon specific metadata to the
    Carbon store.

  """

  # After job completes, add the unischema metadata and check for the metadata summary file
  spark_config = {}
  _init_spark(spark, spark_config, blocklet_size_mb, use_summary_metadata)
  yield

  # After job completes, add the unischema metadata and check for the metadata summary file
  if pyarrow_filesystem is None:
    resolver = FilesystemResolver(dataset_url, spark.sparkContext._jsc.hadoopConfiguration())
    # filesystem = resolver.filesystem()
    dataset_path = resolver.get_dataset_path()
  else:
    # filesystem = pyarrow_filesystem
    dataset_path = urlparse(dataset_url).path

  carbon_dataset = CarbonDataset(dataset_path)
  _generate_unischema_metadata_carbon(carbon_dataset, schema)
  if not use_summary_metadata:
    _generate_num_blocklets_per_file_carbon(carbon_dataset, spark.sparkContext)

  _cleanup_spark(spark, spark_config, blocklet_size_mb)


def _generate_unischema_metadata_carbon(carbon_schema, schema):
  """
  Generates the serialized unischema and adds it to the dataset carbon metadata to be used upon reading.
  :param dataset: (CarbonDataset) Dataset to attach schema
  :param schema:  (Unischema) Schema to attach to dataset
  :return: None
  """
  # TODO: Simply pickling unischema will break if the UnischemaField class is changed,
  #  or the codec classes are changed. We likely need something more robust.
  serialized_schema = pickle.dumps(schema)
  carbon_utils.add_to_dataset_metadata_carbon(carbon_schema, UNISCHEMA_KEY, serialized_schema)


def _generate_num_blocklets_per_file_carbon(carbon_dataset, spark_context):
  """
  Generates the metadata file containing the number of blocklets in each file
  for the carbon dataset located at the dataset_url. It does this in spark by
  opening all carbon files in the dataset on the executors and collecting the
  number of blocklets in each file back on the driver.
  :param dataset: CarbonDataset
  :param spark_context: spark context to use for retrieving the number of blocklets
  in each carbon file in parallel
  :return: None, upon successful completion the metadata file will exist.
  """
  # if not isinstance(dataset.paths, str):
  #     raise ValueError('Expected dataset.paths to be a single path, not a list of paths')
  pieces = carbon_dataset.pieces
  # Get the common prefix of all the base path in order to retrieve a relative path
  paths = [piece.path for piece in pieces]

  # Needed pieces from the dataset must be extracted for spark because the dataset object is not serializable

  # TODO add number of blocklets for each carbonfile
  def get_carbon_blocklet_info(path):
    return path, 1

  number_of_blocklets = spark_context.parallelize(paths, len(paths)) \
    .map(get_carbon_blocklet_info) \
    .collect()
  number_of_blocklets_str = json.dumps(dict(number_of_blocklets))
  # Add the dict for the number of blocklets in each file to the carbon file metadata footer
  carbon_utils.add_to_dataset_metadata_carbon(carbon_dataset, BLOCKLETS_PER_FILE_KEY, number_of_blocklets_str)


def get_schema_carbon(carbon_dataset):
  """Retrieves schema object stored as part of dataset methadata.

  :param dataset: CarbonDataset
  :return: A :class:`petastorm.unischema.Unischema` object
  """
  if not carbon_dataset.common_metadata:
    raise PycarbonMetadataError(
      'Could not find _common_metadata file. Use materialize_dataset(..) in'
      ' pycarbon.etl.carbon_dataset_metadata.py to generate this file in your ETL code.'
      ' You can generate it on an existing dataset using pycarbon-generate-metadata.py')
  # TODO add pycarbon-generate-metadata.py

  dataset_metadata_dict = carbon_dataset.common_metadata.metadata

  # Read schema
  if UNISCHEMA_KEY not in dataset_metadata_dict:
    raise PycarbonMetadataError(
      'Could not find the unischema in the dataset common metadata file.'
      ' Please provide or generate dataset with the unischema attached.'
      ' Common Metadata file might not be generated properly.'
      ' Make sure to use materialize_dataset(..) in pycarbon.etl.carbon_dataset_metadata to'
      ' properly generate this file in your ETL code.'
      ' You can generate it on an existing dataset using pycarbon-generate-metadata.py')
  ser_schema = dataset_metadata_dict[UNISCHEMA_KEY]
  # Since we have moved the unischema class around few times, unpickling old schemas will not work. In this case we
  # override the old import path to get backwards compatibility

  schema = depickle_legacy_package_name_compatible(ser_schema)

  return schema


def get_schema_from_dataset_url_carbon(dataset_url,
                                       key=None,
                                       secret=None,
                                       endpoint=None,
                                       proxy=None,
                                       proxy_port=None,
                                       filesystem=None):
  """Returns a :class:`petastorm.unischema.Unischema` object loaded from a dataset specified by a url.

  :param dataset_url: A dataset URL
  :param key: access key
  :param secret: secret key
  :param endpoint: endpoint_url
  :param proxy: proxy
  :param proxy_port:  proxy_port
  :param filesystem: filesystem
  :return: A :class:`petastorm.unischema.Unischema` object
  """

  # Get a unischema stored in the dataset metadata.
  stored_schema = get_schema_carbon(CarbonDataset(dataset_url,
                                                  key=key,
                                                  secret=secret,
                                                  endpoint=endpoint,
                                                  proxy=proxy,
                                                  proxy_port=proxy_port,
                                                  filesystem=filesystem))

  return stored_schema


def infer_or_load_unischema_carbon(carbon_dataset):
  """Try to recover Unischema object stored by ``materialize_dataset`` function. If it can be loaded, infer
      Unischema from native Carbon schema"""
  try:
    return get_schema_carbon(carbon_dataset)
  except PycarbonMetadataError:
    logger.info('Failed loading Unischema from metadata in %s. Assuming the dataset was not created with '
                'Pycarbon. Will try to construct from native Carbon schema.')

    unischema_fields = []
    arrow_schema = carbon_dataset.schema
    for column_name in arrow_schema.names:
      arrow_field = arrow_schema.field_by_name(column_name)
      field_type = arrow_field.type
      codec, np_type = _numpy_and_codec_from_arrow_type(field_type)

      unischema_fields.append(UnischemaField(column_name, np_type, (), codec, arrow_field.nullable))
    return Unischema('inferred_schema', unischema_fields)
