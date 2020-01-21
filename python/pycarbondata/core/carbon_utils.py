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


import logging
import os

import pyarrow

from pyarrow.filesystem import LocalFileSystem

logger = logging.getLogger(__name__)


def add_to_dataset_metadata_carbon(carbon_dataset, key, value):
  """
  Adds a key and value to the carbon metadata file of a carbon dataset.
  :param dataset: (CarbonDataset) carbon dataset
  :param key:     (str) key of metadata entry
  :param value:   (str) value of metadata
  """
  if not isinstance(carbon_dataset.path, str):
    raise ValueError('Expected dataset.paths to be a single path, not a list of paths')

  metadata_file_path = carbon_dataset.path.rstrip('/') + '/_metadata'
  common_metadata_file_path = carbon_dataset.path.rstrip('/') + '/_common_metadata'
  common_metadata_file_crc_path = carbon_dataset.path.rstrip('/') + '/._common_metadata.crc'

  # TODO currenlty usinf carbon to read and write _common_metadat, need to handle in carbon
  # If the metadata file already exists, add to it.
  # Otherwise fetch the schema from one of the existing carbon files in the dataset
  if carbon_dataset.fs.exists(common_metadata_file_path):
    with carbon_dataset.fs.open(common_metadata_file_path) as f:
      arrow_metadata = pyarrow.parquet.read_metadata(f)
      base_schema = arrow_metadata.schema.to_arrow_schema()
  elif carbon_dataset.fs.exists(metadata_file_path):
    # If just the metadata file exists and not the common metadata file, copy the contents of
    # the metadata file to the common_metadata file for backwards compatibility
    with carbon_dataset.fs.open(metadata_file_path) as f:
      arrow_metadata = pyarrow.parquet.read_metadata(f)
      base_schema = arrow_metadata.schema.to_arrow_schema()
  else:
    base_schema = carbon_dataset.schema

  # base_schema.metadata may be None, e.g.
  metadata_dict = base_schema.metadata or dict()
  metadata_dict[key] = value
  schema = base_schema.add_metadata(metadata_dict)

  with carbon_dataset.fs.open(common_metadata_file_path, 'wb') as metadata_file:
    pyarrow.parquet.write_metadata(schema, metadata_file)

  # We have just modified _common_metadata file, but the filesystem implementation used by pyarrow does not
  # update the .crc value. We better delete the .crc to make sure there is no mismatch between _common_metadata
  # content and the checksum.
  if isinstance(carbon_dataset.fs, LocalFileSystem) and carbon_dataset.fs.exists(common_metadata_file_crc_path):
    try:
      carbon_dataset.fs.rm(common_metadata_file_crc_path)
    except NotImplementedError:
      os.remove(common_metadata_file_crc_path)
