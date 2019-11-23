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


import pyarrow as pa
from modelarts import manifest
from modelarts.field_name import CARBON
from pyarrow.filesystem import (_ensure_filesystem)
from pyarrow.filesystem import (_get_fs_from_path)
from pyarrow.parquet import ParquetFile
from six.moves.urllib.parse import urlparse

from pycarbon.core.Constants import LOCAL_FILE_PREFIX
from pycarbon.sdk.ArrowCarbonReader import ArrowCarbonReader
from pycarbon.sdk.CarbonSchemaReader import CarbonSchemaReader
from pycarbon.sdk.Configuration import Configuration


class CarbonDataset(object):
  def __init__(self, path,
               key=None,
               secret=None,
               endpoint=None,
               proxy=None,
               proxy_port=None,
               filesystem=None):
    self.path = path
    self.url_path = urlparse(path)

    if str(path).endswith(".manifest"):
      self.manifest_path = path
      if str(path).startswith(LOCAL_FILE_PREFIX):
        self.manifest_path = str(path)[len(LOCAL_FILE_PREFIX):]

    if filesystem is None:
      a_path = self.path
      if isinstance(a_path, list):
        a_path = a_path[0]
      self.fs = _get_fs_from_path(a_path)
    else:
      self.fs = _ensure_filesystem(filesystem)

    self.pieces = list()

    if self.url_path.scheme == 's3a':
      if key is None or secret is None or endpoint is None:
        raise ValueError('key, secret, endpoint should not be None')

      if proxy is None and proxy_port is None:
        carbon_splits = ArrowCarbonReader().builder(self.path) \
          .withHadoopConf("fs.s3a.access.key", key) \
          .withHadoopConf("fs.s3a.secret.key", secret) \
          .withHadoopConf("fs.s3a.endpoint", endpoint) \
          .getSplits(True)

        configuration = Configuration()
        configuration.set("fs.s3a.access.key", key)
        configuration.set("fs.s3a.secret.key", secret)
        configuration.set("fs.s3a.endpoint", endpoint)

        self.configuration = configuration

      elif proxy is not None and proxy_port is not None:
        carbon_splits = ArrowCarbonReader().builder(self.path) \
          .withHadoopConf("fs.s3a.access.key", key) \
          .withHadoopConf("fs.s3a.secret.key", secret) \
          .withHadoopConf("fs.s3a.endpoint", endpoint) \
          .withHadoopConf("fs.s3a.proxy.host", proxy) \
          .withHadoopConf("fs.s3a.proxy.port", proxy_port) \
          .getSplits(True)

        configuration = Configuration()
        configuration.set("fs.s3a.access.key", key)
        configuration.set("fs.s3a.secret.key", secret)
        configuration.set("fs.s3a.endpoint", endpoint)
        configuration.set("fs.s3a.proxy.host", proxy)
        configuration.set("fs.s3a.proxy.port", proxy_port)

        self.configuration = configuration
      else:
        raise ValueError('wrong proxy & proxy_port configuration')

      if str(path).endswith(".manifest"):
        from obs import ObsClient
        obsClient = ObsClient(access_key_id=key, secret_access_key=secret,
                              server=str(endpoint).replace('http://', ''),
                              long_conn_mode=True)
        sources = manifest.getSources(self.manifest_path, CARBON, obsClient)
        if sources:
          self.file_path = sources[0]
        else:
          raise Exception("Manifest source can't be None!")
        carbon_schema = CarbonSchemaReader().readSchema(self.file_path, self.configuration.conf)
      else:
        carbon_schema = CarbonSchemaReader().readSchema(self.path, self.configuration.conf)

      for split in carbon_splits:
        # split = self.url_path.scheme + "://" + self.url_path.netloc + split
        folder_path = path
        if str(path).endswith(".manifest"):
          folder_path = str(self.file_path)[0:(str(self.file_path).rindex('/'))]
        self.pieces.append(CarbonDatasetPiece(folder_path, carbon_schema, split,
                                              key=key, secret=secret, endpoint=endpoint,
                                              proxy=proxy, proxy_port=proxy_port))

    else:
      if str(path).endswith(".manifest"):
        sources = manifest.getSources(self.manifest_path, CARBON)
        if sources:
          self.file_path = sources[0]
        else:
          raise Exception("Manifest source can't be None!")

        try:
          carbon_schema = CarbonSchemaReader().readSchema(self.file_path)
        except:
          raise Exception("readSchema has some errors: " + self.file_path)
      else:
        try:
          carbon_schema = CarbonSchemaReader().readSchema(self.path)
        except:
          raise Exception("readSchema has some errors")

      carbon_splits = ArrowCarbonReader().builder(self.path) \
        .getSplits(True)

      for split in carbon_splits:
        # split = self.url_path.scheme + "://" + self.url_path.netloc + split
        if str(path).endswith(".manifest"):
          self.pieces.append(
            CarbonDatasetPiece(str(self.file_path)[0:(str(self.file_path).rindex('/'))], carbon_schema, split))
        else:
          self.pieces.append(CarbonDatasetPiece(path, carbon_schema, split))

    self.number_of_splits = len(self.pieces)
    self.schema = self.getArrowSchema()
    # TODO add mechanism to get the file path based on file filter
    self.common_metadata_path = self.url_path.path + '/_common_metadata'
    self.common_metadata = None
    try:
      if self.fs.exists(self.common_metadata_path):
        with self.fs.open(self.common_metadata_path) as f:
          self.common_metadata = ParquetFile(f).metadata
    except:
      self.common_metadata = None

  def getArrowSchema(self):
    file_path = self.path

    if str(self.path).endswith(".manifest"):
      file_path = self.file_path
    if self.url_path.scheme == 's3a':
      buf = CarbonSchemaReader().readSchema(file_path, True, self.configuration.conf).tostring()
    else:
      buf = CarbonSchemaReader().readSchema(file_path, True).tostring()

    reader = pa.RecordBatchFileReader(pa.BufferReader(bytes(buf)))
    return reader.read_all().schema


class CarbonDatasetPiece(object):
  def __init__(self, path, carbon_schema, input_split,
               key=None,
               secret=None,
               endpoint=None,
               proxy=None,
               proxy_port=None):
    self.path = path
    self.url_path = urlparse(path)
    self.input_split = input_split
    self.carbon_schema = carbon_schema
    # TODO get record count from carbonapp based on file
    self.num_rows = 10000
    self.use_s3 = False

    if self.url_path.scheme == 's3a':
      self.use_s3 = True

      if key is None or secret is None or endpoint is None:
        raise ValueError('key, secret, endpoint should not be None')

      self.key = key
      self.secret = secret
      self.endpoint = endpoint

      if proxy is None and proxy_port is None:
        self.proxy = proxy
        self.proxy_port = proxy_port
      elif proxy is not None and proxy_port is not None:
        self.proxy = proxy
        self.proxy_port = proxy_port
      else:
        raise ValueError('wrong proxy & proxy_port configuration')

  def read_all(self, columns):
    # rebuilding the reader as need to read specific columns
    carbon_reader_builder = ArrowCarbonReader().builder(self.input_split)
    carbon_schema_reader = CarbonSchemaReader()
    if columns is not None:
      carbon_reader_builder = carbon_reader_builder.projection(columns)
      updatedSchema = carbon_schema_reader.reorderSchemaBasedOnProjection(columns, self.carbon_schema)
    else:
      # TODO Currently when projection is not added in carbon reader
      # carbon returns record in dimensions+measures,but here we need based on actual schema order
      # so for handling this adding projection columns based on schema
      updatedSchema = self.carbon_schema
      projection = carbon_schema_reader.getProjectionBasedOnSchema(updatedSchema)
      carbon_reader_builder = carbon_reader_builder.projection(projection)

    if self.use_s3:
      if self.proxy is None and self.proxy_port is None:
        carbon_reader = carbon_reader_builder \
          .withHadoopConf("fs.s3a.access.key", self.key) \
          .withHadoopConf("fs.s3a.secret.key", self.secret) \
          .withHadoopConf("fs.s3a.endpoint", self.endpoint) \
          .build()
      else:
        carbon_reader = carbon_reader_builder \
          .withHadoopConf("fs.s3a.access.key", self.key) \
          .withHadoopConf("fs.s3a.secret.key", self.secret) \
          .withHadoopConf("fs.s3a.endpoint", self.endpoint) \
          .withHadoopConf("fs.s3a.proxy.host", self.proxy) \
          .withHadoopConf("fs.s3a.proxy.port", self.proxy_port) \
          .build()
    else:
      carbon_reader = carbon_reader_builder.build()

    data = carbon_reader.read(updatedSchema)
    carbon_reader.close()
    return data
