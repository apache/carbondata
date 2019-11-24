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


class CarbonReader(object):
  """
  How to create CarbonReader:
  1. new CarbonReader() and call .builder() first
  2. call different configuration, like  .withFolder(path)
  3. call .build() to create CarbonReader
  4. read data by hasNext(), readNextRow(), readNextBatchRow(), readArrowBatch()
  """

  def __init__(self):
    from jnius import autoclass
    self.readerClass = autoclass('org.apache.carbondata.sdk.file.CarbonReader')

  def builder(self):
    """
    :return: updated CarbonReader
    """
    self.CarbonReaderBuilder = self.readerClass.builder()
    return self

  def withFile(self, file_name):
    """
    read carbonData file from the file

    :param file_name: CarbonData file name
    :return: updated CarbonReader
    """
    self.CarbonReaderBuilder.withFile(file_name)
    return self

  def withFileLists(self, file_lists):
    """
    read carbonData file from the file list

    :param file_lists: CarbonData file list
    :return: updated CarbonReader
    """
    self.CarbonReaderBuilder.withFileLists(file_lists)
    return self

  def withFolder(self, folder_name):
    """
    read carbonData file from this folder

    :param file_name: folder name
    :return: updated CarbonReader
    """
    self.CarbonReaderBuilder.withFolder(folder_name)
    return self

  def withBatch(self, batch_size):
    """

    :param batch_size:
    :return: updated CarbonReader
    """
    self.CarbonReaderBuilder.withBatch(batch_size)
    return self

  def projection(self, projection_list):
    """
    Configure the projection column names of carbon reader

    :param projection_list: the list of projection column names
    :return: updated CarbonReader
    """
    self.CarbonReaderBuilder.projection(projection_list)
    return self

  def filterEqual(self, column_name, value):
    """
    filter column name equal value
    :param column_name: column_name in CarbonData file
    :param value: the value of column_name
    :return: updated CarbonReader
    """
    from jnius import autoclass
    equal_to_expression_class = autoclass('org.apache.carbondata.core.scan.expression.conditional.EqualToExpression')
    data_types_class = autoclass('org.apache.carbondata.core.metadata.datatype.DataTypes')
    column_expression_class = autoclass('org.apache.carbondata.core.scan.expression.ColumnExpression')
    literal_expression_class = autoclass('org.apache.carbondata.core.scan.expression.LiteralExpression')

    column_expression = column_expression_class(column_name, data_types_class.STRING)
    literal_expression = literal_expression_class(value, data_types_class.STRING)
    equal_to_expression = equal_to_expression_class(column_expression, literal_expression)

    self.CarbonReaderBuilder.filter(equal_to_expression)
    return self

  def withHadoopConf(self, key, value):
    """
    To support hadoop configuration, can set s3a AK,SK,end point and other conf with this

    :param key: key word  of configuration
    :param value: value of configuration
    :return: updated CarbonReader
    """
    self.CarbonReaderBuilder.withHadoopConf(key, value)
    return self

  def build(self):
    """
    Build CarbonReader

    :return:
    """
    self.reader = self.CarbonReaderBuilder.build()
    return self

  def splitAsArray(self, maxSplits):
    return self.reader.split(maxSplits)

  def hasNext(self):
    """
    Return true if has next row

    :return:
    """
    return self.reader.hasNext()

  def readNextRow(self):
    """
    Read and return next row object

    :return:
    """
    return self.reader.readNextRow()

  def readNextBatchRow(self):
    """
    Read and return next batch row objects

    :return:
    """
    return self.reader.readNextBatchRow()

  def readArrowBatch(self, schema):
    return self.reader.readArrowBatch(schema)

  def close(self):
    """
    Close reader

    :return:
    """
    return self.reader.close()
