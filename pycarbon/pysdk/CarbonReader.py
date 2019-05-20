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


class CarbonReader(object):
  def __init__(self):
    from jnius import autoclass
    self.readerClass = autoclass('org.apache.carbondata.sdk.file.CarbonReader')

  def builder(self):
    self.CarbonReaderBuilder = self.readerClass.builder()
    return self

  def withFile(self, file_name):
    self.CarbonReaderBuilder.withFile(file_name)
    return self

  def withFileLists(self, file_lists):
    self.CarbonReaderBuilder.withFileLists(file_lists)
    return self

  def withFolder(self, file_name):
    self.CarbonReaderBuilder.withFolder(file_name)
    return self

  def withBatch(self, batch_size):
    self.CarbonReaderBuilder.withBatch(batch_size)
    return self

  def projection(self, projection_list):
    self.CarbonReaderBuilder.projection(projection_list)
    return self

  def filterEqual(self, column_name, value):
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
    self.CarbonReaderBuilder.withHadoopConf(key, value)
    return self

  def build(self):
    self.reader = self.CarbonReaderBuilder.build()
    return self

  def splitAsArray(self, maxSplits):
    return self.reader.split(maxSplits)

  def hasNext(self):
    return self.reader.hasNext()

  def readNextRow(self):
    return self.reader.readNextRow()

  def readNextBatchRow(self):
    return self.reader.readNextBatchRow()

  def readArrowBatch(self, schema):
    return self.reader.readArrowBatch(schema)

  def close(self):
    return self.reader.close()
