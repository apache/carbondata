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


class CarbonSchemaReader(object):
  """
  How to use it:
  1. create CarbonSchemaReader: carbonSchemaReader = CarbonSchemaReader()
  2. read schema from path: schema = carbonSchemaReader.readSchema(path)
  path can be a folder, carbonindex file and carbondata file.
  """
  def __init__(self):
    from jnius import autoclass
    self.carbonSchemaReader = autoclass('org.apache.carbondata.sdk.file.CarbonSchemaReader')
    self.Schema = autoclass('org.apache.carbondata.sdk.file.Schema')

  def readSchema(self, path, getAsBuffer=False, validateSchema=False, conf=None):
    """
    Read CarbonData schema from path.
    :param path: data path, path can be a folder, carbonindex file name and carbondata file name
    :param getAsBuffer:  whether get as buffer
    :param validateSchema: whether validate schema.
    :param conf: configuration for ak, sk, endpoint and so on.
    :return: CarbonData schema.
    """
    if getAsBuffer == True:
      return self.carbonSchemaReader.getArrowSchemaAsBytes(path)
    if conf is None:
      schema = self.carbonSchemaReader.readSchema(path, validateSchema)
    else:
      schema = self.carbonSchemaReader.readSchema(path, validateSchema, conf)
    newSchema = schema.asOriginOrder()
    return newSchema

  def reorderSchemaBasedOnProjection(self, columns, schema):
    fields = schema.getFields()
    updateFields = list()
    for column in columns:
      for field in fields:
        if column.casefold() == field.getFieldName().casefold():
          updateFields.append(field)
          break

    updatedSchema = self.Schema(updateFields)
    return updatedSchema

  def getProjectionBasedOnSchema(self, schema):
    fields = schema.getFields()
    projection = list()
    for field in fields:
      projection.append(field.getFieldName())
    return projection
