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


class PaginationCarbonReader(object):
  def __init__(self):
    from jnius import autoclass
    self.readerClass = autoclass('org.apache.carbondata.sdk.file.PaginationCarbonReader')

  def builder(self, path, table_name):
    self.PaginationCarbonReaderBuilder = self.readerClass.builder(path, table_name)
    return self

  def projection(self, projection_list):
    self.PaginationCarbonReaderBuilder.projection(projection_list)
    return self

  def withHadoopConf(self, key, value):
    if "fs.s3a.access.key" == key:
      self.ak = value
    elif "fs.s3a.secret.key" == key:
      self.sk = value
    elif "fs.s3a.endpoint" == key:
      self.end_point = value
    elif "fs.s3a.proxy.host" == key:
      self.host = value
    elif "fs.s3a.proxy.port" == key:
      self.port = value

    self.PaginationCarbonReaderBuilder.withHadoopConf(key, value)
    return self

  def build(self):
    self.reader = self.PaginationCarbonReaderBuilder.buildPaginationReader()
    return self.reader

  def read(self, from_index, to_index):
    return self.reader.read(from_index, to_index)

  def getTotalRowsAsString(self):
    return self.reader.getTotalRowsAsString()

  def close(self):
    return self.reader.close()

