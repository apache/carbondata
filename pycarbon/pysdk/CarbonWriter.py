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


class CarbonWriter(object):
  def __init__(self):
    from jnius import autoclass
    self.writerClass = autoclass('org.apache.carbondata.sdk.file.CarbonWriter')

  def builder(self):
    self.CarbonWriterBuilder = self.writerClass.builder()
    return self

  def outputPath(self, path):
    self.CarbonWriterBuilder.outputPath(path)
    return self

  def withCsvInput(self, jsonSchema):
    self.CarbonWriterBuilder.withCsvInput(jsonSchema)
    return self

  def writtenBy(self, name):
    self.CarbonWriterBuilder.writtenBy(name)
    return self

  def withLoadOption(self, key, value):
    self.CarbonWriterBuilder.withLoadOption(key, value)
    return self

  def withPageSizeInMb(self, value):
    self.CarbonWriterBuilder.withPageSizeInMb(value)
    return self

  def withHadoopConf(self, key, value):
    self.CarbonWriterBuilder.withHadoopConf(key, value)
    return self

  def build(self):
    self.writer = self.CarbonWriterBuilder.build()
    return self

  def write(self, data):
    return self.writer.write(data)

  def close(self):
    return self.writer.close()
