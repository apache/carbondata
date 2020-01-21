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


class CarbonWriter(object):
  """
  How to create CarbonWriter:
  1. new CarbonWriter() and call .builder() first
  2. call different configuration, like  .outputPath(path)
  3. call .build() to create CarbonWriter
  4. write data by write()
  5. call close() to write data to local/HDFS/S3
  """
  def __init__(self):
    from jnius import autoclass
    self.writerClass = autoclass('org.apache.carbondata.sdk.file.CarbonWriter')

  def builder(self):
    self.CarbonWriterBuilder = self.writerClass.builder()
    return self

  def outputPath(self, path):
    """
    Sets the output path of the writer builder

    :param path: is the absolute path where output files are written
                This method must be called when building CarbonWriterBuilder
    :return: updated CarbonWriter
    """
    self.CarbonWriterBuilder.outputPath(path)
    return self

  def withCsvInput(self, jsonSchema):
    """
    accepts row in CSV format

    :param jsonSchema:json format schema
    :return: updated CarbonWriter object
    """
    self.CarbonWriterBuilder.withCsvInput(jsonSchema)
    return self

  def writtenBy(self, name):
    """
    Record the name who write this CarbonData file
    :param name: The name is writing the CarbonData files
    :return:  updated CarbonWriter object
    """
    self.CarbonWriterBuilder.writtenBy(name)
    return self

  def withLoadOption(self, key, value):
    """
    To support the load options for sdk writer

    :param key: the key of load option
    :param value:  the value of load option
    :return:  updated CarbonWriter object
    """
    self.CarbonWriterBuilder.withLoadOption(key, value)
    return self

  def withPageSizeInMb(self, value):
    """
     To set the blocklet size of CarbonData file

    :param value: is page size in MB
    :return: updated CarbonWriter
    """
    self.CarbonWriterBuilder.withPageSizeInMb(value)
    return self

  def withHadoopConf(self, key, value):
    """
    To support hadoop configuration, can set s3a AK,SK,end point and other conf with this

    :param key: key word  of configuration
    :param value: value of configuration
    :return: updated CarbonWriter
    """
    self.CarbonWriterBuilder.withHadoopConf(key, value)
    return self

  def build(self):
    """
    Build a  CarbonWriter
    This writer is not thread safe,
    use withThreadSafe() configuration in multi thread environment
    :return:
    """
    self.writer = self.CarbonWriterBuilder.build()
    return self

  def write(self, data):
    """
    Write an object to the file, the format of the object depends on the implementation.
    :param data:
    :return:
    """
    return self.writer.write(data)

  def close(self):
    """
     Flush and close the writer

    :return:
    """
    return self.writer.close()
