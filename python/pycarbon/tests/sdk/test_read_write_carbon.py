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

import pytest

from pycarbon.sdk.CarbonReader import CarbonReader
from pycarbon.sdk.CarbonSchemaReader import CarbonSchemaReader
from pycarbon.sdk.CarbonWriter import CarbonWriter

import base64
import time
import shutil
import os
import jnius_config

jnius_config.set_classpath("../../../sdk/sdk/target/carbondata-sdk.jar")
IMAGE_DATA_PATH = "./resources"

def test_run_write_carbon():
  jsonSchema = "[{stringField:string},{shortField:short},{intField:int}]"
  path = "/tmp/data/writeCarbon" + str(time.time())

  if os.path.exists(path):
    shutil.rmtree(path)

  writer = CarbonWriter() \
    .builder() \
    .outputPath(path) \
    .withCsvInput(jsonSchema) \
    .writtenBy("pycarbon") \
    .build()

  for i in range(0, 10):
    from jnius import autoclass
    arrayListClass = autoclass("java.util.ArrayList")
    data_list = arrayListClass()
    data_list.add("pycarbon")
    data_list.add(str(i))
    data_list.add(str(i * 10))
    writer.write(data_list.toArray())

  writer.close()

  reader = CarbonReader() \
    .builder() \
    .withFolder(path) \
    .withBatch(1000) \
    .build()

  i = 0
  while reader.hasNext():
    rows = reader.readNextBatchRow()
    i += len(rows)

  assert 10 == i
  reader.close()


  carbonSchemaReader = CarbonSchemaReader()
  schema = carbonSchemaReader.readSchema(path)

  assert 3 == schema.getFieldsLength()


  writer = CarbonWriter() \
    .builder() \
    .outputPath(path) \
    .withCsvInput(jsonSchema) \
    .writtenBy("pycarbon") \
    .build()

  for i in range(0, 10):
    from jnius import autoclass
    arrayListClass = autoclass("java.util.ArrayList")
    data_list = arrayListClass()
    data_list.add("pycarbon")
    data_list.add(str(i))
    data_list.add(str(i * 10))
    writer.write(data_list.toArray())

  writer.close()

  carbonSchemaReader = CarbonSchemaReader()
  schema = carbonSchemaReader.readSchema(getAsBuffer=False, path=path, validateSchema=True)

  assert 3 == schema.getFieldsLength()

  shutil.rmtree(path)


def test_run_write_carbon_binary_base64_encode():
  jsonSchema = "[{stringField:string},{shortField:short},{intField:int},{binaryField:binary}]"
  path = "/tmp/data/writeCarbon" + str(time.time())

  if os.path.exists(path):
    shutil.rmtree(path)

  jpg_path = IMAGE_DATA_PATH + "/carbondatalogo.jpg"

  writer = CarbonWriter() \
    .builder() \
    .outputPath(path) \
    .withCsvInput(jsonSchema) \
    .writtenBy("pycarbon") \
    .build()

  with open(jpg_path, mode='rb+') as file_object:
    content = file_object.read()

  for i in range(0, 10):
    from jnius import autoclass

    arrayListClass = autoclass("java.util.ArrayList")
    data_list = arrayListClass()
    data_list.add("pycarbon")
    data_list.add(str(i))
    data_list.add(str(i * 10))
    data_list.add(base64.b64encode(content))
    writer.write(data_list.toArray())

  writer.close()

  reader = CarbonReader() \
    .builder() \
    .withFolder(path) \
    .withBatch(1000) \
    .build()

  i = 0
  while reader.hasNext():
    rows = reader.readNextBatchRow()
    for row in rows:
      i += 1
      for column in row:
        from jnius.jnius import ByteArray
        if 1 == i and isinstance(column, ByteArray) and len(column) > 1000:
          with open(path + "/image.jpg", 'wb+') as file_object:
            file_object.write(base64.b64decode(column.tostring()))

  assert 10 == i
  reader.close()

  shutil.rmtree(path)


# TODO: to be supported
@pytest.mark.skip("write binary to be supported")
def test_run_write_carbon_binary():
  jsonSchema = "[{stringField:string},{shortField:short},{intField:int},{binaryField:binary}]"
  path = "/tmp/data/writeCarbon" + str(time.time())

  if os.path.exists(path):
    shutil.rmtree(path)

  jpg_path = IMAGE_DATA_PATH + "/carbondatalogo.jpg"

  writer = CarbonWriter() \
    .builder() \
    .outputPath(path) \
    .withCsvInput(jsonSchema) \
    .writtenBy("pycarbon") \
    .build()

  with open(jpg_path, mode='rb+') as file_object:
    content = file_object.read()

  for i in range(0, 10):
    from jnius import autoclass

    arrayListClass = autoclass("java.util.ArrayList")
    data_list = arrayListClass()
    data_list.add("pycarbon")
    data_list.add(str(i))
    data_list.add(str(i * 10))
    data_list.add(content)
    writer.write(data_list.toArray())

  writer.close()

  reader = CarbonReader() \
    .builder() \
    .withFolder(path) \
    .withBatch(1000) \
    .build()

  i = 0
  while reader.hasNext():
    rows = reader.readNextBatchRow()
    for row in rows:
      i += 1
      for column in row:
        from jnius.jnius import ByteArray
        if 1 == i and isinstance(column, ByteArray) and len(column) > 1000:
          with open(path + "/image.jpg", 'wb+') as file_object:
            file_object.write(column.tostring())

  assert 10 == i
  reader.close()

  shutil.rmtree(path)


def test_run_write_carbon_binary_base64_encode_many_files():
  jsonSchema = "[{stringField:string},{shortField:short},{intField:int},{binaryField:binary},{txtField:string}]"
  path = "/tmp/data/writeCarbon" + str(time.time())

  if os.path.exists(path):
    shutil.rmtree(path)

  jpg_path = IMAGE_DATA_PATH + "/flowers"

  from jnius import autoclass

  sdkUtilClass = autoclass("org.apache.carbondata.sdk.file.utils.SDKUtil")
  jpg_files = sdkUtilClass.listFiles(jpg_path, '.jpg')

  writer = CarbonWriter() \
    .builder() \
    .outputPath(path) \
    .withCsvInput(jsonSchema) \
    .writtenBy("pycarbon") \
    .build()

  for i in range(0, jpg_files.size()):
    jpg_path = jpg_files.get(i)
    with open(jpg_path, mode='rb+') as file_object:
      content = file_object.read()

    with open(str(jpg_path).replace('.jpg', '.txt'), mode='r+') as file_object:
      txt = file_object.read()

    arrayListClass = autoclass("java.util.ArrayList")
    data_list = arrayListClass()
    data_list.add("pycarbon")
    data_list.add(str(i))
    data_list.add(str(i * 10))
    data_list.add(base64.b64encode(content))
    data_list.add(txt)
    writer.write(data_list.toArray())

  writer.close()

  reader = CarbonReader() \
    .builder() \
    .withFolder(path) \
    .build()

  i = 0
  while reader.hasNext():
    rows = reader.readNextBatchRow()

    for row in rows:
      i += 1
      for column in row:
        from jnius.jnius import ByteArray
        if isinstance(column, ByteArray) and len(column) > 1000:
          with open(path + "/image" + str(i) + ".jpg", 'wb+') as file_object:
            file_object.write(base64.b64decode(column.tostring()))

  assert 3 == i
  reader.close()

  shutil.rmtree(path)


def test_run_write_carbon_binary_base64_encode_voc():
  jsonSchema = "[{stringField:string},{shortField:short},{intField:int},{binaryField:binary},{txtField:string}]"
  path = "/tmp/data/writeCarbon" + str(time.time())

  if os.path.exists(path):
    shutil.rmtree(path)

  jpg_path = IMAGE_DATA_PATH + "/voc"

  from jnius import autoclass

  sdkUtilClass = autoclass("org.apache.carbondata.sdk.file.utils.SDKUtil")
  jpg_files = sdkUtilClass.listFiles(jpg_path, '.jpg')

  writer = CarbonWriter() \
    .builder() \
    .outputPath(path) \
    .withCsvInput(jsonSchema) \
    .writtenBy("pycarbon") \
    .build()

  for i in range(0, jpg_files.size()):
    jpg_path = jpg_files.get(i)
    with open(jpg_path, mode='rb+') as file_object:
      content = file_object.read()

    with open(str(jpg_path).replace('.jpg', '.xml'), mode='r+') as file_object:
      txt = file_object.read()

    arrayListClass = autoclass("java.util.ArrayList")
    data_list = arrayListClass()
    data_list.add("pycarbon")
    data_list.add(str(i))
    data_list.add(str(i * 10))
    data_list.add(base64.b64encode(content))
    data_list.add(txt)
    writer.write(data_list.toArray())

  writer.close()

  reader = CarbonReader() \
    .builder() \
    .withFolder(path) \
    .withBatch(1000) \
    .build()

  i = 0
  while reader.hasNext():
    rows = reader.readNextBatchRow()
    for row in rows:
      i += 1
      for column in row:
        from jnius.jnius import ByteArray
        if isinstance(column, ByteArray) and len(column) > 1000:
          with open(path + "/image" + str(i) + ".jpg", 'wb+') as file_object:
            file_object.write(base64.b64decode(column.tostring()))

  assert 5 == i
  reader.close()

  shutil.rmtree(path)


def test_run_write_carbon_binary_base64_encode_vocForSegmentationClass():
  jsonSchema = "[{stringField:string},{shortField:short},{intField:int},{binaryField:binary},{segField:binary}]"
  path = "/tmp/data/writeCarbon" + str(time.time())

  if os.path.exists(path):
    shutil.rmtree(path)

  jpg_path = IMAGE_DATA_PATH + "/vocForSegmentationClass"

  from jnius import autoclass

  sdkUtilClass = autoclass("org.apache.carbondata.sdk.file.utils.SDKUtil")
  jpg_files = sdkUtilClass.listFiles(jpg_path, '.jpg')

  writer = CarbonWriter() \
    .builder() \
    .outputPath(path) \
    .withCsvInput(jsonSchema) \
    .writtenBy("pycarbon") \
    .build()

  for i in range(0, jpg_files.size()):
    jpg_path = jpg_files.get(i)
    with open(jpg_path, mode='rb+') as file_object:
      content = file_object.read()

    with open(str(jpg_path).replace('.jpg', '.png'), mode='rb+') as file_object:
      png_data = file_object.read()

    arrayListClass = autoclass("java.util.ArrayList")
    data_list = arrayListClass()
    data_list.add("pycarbon")
    data_list.add(str(i))
    data_list.add(str(i * 10))
    data_list.add(base64.b64encode(content))
    data_list.add(base64.b64encode(png_data))
    writer.write(data_list.toArray())

  writer.close()

  reader = CarbonReader() \
    .builder() \
    .withFolder(path) \
    .withBatch(1000) \
    .build()

  i = 0
  while reader.hasNext():
    rows = reader.readNextBatchRow()
    for row in rows:
      i += 1
      num = 0
      for column in row:
        num += 1
        from jnius.jnius import ByteArray
        if isinstance(column, ByteArray) and len(column) > 1000:
          with open(path + "/image" + str(i) + "_" + str(num) + ".jpg", 'wb+') as file_object:
            file_object.write(base64.b64decode(column.tostring()))

  assert 3 == i
  reader.close()

  shutil.rmtree(path)


def test_run_write_carbon_binary_base64_encode_decodeInJava_many_files():
  jsonSchema = "[{stringField:string},{shortField:short},{intField:int},{binaryField:binary},{txtField:string}]"
  path = "/tmp/data/writeCarbon" + str(time.time())

  if os.path.exists(path):
    shutil.rmtree(path)

  jpg_path = IMAGE_DATA_PATH + "/flowers"

  from jnius import autoclass

  sdkUtilClass = autoclass("org.apache.carbondata.sdk.file.utils.SDKUtil")
  jpg_files = sdkUtilClass.listFiles(jpg_path, '.jpg')

  writer = CarbonWriter() \
    .builder() \
    .outputPath(path) \
    .withCsvInput(jsonSchema) \
    .writtenBy("pycarbon") \
    .withLoadOption("binary_decoder", "base64") \
    .withPageSizeInMb(1) \
    .build()

  for i in range(0, jpg_files.size()):
    jpg_path = jpg_files.get(i)
    with open(jpg_path, mode='rb+') as file_object:
      content = file_object.read()

    with open(str(jpg_path).replace('.jpg', '.txt'), mode='r+') as file_object:
      txt = file_object.read()

    arrayListClass = autoclass("java.util.ArrayList")
    data_list = arrayListClass()
    data_list.add("pycarbon")
    data_list.add(str(i))
    data_list.add(str(i * 10))
    data_list.add(base64.b64encode(content))
    data_list.add(txt)
    writer.write(data_list.toArray())

  writer.close()

  reader = CarbonReader() \
    .builder() \
    .withFolder(path) \
    .withBatch(1000) \
    .build()

  i = 0
  while reader.hasNext():
    rows = reader.readNextBatchRow()
    for row in rows:
      i += 1
      for column in row:
        from jnius.jnius import ByteArray
        if isinstance(column, ByteArray) and len(column) > 1000 and i < 20:
          with open(path + "/image" + str(i) + ".jpg", 'wb+') as file_object:
            file_object.write((column.tostring()))

  assert 3 == i
  reader.close()

  shutil.rmtree(path)
