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

from pycarbon.pysdk.CarbonReader import CarbonReader
import time

from pycarbon.pysdk.CarbonWriter import CarbonWriter
from pycarbon.tests import DEFAULT_CARBONSDK_PATH

import unittest
import base64


class WriteCarbonTest(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    import jnius_config

    jnius_config.set_classpath(DEFAULT_CARBONSDK_PATH)

    print("TestCase  start running ")

  def test_1_run_write_carbon(self):

    start = time.time()
    jsonSchema = "[{stringField:string},{shortField:short},{intField:int}]";
    path = "/tmp/data/writeCarbon" + str(time.time())
    print(path)
    writer = CarbonWriter() \
      .builder() \
      .outputPath(path) \
      .withCsvInput(jsonSchema) \
      .writtenBy("pycarbon") \
      .build()

    for i in range(0, 10):
      from jnius import autoclass
      arrayListClass = autoclass("java.util.ArrayList");
      list = arrayListClass()
      list.add("pycarbon")
      list.add(str(i))
      list.add(str(i * 10))
      writer.write(list.toArray())
    writer.close()

    reader = CarbonReader() \
      .builder() \
      .withFolder(path) \
      .withBatch(1000) \
      .build()

    i = 0
    while (reader.hasNext()):
      rows = reader.readNextBatchRow()
      for row in rows:
        i = i + 1
        print()
        if 1 == i % 10:
          print(str(i) + " to " + str(i + 9) + ":")
        for column in row:
          print(column, end="\t")

    print()
    print("number of rows by read: " + str(i))
    assert 10 == i
    reader.close()

    end = time.time()
    print("all time is " + str(end - start))
    print("Finish")

  def test_2_run_write_carbon_binary_base64_encode(self):

    start = time.time()
    jsonSchema = "[{stringField:string},{shortField:short},{intField:int},{binaryField:binary}]";
    path = "/tmp/data/writeCarbon" + str(time.time())
    jpg_path = "../../data/image/carbondatalogo.jpg"
    print(path)
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

      arrayListClass = autoclass("java.util.ArrayList");
      list = arrayListClass()
      list.add("pycarbon")
      list.add(str(i))
      list.add(str(i * 10))
      list.add(base64.b64encode(content))

      writer.write(list.toArray())
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
        i = i + 1
        print()
        if 1 == i % 10:
          print(str(i) + " to " + str(i + 9) + ":")
        for column in row:
          print(column, end="\t")
          from jnius.jnius import ByteArray
          if 1 == i and isinstance(column, ByteArray) and len(column) > 1000:
            with open(path + "/image.jpg", 'wb+') as file_object:
              file_object.write(base64.b64decode(column.tostring()))

    print()
    print("number of rows by read: " + str(i))
    assert 10 == i
    reader.close()

    end = time.time()
    print("all time is " + str(end - start))
    print("Finish")

  # TODO: to be supported
  @unittest.skip
  def skiptest_3_run_write_carbon_binary(self):

    start = time.time()
    jsonSchema = "[{stringField:string},{shortField:short},{intField:int},{binaryField:binary}]";
    path = "/tmp/data/writeCarbon" + str(time.time())
    out_path = "/tmp/data/writeCarbon2_" + str(time.time()) + "/image.jpg"
    jpg_path = "../../data/image/carbondatalogo.jpg"
    print(path)
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

      arrayListClass = autoclass("java.util.ArrayList");
      list = arrayListClass()
      list.add("pycarbon")
      list.add(str(i))
      list.add(str(i * 10))
      list.add(content)

      writer.write(list)
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
        i = i + 1
        print()
        if 1 == i % 10:
          print(str(i) + " to " + str(i + 9) + ":")
        for column in row:
          print(column, end="\t")
          from jnius.jnius import ByteArray
          if 1 == i and isinstance(column, ByteArray) and len(column) > 1000:
            with open(path + "/image.jpg", 'wb+') as file_object:
              file_object.write(column.tostring())

    print()
    print("number of rows by read: " + str(i))
    assert 10 == i
    reader.close()

    end = time.time()
    print("all time is " + str(end - start))
    print("Finish")

  def test_4_run_write_image(self):
    path = "../../data/image/carbondatalogo.jpg"
    a = 2 + 1
    print(a)
    from skimage import io
    img = io.imread(path)
    io.imshow(img)

    with open(path, mode='rb+') as file_object:
      content2 = file_object.read()
      bytesdata = bytes(content2)
      print(len(content2))
      print(len(bytesdata))

    with open(path, encoding="ISO-8859-1") as file_object:
      content2 = file_object.read()
      bytesdata = bytes(content2, encoding="ISO-8859-1")
      print(len(content2))
      print(len(bytesdata))

  def test_5_run_write_carbon_binary_base64_encode_many_files(self):

    start = time.time()
    jsonSchema = "[{stringField:string},{shortField:short},{intField:int},{binaryField:binary},{txtField:string}]";
    path = "/tmp/data/writeCarbon" + str(time.time())
    jpg_path = "../../data/image/flowers"

    from jnius import autoclass

    sdkUtilClass = autoclass("org.apache.carbondata.sdk.file.utils.SDKUtil");
    jpg_files = sdkUtilClass.listFiles(jpg_path, '.jpg')
    print(path)

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

      from jnius import autoclass

      arrayListClass = autoclass("java.util.ArrayList");
      list = arrayListClass()
      list.add("pycarbon")
      list.add(str(i))
      list.add(str(i * 10))
      list.add(base64.b64encode(content))
      list.add(txt)

      writer.write(list.toArray())

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
        i = i + 1
        print()
        if 1 == i % 10:
          print(str(i) + " to " + str(i + 9) + ":")
        for column in row:
          print(column, end="\t")
          from jnius.jnius import ByteArray
          if isinstance(column, ByteArray) and len(column) > 1000:
            with open(path + "/image" + str(i) + ".jpg", 'wb+') as file_object:
              file_object.write(base64.b64decode(column.tostring()))

    print()
    print("number of rows by read: " + str(i))
    assert 3 == i
    reader.close()

    end = time.time()
    print("all time is " + str(end - start))
    print("Finish")

  def test_6_run_write_carbon_binary_base64_encode_voc(self):

    start = time.time()
    jsonSchema = "[{stringField:string},{shortField:short},{intField:int},{binaryField:binary},{txtField:string}]";
    path = "/tmp/data/writeCarbon" + str(time.time())
    jpg_path = "../../data/image/voc"

    from jnius import autoclass

    sdkUtilClass = autoclass("org.apache.carbondata.sdk.file.utils.SDKUtil");
    jpg_files = sdkUtilClass.listFiles(jpg_path, '.jpg')
    print(path)

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

      from jnius import autoclass

      arrayListClass = autoclass("java.util.ArrayList");
      list = arrayListClass()
      list.add("pycarbon")
      list.add(str(i))
      list.add(str(i * 10))
      list.add(base64.b64encode(content))
      list.add(txt)

      writer.write(list.toArray())

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
        i = i + 1
        print()
        if 1 == i % 10:
          print(str(i) + " to " + str(i + 9) + ":")
        for column in row:
          print(column, end="\t")
          from jnius.jnius import ByteArray
          if isinstance(column, ByteArray) and len(column) > 1000:
            with open(path + "/image" + str(i) + ".jpg", 'wb+') as file_object:
              file_object.write(base64.b64decode(column.tostring()))

    print()
    print("number of rows by read: " + str(i))
    assert 5 == i
    reader.close()

    end = time.time()
    print("all time is " + str(end - start))
    print("Finish")

  def test_7_run_write_carbon_binary_base64_encode_vocForSegmentationClass(self):

    start = time.time()
    jsonSchema = "[{stringField:string},{shortField:short},{intField:int},{binaryField:binary},{segField:binary}]";
    path = "/tmp/data/writeCarbon" + str(time.time())
    jpg_path = "../../data/image/vocForSegmentationClass"

    from jnius import autoclass

    sdkUtilClass = autoclass("org.apache.carbondata.sdk.file.utils.SDKUtil");
    jpg_files = sdkUtilClass.listFiles(jpg_path, '.jpg')
    print(path)

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

      from jnius import autoclass

      arrayListClass = autoclass("java.util.ArrayList");
      list = arrayListClass()
      list.add("pycarbon")
      list.add(str(i))
      list.add(str(i * 10))
      list.add(base64.b64encode(content))
      list.add(base64.b64encode(png_data))

      writer.write(list.toArray())

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
        i = i + 1
        print()
        if 1 == i % 10:
          print(str(i) + " to " + str(i + 9) + ":")
        num = 0
        for column in row:
          num = num + 1
          print(column, end="\t")
          from jnius.jnius import ByteArray
          if isinstance(column, ByteArray) and len(column) > 1000:
            with open(path + "/image" + str(i) + "_" + str(num) + ".jpg", 'wb+') as file_object:
              file_object.write(base64.b64decode(column.tostring()))

    print()
    print("number of rows by read: " + str(i))
    assert 3 == i
    reader.close()

    end = time.time()
    print("all time is " + str(end - start))
    print("Finish")

  def test_8_run_write_carbon_binary_base64_encode_decodeInJava_many_files(self):

    start = time.time()
    jsonSchema = "[{stringField:string},{shortField:short},{intField:int},{binaryField:binary},{txtField:string}]";
    path = "/tmp/data/writeCarbon" + str(time.time())
    jpg_path = "../../data/image/flowers"

    from jnius import autoclass

    sdkUtilClass = autoclass("org.apache.carbondata.sdk.file.utils.SDKUtil");
    jpg_files = sdkUtilClass.listFiles(jpg_path, '.jpg')
    print(path)

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

      from jnius import autoclass

      arrayListClass = autoclass("java.util.ArrayList");
      list = arrayListClass()
      list.add("pycarbon")
      list.add(str(i))
      list.add(str(i * 10))
      list.add(base64.b64encode(content))
      list.add(txt)

      writer.write(list.toArray())

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
        i = i + 1
        print()
        if 1 == i % 10:
          print(str(i) + " to " + str(i + 9) + ":")
        for column in row:
          print(column, end="\t")
          from jnius.jnius import ByteArray
          if isinstance(column, ByteArray) and len(column) > 1000 and i < 20:
            with open(path + "/image" + str(i) + ".jpg", 'wb+') as file_object:
              file_object.write((column.tostring()))

    print()
    print("number of rows by read: " + str(i))
    assert 3 == i
    reader.close()

    end = time.time()
    print("all time is " + str(end - start))
    print("Finish")
