/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.sdk.file;

import junit.framework.TestCase;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.util.BinaryUtil;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Assert;
import org.junit.Test;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.stream.FileImageInputStream;
import javax.imageio.stream.ImageInputStream;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.carbondata.sdk.file.utils.SDKUtil.listFiles;

public class ImageTest extends TestCase {

  @Test
  public void testWriteWithByteArrayDataType() throws IOException, InvalidLoadOptionException, InterruptedException {
    String imagePath = "./src/test/resources/image/carbondatalogo.jpg";
    int num = 1;
    int rows = 10;
    String path = "./target/binary";
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (IOException e) {
      e.printStackTrace();
    }
    Field[] fields = new Field[7];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("image1", DataTypes.BINARY);
    fields[3] = new Field("image2", DataTypes.BINARY);
    fields[4] = new Field("image3", DataTypes.BINARY);
    fields[5] = new Field("decodeString", DataTypes.BINARY);
    fields[6] = new Field("decodeByte", DataTypes.BINARY);
    String[] projection = new String[]{"name", "age", "image1",
        "image2", "image3", "decodeString", "decodeByte"};
    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example")
          .withPageSizeInMb(1)
          .withLoadOption("binary_decoder", "base64")
          .build();

      for (int i = 0; i < rows; i++) {
        // read image and encode to Hex
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(imagePath));
        originBinary = new byte[bis.available()];
        while ((bis.read(originBinary)) != -1) {
        }
        // write data
        writer.write(new Object[]{"robot" + (i % 10), i, originBinary,
            originBinary, originBinary, "YWJj", "YWJj".getBytes()});
        bis.close();
      }
      writer.close();
    }

    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .projection(projection)
        .build();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      byte[] outputBinary = (byte[]) row[2];
      byte[] outputBinary2 = (byte[]) row[3];
      byte[] outputBinary3 = (byte[]) row[4];
      String stringValue = new String((byte[]) row[5]);
      String byteValue = new String((byte[]) row[6]);
      // when input is string, it will be decoded by base64.
      Assert.assertTrue("abc".equals(stringValue));
      // when input is byte[], it will be not decoded by base64.
      Assert.assertTrue("YWJj".equals(byteValue));
      System.out.println(row[0] + " " + row[1] + " image1 size:" + outputBinary.length
          + " image2 size:" + outputBinary2.length + " image3 size:" + outputBinary3.length
          + "\t" + stringValue + "\t" + byteValue);

      for (int k = 0; k < 3; k++) {

        byte[] originBinaryTemp = (byte[]) row[2 + k];
        // validate output binary data and origin binary data
        assert (originBinaryTemp.length == outputBinary.length);
        for (int j = 0; j < originBinaryTemp.length; j++) {
          assert (originBinaryTemp[j] == outputBinary[j]);
          assert (originBinary[j] == outputBinary[j]);
        }

        // save image, user can compare the save image and original image
        String destString = "./target/binary/image" + k + "_" + i + ".jpg";
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
        bos.write(originBinaryTemp);
        bos.close();
      }
      i++;
    }
    System.out.println("\nFinished");
    reader.close();
  }

  @Test
  public void testWriteBinaryWithSort() {
    int num = 1;
    String path = "./target/binary";
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (IOException e) {
      e.printStackTrace();
    }
    Field[] fields = new Field[5];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("image1", DataTypes.BINARY);
    fields[3] = new Field("image2", DataTypes.BINARY);
    fields[4] = new Field("image3", DataTypes.BINARY);

    // read and write image data
    for (int j = 0; j < num; j++) {
      try {
        CarbonWriter
            .builder()
            .outputPath(path)
            .withCsvInput(new Schema(fields))
            .writtenBy("SDKS3Example")
            .withPageSizeInMb(1)
            .withTableProperty("sort_columns", "image1")
            .build();
        assert (false);
      } catch (Exception e) {
        assert (e.getMessage().contains("sort columns not supported for array, struct, map, double, float, decimal, varchar, binary"));
      }
    }
  }

  @Test
  public void testWriteBinaryWithLong_string_columns() {
    int num = 1;
    String path = "./target/binary";
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (IOException e) {
      e.printStackTrace();
    }
    Field[] fields = new Field[5];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("image1", DataTypes.BINARY);
    fields[3] = new Field("image2", DataTypes.BINARY);
    fields[4] = new Field("image3", DataTypes.BINARY);

    // read and write image data
    for (int j = 0; j < num; j++) {
      try {
        CarbonWriter
            .builder()
            .outputPath(path)
            .withCsvInput(new Schema(fields))
            .writtenBy("SDKS3Example")
            .withPageSizeInMb(1)
            .withTableProperty("long_string_columns", "image1")
            .build();
        assert (false);
      } catch (Exception e) {
        assert (e.getMessage().contains("long string column : image1 is not supported for data type: BINARY"));
      }
    }
  }

  @Test
  public void testWriteBinaryWithInverted_index() {
    int num = 1;
    String path = "./target/binary";
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (IOException e) {
      e.printStackTrace();
    }
    Field[] fields = new Field[5];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("image1", DataTypes.BINARY);
    fields[3] = new Field("image2", DataTypes.BINARY);
    fields[4] = new Field("image3", DataTypes.BINARY);

    // read and write image data
    for (int j = 0; j < num; j++) {
      try {
        CarbonWriter
            .builder()
            .outputPath(path)
            .withCsvInput(new Schema(fields))
            .writtenBy("SDKS3Example")
            .withPageSizeInMb(1)
            .withTableProperty("inverted_index", "image1")
            .build();
        // TODO: should throw exception
        //        assert(false);
      } catch (Exception e) {
        System.out.println(e.getMessage());
        assert (e.getMessage().contains("INVERTED_INDEX column: image1 should be present in SORT_COLUMNS"));
      }
    }
  }

  @Test
  public void testWriteWithNull() throws IOException, InvalidLoadOptionException {
    String imagePath = "./src/test/resources/image/carbondatalogo.jpg";
    int num = 1;
    int rows = 10;
    String path = "./target/binary";
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (IOException e) {
      e.printStackTrace();
    }
    Field[] fields = new Field[5];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("image1", DataTypes.BINARY);
    fields[3] = new Field("image2", DataTypes.BINARY);
    fields[4] = new Field("image3", DataTypes.BINARY);

    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example")
          .withPageSizeInMb(1)
          .withLoadOption("bad_records_action", "force")
          .build();

      for (int i = 0; i < rows; i++) {
        // read image and encode to Hex
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(imagePath));
        originBinary = new byte[bis.available()];
        while ((bis.read(originBinary)) != -1) {
        }
        // write data
        writer.write(new Object[]{"robot" + (i % 10), i, originBinary, originBinary, 1});
        bis.close();
      }
      try {
        writer.close();
      } catch (Exception e) {
        assert (e.getMessage().contains("Binary only support String and byte[] data type"));
      }
    }

  }

  @Test
  public void testBinaryWithOrWithoutFilter() throws IOException, InvalidLoadOptionException, InterruptedException, DecoderException {
    String imagePath = "./src/test/resources/image/carbondatalogo.jpg";
    int num = 1;
    int rows = 1;
    String path = "./target/binary";
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (IOException e) {
      e.printStackTrace();
    }
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("image", DataTypes.BINARY);

    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example")
          .withPageSizeInMb(1)
          .build();

      for (int i = 0; i < rows; i++) {
        // read image and encode to Hex
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(imagePath));
        char[] hexValue = null;
        originBinary = new byte[bis.available()];
        while ((bis.read(originBinary)) != -1) {
          hexValue = Hex.encodeHex(originBinary);
        }
        // write data
        writer.write(new String[]{"robot" + (i % 10), String.valueOf(i), String.valueOf(hexValue)});
        bis.close();
      }
      writer.close();
    }

    // Read data with filter
    EqualToExpression equalToExpression = new EqualToExpression(
        new ColumnExpression("name", DataTypes.STRING),
        new LiteralExpression("robot0", DataTypes.STRING));

    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .filter(equalToExpression)
        .build();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      byte[] outputBinary = Hex.decodeHex(new String((byte[]) row[1]).toCharArray());
      System.out.println(row[0] + " " + row[2] + " image size:" + outputBinary.length);

      // validate output binary data and origin binary data
      assert (originBinary.length == outputBinary.length);
      for (int j = 0; j < originBinary.length; j++) {
        assert (originBinary[j] == outputBinary[j]);
      }
      String value = new String(outputBinary);
      Assert.assertTrue(value.startsWith("�PNG"));
      // save image, user can compare the save image and original image
      String destString = "./target/binary/image" + i + ".jpg";
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
      bos.write(outputBinary);
      bos.close();
      i++;
    }
    System.out.println("\nFinished");
    reader.close();

    CarbonReader reader2 = CarbonReader
        .builder(path, "_temp")
        .build();

    System.out.println("\nData:");
    i = 0;
    while (i < 20 && reader2.hasNext()) {
      Object[] row = (Object[]) reader2.readNextRow();

      byte[] outputBinary = Hex.decodeHex(new String((byte[]) row[1]).toCharArray());
      System.out.println(row[0] + " " + row[2] + " image size:" + outputBinary.length);

      // validate output binary data and origin binary data
      assert (originBinary.length == outputBinary.length);
      for (int j = 0; j < originBinary.length; j++) {
        assert (originBinary[j] == outputBinary[j]);
      }

      // save image, user can compare the save image and original image
      String destString = "./target/binary/image" + i + ".jpg";
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
      bos.write(outputBinary);
      bos.close();
      i++;
    }
    reader2.close();
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println("\nFinished");
  }

  @Test
  public void testBinaryWithManyImages() throws IOException, InvalidLoadOptionException, InterruptedException {
    int num = 1;
    String path = "./target/flowers";
    Field[] fields = new Field[5];
    fields[0] = new Field("binaryId", DataTypes.INT);
    fields[1] = new Field("binaryName", DataTypes.STRING);
    fields[2] = new Field("binary", "Binary");
    fields[3] = new Field("labelName", DataTypes.STRING);
    fields[4] = new Field("labelContent", DataTypes.STRING);

    String imageFolder = "./src/test/resources/image/flowers";

    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example")
          .withPageSizeInMb(1)
          .build();
      File file = new File(imageFolder);
      File[] files = file.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if (name == null) {
            return false;
          }
          return name.endsWith(".jpg");
        }
      });

      if (null != files) {
        for (int i = 0; i < files.length; i++) {
          // read image and encode to Hex
          BufferedInputStream bis = new BufferedInputStream(new FileInputStream(files[i]));
          char[] hexValue = null;
          originBinary = new byte[bis.available()];
          while ((bis.read(originBinary)) != -1) {
            hexValue = Hex.encodeHex(originBinary);
          }

          String txtFileName = files[i].getCanonicalPath().split(".jpg")[0] + ".txt";
          BufferedInputStream txtBis = new BufferedInputStream(new FileInputStream(txtFileName));
          String txtValue = null;
          byte[] txtBinary = null;
          txtBinary = new byte[txtBis.available()];
          while ((txtBis.read(txtBinary)) != -1) {
            txtValue = new String(txtBinary, "UTF-8");
          }
          // write data
          System.out.println(files[i].getCanonicalPath());
          writer.write(new String[]{String.valueOf(i), files[i].getCanonicalPath(), String.valueOf(hexValue),
              txtFileName, txtValue});
          bis.close();
        }
      }
      writer.close();
    }

    CarbonReader reader = CarbonReader
        .builder(path)
        .build();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      byte[] outputBinary = (byte[]) row[1];
      System.out.println(row[0] + " " + row[2] + " image size:" + outputBinary.length);

      // save image, user can compare the save image and original image
      String destString = "./target/flowers/image" + i + ".jpg";
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
      bos.write(outputBinary);
      bos.close();
      i++;
    }
    System.out.println("\nFinished");
    reader.close();
  }

  public void testWriteTwoImageColumn() throws Exception {
    String imagePath = "./src/test/resources/image/vocForSegmentationClass";
    String path = "./target/vocForSegmentationClass";
    int num = 1;
    Field[] fields = new Field[4];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("rawImage", DataTypes.BINARY);
    fields[3] = new Field("segmentationClass", DataTypes.BINARY);

    byte[] originBinary = null;
    byte[] originBinary2 = null;

    Object[] files = listFiles(imagePath, ".jpg").toArray();
    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example")
          .withPageSizeInMb(1)
          .build();

      for (int i = 0; i < files.length; i++) {
        // read image and encode to Hex
        String filePath = (String) files[i];
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(filePath));
        originBinary = new byte[bis.available()];
        while ((bis.read(originBinary)) != -1) {
        }

        BufferedInputStream bis2 = new BufferedInputStream(new FileInputStream(filePath.replace(".jpg", ".png")));
        originBinary2 = new byte[bis2.available()];
        while ((bis2.read(originBinary2)) != -1) {
        }

        // write data
        writer.write(new Object[]{"robot" + (i % 10), i, originBinary, originBinary2});
        bis.close();
        bis2.close();
      }
      writer.close();
    }

    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .build();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      byte[] outputBinary = (byte[]) row[1];
      byte[] outputBinary2 = (byte[]) row[2];
      System.out.println(row[0] + " " + row[3] + " image1 size:" + outputBinary.length
          + " image2 size:" + outputBinary2.length);

      for (int k = 0; k < 2; k++) {

        byte[] originBinaryTemp = (byte[]) row[1 + k];

        // save image, user can compare the save image and original image
        String destString = null;
        if (k == 0) {
          destString = "./target/vocForSegmentationClass/image" + k + "_" + i + ".jpg";
        } else {
          destString = "./target/vocForSegmentationClass/image" + k + "_" + i + ".png";
        }
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
        bos.write(originBinaryTemp);
        bos.close();
      }
      i++;
    }
    System.out.println("\nFinished");
    reader.close();
  }

  @Test
  public void testWriteWithByteArrayDataTypeAndManyImagesTxt()
      throws Exception {
    long startWrite = System.nanoTime();
    String sourceImageFolder = "./src/test/resources/image/flowers";
    String outputPath = "./target/flowers";
    String preDestPath = "./target/flowers/image";
    String sufAnnotation = ".txt";
    BinaryUtil.binaryToCarbon(sourceImageFolder, outputPath, sufAnnotation, ".jpg");
    BinaryUtil.carbonToBinary(outputPath, preDestPath);
    long endWrite = System.nanoTime();
    System.out.println("write time is " + (endWrite - startWrite) / 1000000000.0 + "s");
  }

  @Test
  public void testWriteWithByteArrayDataTypeAndManyImagesXml()
      throws Exception {
    long startWrite = System.nanoTime();
    String sourceImageFolder = "./src/test/resources/image/voc";

    String outputPath = "./target/voc";
    String preDestPath = "./target/voc/image";
    String sufAnnotation = ".xml";
    BinaryUtil.binaryToCarbon(sourceImageFolder, outputPath, sufAnnotation, ".jpg");
    BinaryUtil.carbonToBinary(outputPath, preDestPath);
    long endWrite = System.nanoTime();
    System.out.println("write time is " + (endWrite - startWrite) / 1000000000.0 + "s");
    ReadPerformance();
  }

  public void ReadPerformance() throws Exception {
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB, "2048");

    long start = System.nanoTime();
    int i = 0;
    String path = "./target/voc";
    CarbonReader reader2 = CarbonReader
        .builder(path)
        .withBatch(1000)
        .build();

    System.out.println("\nData2:");
    i = 0;
    while (reader2.hasNext()) {
      Object[] rows = reader2.readNextBatchRow();

      for (int j = 0; j < rows.length; j++) {
        Object[] row = (Object[]) rows[j];
        i++;
        if (0 == i % 1000) {
          System.out.println(i);
        }
        for (int k = 0; k < row.length; k++) {
          Object column = row[k];
        }
      }
    }

    System.out.println(i);
    reader2.close();
    long end = System.nanoTime();
    System.out.println("all time is " + (end - start) / 1000000000.0);
    System.out.println("\nFinished");
  }

  @Test
  public void testWriteWithByteArrayDataTypeAndManyImagesTxt3()
      throws Exception {
    String sourceImageFolder = "./src/test/resources/image/flowers";
    String outputPath = "./target/flowers2";
    String preDestPath = "./target/flowers2/image";
    String sufAnnotation = ".txt";
    try {
      FileUtils.deleteDirectory(new File(outputPath));
    } catch (IOException e) {
      e.printStackTrace();
    }
    binaryToCarbonWithHWD(sourceImageFolder, outputPath, preDestPath, sufAnnotation, ".jpg", 2000);
    try {
      FileUtils.deleteDirectory(new File(outputPath));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testNumberOfFiles() throws Exception {
    String sourceImageFolder = "./src/test/resources/image/flowers";
    List result = listFiles(sourceImageFolder, ".jpg");
    assertEquals(3, result.size());
  }

  @Test
  public void testWriteNonBase64WithBase64Decoder() throws IOException, InvalidLoadOptionException, InterruptedException {
    String imagePath = "./src/test/resources/image/carbondatalogo.jpg";
    int num = 1;
    int rows = 10;
    String path = "./target/binary";
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (IOException e) {
      e.printStackTrace();
    }
    Field[] fields = new Field[7];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("image1", DataTypes.BINARY);
    fields[3] = new Field("image2", DataTypes.BINARY);
    fields[4] = new Field("image3", DataTypes.BINARY);
    fields[5] = new Field("decodeString", DataTypes.BINARY);
    fields[6] = new Field("decodeByte", DataTypes.BINARY);
    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example")
          .withPageSizeInMb(1)
          .withLoadOption("binary_decoder", "base64")
          .build();

      for (int i = 0; i < rows; i++) {
        // read image and encode to Hex
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(imagePath));
        originBinary = new byte[bis.available()];
        while ((bis.read(originBinary)) != -1) {
        }
        // write data
        writer.write(new Object[]{"robot" + (i % 10), i, originBinary,
            originBinary, originBinary, "^YWJj", "^YWJj".getBytes()});
        bis.close();
      }
      try {
        writer.close();
        Assert.assertTrue(false);
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains("Binary decoder is base64, but data is not base64"));
      }
    }
  }

  public void testInvalidValueForBinaryDecoder() throws IOException, InvalidLoadOptionException {
    String path = "./target/binary";
    Field[] fields = new Field[7];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("image1", DataTypes.BINARY);
    fields[3] = new Field("image2", DataTypes.BINARY);
    fields[4] = new Field("image3", DataTypes.BINARY);
    fields[5] = new Field("decodeString", DataTypes.BINARY);
    fields[6] = new Field("decodeByte", DataTypes.BINARY);
    try {
      CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example")
          .withPageSizeInMb(1)
          .withLoadOption("binary_decoder", "base")
          .build();
      Assert.assertTrue(false);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Binary decoder only support Base64, Hex or no decode for string, don't support base"));
    }
  }

  public void binaryToCarbonWithHWD(String sourceImageFolder, String outputPath, String preDestPath,
                                    String sufAnnotation, final String sufImage, int numToWrite)
      throws Exception {
    int num = 1;
    Field[] fields = new Field[7];
    fields[0] = new Field("height", DataTypes.INT);
    fields[1] = new Field("width", DataTypes.INT);
    fields[2] = new Field("depth", DataTypes.INT);
    fields[3] = new Field("binaryName", DataTypes.STRING);
    fields[4] = new Field("binary", DataTypes.BINARY);
    fields[5] = new Field("labelName", DataTypes.STRING);
    fields[6] = new Field("labelContent", DataTypes.STRING);

    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {

      Object[] files = listFiles(sourceImageFolder, sufImage).toArray();

      int index = 0;

      if (null != files) {
        CarbonWriter writer = CarbonWriter
            .builder()
            .outputPath(outputPath)
            .withCsvInput(new Schema(fields))
            .withBlockSize(256)
            .writtenBy("SDKS3Example")
            .withPageSizeInMb(1)
            .build();

        for (int i = 0; i < files.length; i++) {
          if (0 == index % numToWrite) {
            writer.close();
            writer = CarbonWriter
                .builder()
                .outputPath(outputPath)
                .withCsvInput(new Schema(fields))
                .withBlockSize(256)
                .writtenBy("SDKS3Example")
                .withPageSizeInMb(1)
                .build();
          }
          index++;

          // read image and encode to Hex
          File file = new File((String) files[i]);
          System.out.println(file.getCanonicalPath());
          BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
          int depth = 0;
          boolean isGray;
          boolean hasAlpha;
          BufferedImage bufferedImage = null;
          try {
            bufferedImage = ImageIO.read(file);
            isGray = bufferedImage.getColorModel().getColorSpace().getType() == ColorSpace.TYPE_GRAY;
            hasAlpha = bufferedImage.getColorModel().hasAlpha();

            if (isGray) {
              depth = 1;
            } else if (hasAlpha) {
              depth = 4;
            } else {
              depth = 3;
            }

          } catch (Exception e) {
            e.printStackTrace();
            System.out.println(i);
            ImageInputStream stream = new FileImageInputStream(new File(file.getCanonicalPath()));
            Iterator<ImageReader> iter = ImageIO.getImageReaders(stream);

            Exception lastException = null;
            while (iter.hasNext()) {
              ImageReader reader = null;
              try {
                reader = (ImageReader) iter.next();
                ImageReadParam param = reader.getDefaultReadParam();
                reader.setInput(stream, true, true);
                Iterator<ImageTypeSpecifier> imageTypes = reader.getImageTypes(0);

                while (imageTypes.hasNext()) {
                  ImageTypeSpecifier imageTypeSpecifier = imageTypes.next();
                  System.out.println(imageTypeSpecifier.getColorModel().getColorSpace().getType());
                  int bufferedImageType = imageTypeSpecifier.getBufferedImageType();
                  if (bufferedImageType == BufferedImage.TYPE_BYTE_GRAY) {
                    param.setDestinationType(imageTypeSpecifier);
                    break;
                  }
                }
                bufferedImage = reader.read(0, param);
                isGray = bufferedImage.getColorModel().getColorSpace().getType() == ColorSpace.TYPE_GRAY;
                hasAlpha = bufferedImage.getColorModel().hasAlpha();

                if (isGray) {
                  depth = 1;
                } else if (hasAlpha) {
                  depth = 4;
                } else {
                  depth = 3;
                }
                if (null != bufferedImage) break;
              } catch (Exception e2) {
                lastException = e2;
              } finally {
                if (null != reader) reader.dispose();
              }
            }
            // If you don't have an image at the end of all readers
            if (null == bufferedImage) {
              if (null != lastException) {
                throw lastException;
              }
            }
          } finally {
            originBinary = new byte[bis.available()];
            while ((bis.read(originBinary)) != -1) {
            }

            String txtFileName = file.getCanonicalPath().split(sufImage)[0] + sufAnnotation;
            BufferedInputStream txtBis = new BufferedInputStream(new FileInputStream(txtFileName));
            String txtValue = null;
            byte[] txtBinary = null;
            txtBinary = new byte[txtBis.available()];
            while ((txtBis.read(txtBinary)) != -1) {
              txtValue = new String(txtBinary, "UTF-8");
            }
            // write data
            writer.write(new Object[]{bufferedImage.getHeight(), bufferedImage.getWidth(), depth, file.getCanonicalPath(), originBinary,
                txtFileName, txtValue.replace("\n", "")});
            bis.close();
          }
        }
        writer.close();
      }
    }

    CarbonReader reader = CarbonReader
        .builder(outputPath)
        .build();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      byte[] outputBinary = (byte[]) row[1];
      System.out.println(row[2] + " " + row[3] + " " + row[4] + " " + row[5] + " image size:" + outputBinary.length + " " + row[0]);

      // save image, user can compare the save image and original image
      String destString = preDestPath + i + sufImage;
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
      bos.write(outputBinary);
      bos.close();
      i++;
    }
    System.out.println("\nFinished");
    reader.close();
  }

  @Test
  public void testBinaryWithProjectionAndFileListsAndWithFile() throws Exception {
    int num = 5;
    String path = "./target/flowersFolder";
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (IOException e) {
      e.printStackTrace();
    }
    Field[] fields = new Field[5];
    fields[0] = new Field("imageId", DataTypes.INT);
    fields[1] = new Field("imageName", DataTypes.STRING);
    fields[2] = new Field("imageBinary", DataTypes.BINARY);
    fields[3] = new Field("txtName", DataTypes.STRING);
    fields[4] = new Field("txtContent", DataTypes.STRING);

    String imageFolder = "./src/test/resources/image/flowers";

    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example")
          .withPageSizeInMb(1)
          .build();
      ArrayList files = listFiles(imageFolder, ".jpg");

      if (null != files) {
        for (int i = 0; i < files.size(); i++) {
          // read image and encode to Hex
          BufferedInputStream bis = new BufferedInputStream(new FileInputStream(files.get(i).toString()));
          char[] hexValue = null;
          originBinary = new byte[bis.available()];
          while ((bis.read(originBinary)) != -1) {
            hexValue = Hex.encodeHex(originBinary);
          }

          String txtFileName = files.get(i).toString().split(".jpg")[0] + ".txt";
          BufferedInputStream txtBis = new BufferedInputStream(new FileInputStream(txtFileName));
          String txtValue = null;
          byte[] txtBinary = null;
          txtBinary = new byte[txtBis.available()];
          while ((txtBis.read(txtBinary)) != -1) {
            txtValue = new String(txtBinary, "UTF-8");
          }
          // write data
          System.out.println(files.get(i).toString());
          writer.write(new String[]{String.valueOf(i), files.get(i).toString(), String.valueOf(hexValue),
              txtFileName, txtValue});
          bis.close();
        }
      }
      writer.close();
    }

    // 1. read with file list
    List fileLists = listFiles(path, CarbonTablePath.CARBON_DATA_EXT);
    int fileNum = fileLists.size() / 2;

    Schema schema = CarbonSchemaReader.readSchema((String) fileLists.get(0)).asOriginOrder();
    List projectionLists = new ArrayList();
    projectionLists.add((schema.getFields())[1].getFieldName());
    projectionLists.add((schema.getFields())[2].getFieldName());

    CarbonReader reader = ArrowCarbonReader
        .builder()
        .withFileLists(fileLists.subList(0, fileNum))
        .projection(projectionLists)
        .buildArrowReader();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      assertEquals(2, row.length);
      byte[] outputBinary = (byte[]) row[1];
      System.out.println(row[0] + " " + row[1] + " image size:" + outputBinary.length);
      i++;
    }
    assert (i == fileNum * 3);
    System.out.println("\nFinished: " + i);
    reader.close();

    // 2. read withFile
    CarbonReader reader2 = CarbonReader
        .builder()
        .withFile(fileLists.get(0).toString())
        .build();

    System.out.println("\nData2:");
    i = 0;
    while (i < 20 && reader2.hasNext()) {
      Object[] row = (Object[]) reader2.readNextRow();
      assertEquals(5, row.length);

      assert (null != row[0].toString());
      assert (null != row[0].toString());
      assert (null != row[0].toString());
      byte[] outputBinary = (byte[]) row[1];

      String txt = row[2].toString();
      System.out.println(row[0] + " " + row[2] +
          " image size:" + outputBinary.length + " txt size:" + txt.length());
      i++;
    }
    System.out.println("\nFinished: " + i);
    reader2.close();

    // 3. read with folder
    CarbonReader reader3 = CarbonReader
        .builder(path)
        .withFolder(path)
        .build();

    System.out.println("\nData:");
    i = 0;
    while (i < 20 && reader3.hasNext()) {
      Object[] row = (Object[]) reader3.readNextRow();

      byte[] outputBinary = (byte[]) row[1];
      System.out.println(row[0] + " " + row[2] + " image size:" + outputBinary.length);
      i++;
    }
    System.out.println("\nFinished: " + i);
    reader3.close();

    InputSplit[] splits = ArrowCarbonReader
        .builder()
        .withFileLists(fileLists.subList(0, fileNum))
        .getSplits(true);
    Assert.assertTrue(splits.length == fileNum);
    for (int j = 0; j < splits.length; j++) {
      ArrowCarbonReader.builder(splits[j]).build();
    }
  }

  public void testGetSplitWithFileListsFromDifferentFolder() throws Exception {

    String path1 = "./target/flowersFolder1";
    String path2 = "./target/flowersFolder2";
    writeCarbonFile(path1, 3);
    writeCarbonFile(path2, 2);
    List fileLists = listFiles(path1, CarbonTablePath.CARBON_DATA_EXT);
    fileLists.addAll(listFiles(path2, CarbonTablePath.CARBON_DATA_EXT));

    InputSplit[] splits = ArrowCarbonReader
        .builder()
        .withFileLists(fileLists)
        .getSplits(true);
    Assert.assertTrue(5 == splits.length);
    for (int j = 0; j < splits.length; j++) {
      ArrowCarbonReader.builder(splits[j]).build();
    }
  }

  public void writeCarbonFile(String path, int num) throws Exception {
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (IOException e) {
      e.printStackTrace();
    }
    Field[] fields = new Field[5];
    fields[0] = new Field("imageId", DataTypes.INT);
    fields[1] = new Field("imageName", DataTypes.STRING);
    fields[2] = new Field("imageBinary", DataTypes.BINARY);
    fields[3] = new Field("txtName", DataTypes.STRING);
    fields[4] = new Field("txtContent", DataTypes.STRING);

    String imageFolder = "./src/test/resources/image/flowers";

    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example")
          .withPageSizeInMb(1)
          .build();
      ArrayList files = listFiles(imageFolder, ".jpg");

      if (null != files) {
        for (int i = 0; i < files.size(); i++) {
          // read image and encode to Hex
          BufferedInputStream bis = new BufferedInputStream(new FileInputStream(files.get(i).toString()));
          char[] hexValue = null;
          originBinary = new byte[bis.available()];
          while ((bis.read(originBinary)) != -1) {
            hexValue = Hex.encodeHex(originBinary);
          }

          String txtFileName = files.get(i).toString().split(".jpg")[0] + ".txt";
          BufferedInputStream txtBis = new BufferedInputStream(new FileInputStream(txtFileName));
          String txtValue = null;
          byte[] txtBinary = null;
          txtBinary = new byte[txtBis.available()];
          while ((txtBis.read(txtBinary)) != -1) {
            txtValue = new String(txtBinary, "UTF-8");
          }
          // write data
          System.out.println(files.get(i).toString());
          writer.write(new String[]{String.valueOf(i), files.get(i).toString(), String.valueOf(hexValue),
              txtFileName, txtValue});
          bis.close();
        }
      }
      writer.close();
    }
  }

  @Test public void testBinaryWithComplexType()
      throws IOException, InvalidLoadOptionException, InterruptedException {
    int num = 1;
    int rows = 1;
    String path = "./target/binary";
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (IOException e) {
      e.printStackTrace();
    }
    Field[] fields = new Field[4];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("arrayField", DataTypes.createArrayType(DataTypes.BINARY));
    ArrayList<StructField> structFields = new ArrayList<>();
    structFields.add(new StructField("b", DataTypes.BINARY));
    fields[3] = new Field("structField", DataTypes.createStructType(structFields));

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter.builder().outputPath(path).withCsvInput(new Schema(fields))
          .writtenBy("BinaryExample").withPageSizeInMb(1).build();

      for (int i = 0; i < rows; i++) {
        // write data
        writer.write(new String[] { "robot" + (i % 10), String.valueOf(i), "binary1", "binary2" });
      }
      writer.close();
    }
    CarbonReader reader = CarbonReader.builder(path, "_temp").build();
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      Object[] arrayResult = (Object[]) row[1];
      Object[] structResult = (Object[]) row[2];
      assert (new String((byte[]) arrayResult[0]).equalsIgnoreCase("binary1"));
      assert (new String((byte[]) structResult[0]).equalsIgnoreCase("binary2"));
    }
    reader.close();
  }

  @Test public void testHugeBinaryWithComplexType()
      throws IOException, InvalidLoadOptionException, InterruptedException {
    int num = 1;
    int rows = 1;
    String path = "./target/binary";
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (IOException e) {
      e.printStackTrace();
    }
    Field[] fields = new Field[2];
    fields[0] = new Field("arrayField", DataTypes.createArrayType(DataTypes.BINARY));
    ArrayList<StructField> structFields = new ArrayList<>();
    structFields.add(new StructField("b", DataTypes.BINARY));
    fields[1] = new Field("structField", DataTypes.createStructType(structFields));

    String description = RandomStringUtils.randomAlphabetic(33000);

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter.builder().outputPath(path).withCsvInput(new Schema(fields))
          .writtenBy("BinaryExample").withPageSizeInMb(5).build();

      for (int i = 0; i < rows; i++) {
        // write data
        writer.write(new String[] { description, description });
      }
      writer.close();
    }
    CarbonReader reader = CarbonReader.builder(path, "_temp").build();
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      Object[] arrayResult = (Object[]) row[0];
      Object[] structResult = (Object[]) row[1];
      assert (new String((byte[]) arrayResult[0]).equalsIgnoreCase(description));
      assert (new String((byte[]) structResult[0]).equalsIgnoreCase(description));
    }
    reader.close();
  }

}
