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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.reader.CarbonIndexFileReader;
import org.apache.carbondata.format.BlockIndex;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * MinMaxTest
 */
public class MinMaxTest {
  String path = "./testMinMax";
  @Before
  public void mkDir() throws IOException {
    File file = new File(path);
    FileUtils.deleteDirectory(file);
    FileUtils.forceMkdir(file);
  }

  @After
  public void deleteDir() throws IOException {
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testMinMax() throws IOException {
    Field[] fields = new Field[6];
    fields[0] = new Field("byteField", DataTypes.BYTE);
    fields[1] = new Field("shortField", DataTypes.SHORT);
    fields[2] = new Field("infField", DataTypes.INT);
    fields[3] = new Field("longField", DataTypes.LONG);
    fields[4] = new Field("floatField", DataTypes.FLOAT);
    fields[5] = new Field("doubleField", DataTypes.DOUBLE);
    writeFilesAndVerify(new Schema(fields), path);
    readIndexAndVerify(path);
  }

  public static void writeFilesAndVerify(Schema schema, String path) {
    try {
      CarbonWriter writer =
          CarbonWriter
              .builder()
              .outputPath(path)
              .withCsvInput(schema)
              .writtenBy("TestUtil")
              .build();
      String[] row1 = new String[] {
          String.valueOf(Byte.MIN_VALUE),
          String.valueOf(Short.MIN_VALUE),
          String.valueOf(Integer.MIN_VALUE),
          String.valueOf(Long.MIN_VALUE),
          String.valueOf(Float.MIN_VALUE),
          String.valueOf(Double.MIN_VALUE)
      };
      for (int i = 0; i < 10; i++) {
        writer.write(row1);
      }
      String[] row2 = new String[] {
          String.valueOf(Byte.MAX_VALUE),
          String.valueOf(Short.MAX_VALUE),
          String.valueOf(Integer.MAX_VALUE),
          String.valueOf(Long.MAX_VALUE),
          String.valueOf(Float.MAX_VALUE),
          String.valueOf(Double.MAX_VALUE)
      };
      for (int i = 0; i < 26664; i++) {
        writer.write(row2);
      }
      String[] row3 = new String[] {
          String.valueOf(Byte.MAX_VALUE - 10),
          String.valueOf(Short.MAX_VALUE - 10),
          String.valueOf(Integer.MAX_VALUE - 10),
          String.valueOf(Long.MAX_VALUE - 10),
          String.valueOf(Float.MAX_VALUE - 10),
          String.valueOf(Double.MAX_VALUE - 10)
      };
      for (int i = 0; i < 6666; i++) {
        writer.write(row3);
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
    File[] dataFiles = new File(path).listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT);
      }
    });
    Assert.assertTrue(dataFiles != null);
    Assert.assertTrue(dataFiles.length > 0);
  }

  public static void readIndexAndVerify(String path) {
    File[] dataFiles = new File(path).listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith(CarbonCommonConstants.UPDATE_INDEX_FILE_EXT);
      }
    });
    Assert.assertTrue(dataFiles != null);
    Assert.assertTrue(dataFiles.length > 0);
    CarbonIndexFileReader reader = new CarbonIndexFileReader();
    try {
      reader.openThriftReader(dataFiles[0].getCanonicalPath());
      reader.readIndexHeader();
      BlockIndex index = reader.readBlockIndexInfo();
      List<ByteBuffer> maxValues = index.block_index.min_max_index.max_values;
      Assert.assertEquals(Byte.MAX_VALUE, maxValues.get(0).get());
      maxValues.get(1).position(6);
      Assert.assertEquals(Short.MAX_VALUE, maxValues.get(1).getShort());
      maxValues.get(2).position(4);
      Assert.assertEquals(Integer.MAX_VALUE, maxValues.get(2).getInt());
      Assert.assertEquals(Long.MAX_VALUE, maxValues.get(3).getLong());
      Assert.assertTrue(Float.MAX_VALUE - maxValues.get(4).getFloat() < 1);
      Assert.assertTrue(Double.MAX_VALUE - maxValues.get(5).getDouble() < 1);
      List<ByteBuffer> minValues = index.block_index.min_max_index.min_values;
      Assert.assertEquals(Byte.MIN_VALUE, minValues.get(0).get());
      minValues.get(1).position(6);
      Assert.assertEquals(Short.MIN_VALUE, minValues.get(1).getShort());
      minValues.get(2).position(4);
      Assert.assertEquals(Integer.MIN_VALUE, minValues.get(2).getInt());
      Assert.assertEquals(Long.MIN_VALUE, minValues.get(3).getLong());
      Assert.assertTrue(minValues.get(4).getFloat() - Float.MIN_VALUE < 1);
      Assert.assertTrue(minValues.get(5).getDouble() - Double.MIN_VALUE < 1);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } finally {
      reader.closeThriftReader();
    }
  }
}
