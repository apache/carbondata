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
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.commons.io.FileUtils;

import junit.framework.TestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CarbonSchemaReaderTest extends TestCase {
  String path = "./testWriteFiles";

  @Before
  public void setUp() throws IOException, InvalidLoadOptionException {
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[12];
    fields[0] = new Field("stringField", DataTypes.STRING);
    fields[1] = new Field("shortField", DataTypes.SHORT);
    fields[2] = new Field("intField", DataTypes.INT);
    fields[3] = new Field("longField", DataTypes.LONG);
    fields[4] = new Field("doubleField", DataTypes.DOUBLE);
    fields[5] = new Field("boolField", DataTypes.BOOLEAN);
    fields[6] = new Field("dateField", DataTypes.DATE);
    fields[7] = new Field("timeField", DataTypes.TIMESTAMP);
    fields[8] = new Field("decimalField", DataTypes.createDecimalType(8, 2));
    fields[9] = new Field("varcharField", DataTypes.VARCHAR);
    fields[10] = new Field("arrayField", DataTypes.createArrayType(DataTypes.STRING));
    fields[11] = new Field("floatField", DataTypes.FLOAT);
    Map<String, String> map = new HashMap<>();
    map.put("complex_delimiter_level_1", "#");
    CarbonWriter writer = CarbonWriter.builder()
        .outputPath(path)
        .withLoadOptions(map)
        .withCsvInput(new Schema(fields))
        .writtenBy("CarbonSchemaReaderTest")
        .build();

    for (int i = 0; i < 10; i++) {
      String[] row2 = new String[]{
          "robot" + (i % 10),
          String.valueOf(i % 10000),
          String.valueOf(i),
          String.valueOf(Long.MAX_VALUE - i),
          String.valueOf((double) i / 2),
          String.valueOf(true),
          "2019-03-02",
          "2019-02-12 03:03:34",
          "12.345",
          "varchar",
          "Hello#World#From#Carbon",
          "1.23"
      };
      writer.write(row2);
    }
    writer.close();
  }

  @Test
  public void testReadSchemaFromDataFile() {
    try {
      CarbonFile[] carbonFiles = FileFactory
          .getCarbonFile(path)
          .listFiles(new CarbonFileFilter() {
            @Override
            public boolean accept(CarbonFile file) {
              if (file == null) {
                return false;
              }
              return file.getName().endsWith(".carbondata");
            }
          });
      if (carbonFiles == null || carbonFiles.length < 1) {
        throw new RuntimeException("Carbon data file not exists.");
      }
      String dataFilePath = carbonFiles[0].getAbsolutePath();

      Schema schema = CarbonSchemaReader
          .readSchema(dataFilePath)
          .asOriginOrder();

      assertEquals(schema.getFieldsLength(), 12);
      checkSchema(schema);
    } catch (Throwable e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testReadSchemaWithoutSchemaFilesSchema() {
    try {
      Schema schema = CarbonSchemaReader
          .readSchema(path)
          .asOriginOrder();
      checkSchema(schema);
    } catch (Throwable e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  public boolean checkSchema(Schema schema) {
    assertEquals(schema.getFields().length, 12);
    assert (schema.getFieldName(0).equalsIgnoreCase("stringField"));
    assert (schema.getFieldName(1).equalsIgnoreCase("shortField"));
    assert (schema.getFieldName(2).equalsIgnoreCase("intField"));
    assert (schema.getFieldName(3).equalsIgnoreCase("longField"));
    assert (schema.getFieldName(4).equalsIgnoreCase("doubleField"));
    assert (schema.getFieldName(5).equalsIgnoreCase("boolField"));
    assert (schema.getFieldName(6).equalsIgnoreCase("dateField"));
    assert (schema.getFieldName(7).equalsIgnoreCase("timeField"));
    assert (schema.getFieldName(8).equalsIgnoreCase("decimalField"));
    assert (schema.getFieldName(9).equalsIgnoreCase("varcharField"));
    assert (schema.getFieldName(10).equalsIgnoreCase("arrayField"));
    assert (schema.getFieldName(11).equalsIgnoreCase("floatField"));

    assert (schema.getFieldDataTypeName(0).equalsIgnoreCase("string"));
    assert (schema.getFieldDataTypeName(1).equalsIgnoreCase("short"));
    assert (schema.getFieldDataTypeName(2).equalsIgnoreCase("int"));
    assert (schema.getFieldDataTypeName(3).equalsIgnoreCase("long"));
    assert (schema.getFieldDataTypeName(4).equalsIgnoreCase("double"));
    assert (schema.getFieldDataTypeName(5).equalsIgnoreCase("boolean"));
    assert (schema.getFieldDataTypeName(6).equalsIgnoreCase("date"));
    assert (schema.getFieldDataTypeName(7).equalsIgnoreCase("timestamp"));
    assert (schema.getFieldDataTypeName(8).equalsIgnoreCase("decimal"));
    assert (schema.getFieldDataTypeName(9).equalsIgnoreCase("varchar"));
    assert (schema.getFieldDataTypeName(10).equalsIgnoreCase("array"));
    assert (schema.getArrayElementTypeName(10).equalsIgnoreCase("String"));
    assert (schema.getFieldDataTypeName(11).equalsIgnoreCase("float"));
    return true;
  }

  @Test
  public void testReadSchemaFromIndexFile() {
    try {
      CarbonFile[] carbonFiles = FileFactory
          .getCarbonFile(path)
          .listFiles(new CarbonFileFilter() {
            @Override
            public boolean accept(CarbonFile file) {
              if (file == null) {
                return false;
              }
              return file.getName().endsWith(".carbonindex");
            }
          });
      if (carbonFiles == null || carbonFiles.length < 1) {
        throw new RuntimeException("Carbon index file not exists.");
      }
      String indexFilePath = carbonFiles[0].getAbsolutePath();

      Schema schema = CarbonSchemaReader
          .readSchema(indexFilePath)
          .asOriginOrder();

      assertEquals(schema.getFieldsLength(), 12);
      checkSchema(schema);
    } catch (Throwable e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testReadSchemaAndCheckFilesSchema() {
    try {
      Schema schema = CarbonSchemaReader
          .readSchema(path, true)
          .asOriginOrder();
      checkSchema(schema);
    } catch (Throwable e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testReadSchemaWithDifferentSchema() {
    try {
      int num = 10;
      Field[] fields = new Field[2];
      fields[0] = new Field("name", DataTypes.STRING);
      fields[1] = new Field("age", DataTypes.INT);
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("testReadSchemaWithDifferentSchema")
          .build();

      for (int i = 0; i < num; i++) {
        writer.write(new String[]{"robot" + (i % 10), String.valueOf(i)});
      }
      writer.close();
      try {
        CarbonSchemaReader
            .readSchema(path, true)
            .asOriginOrder();
        Assert.fail();
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage()
            .equalsIgnoreCase("Schema is different between different files."));
      }
    } catch (Throwable e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(new File(path));
  }
}
