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

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.Field;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Test suite for {@link JsonCarbonWriter}
 */
public class JSONCarbonWriterTest {
  String path = "./testLoadJsonFIle";

  @Before
  @After
  public void cleanTestData() {
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private Schema buildSchema() {
    Field[] fields = new Field[9];
    fields[0] = new Field("stringField", "STRING");
    fields[1] = new Field("intField", "INT");
    fields[2] = new Field("shortField", "SHORT");
    fields[3] = new Field("longField", "LONG");
    fields[4] = new Field("doubleField", "DOUBLE");
    fields[5] = new Field("boolField", "BOOLEAN");
    fields[6] = new Field("dateField", "DATE");
    fields[7] = new Field("timeField", "TIMESTAMP");
    fields[8] = new Field("decimalField", "DECIMAL");
    return new Schema(fields);
  }

  private void loadJsonFile(String filePath, Schema schema)
      throws IOException, InvalidLoadOptionException {
    CarbonWriterBuilder carbonWriterBuilder = new CarbonWriterBuilder();
    CarbonWriter carbonWriter = carbonWriterBuilder.withJsonPath(filePath).outputPath(path)
        .withJsonInput(schema).writtenBy("JSONCarbonWriterTest").build();
    carbonWriter.write();
    carbonWriter.close();
    File[] dataFiles = new File(path).listFiles();
    assert (Objects.requireNonNull(dataFiles).length == 2);
  }

  @Test
  public void testJsonFileLoadSingleRow() throws IOException,
      InvalidLoadOptionException, InterruptedException {
    String filePath = "./src/test/resources/file/json_files/allPrimitiveType.json";
    Schema schema = buildSchema();
    loadJsonFile(filePath, schema);

    CarbonReader reader = CarbonReader.builder("./testLoadJsonFIle", "_temp")
        .projection(new String[]{"stringField", "boolField", "decimalField", "longField"}).build();
    int id = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      Assert.assertEquals(row[0], "nihal\"ojha\"");
      Assert.assertEquals(row[1], false);
      Assert.assertEquals(row[2], (new BigDecimal("55.35")).setScale(2, BigDecimal.ROUND_FLOOR));
      Assert.assertEquals(row[3], (long) 1234567);
      id++;
    }
    assert (id == 1);
    reader.close();
  }

  @Test
  public void testJsonFileLoadWithMultipleRow() throws IOException,
      InvalidLoadOptionException, InterruptedException {
    String filePath = "./src/test/resources/file/json_files/allPrimitiveTypeMultipleRows.json";
    Schema schema = buildSchema();
    loadJsonFile(filePath, schema);
    CarbonReader reader = CarbonReader.builder("./testLoadJsonFIle", "_temp")
        .projection(new String[]{"stringField", "boolField", "shortField", "doubleField"}).build();
    int count = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      if (count == 0) {
        Assert.assertEquals(row[0], "nihal");
        Assert.assertEquals(row[1], false);
        Assert.assertEquals((((Short) row[2]).intValue()), 26);
        Assert.assertEquals(row[3], 23.3333);
      } else if (count == 1) {
        Assert.assertEquals(row[0], "ab");
        Assert.assertEquals(row[1], false);
        Assert.assertEquals((((Short) row[2]).intValue()), 25);
        Assert.assertEquals(row[3], 23.3333);
      }
      count++;
    }
    assert (count == 4);
    reader.close();
  }

  @Test
  public void testJsonFileLoadWithComplexSchema() throws IOException,
      InvalidLoadOptionException, InterruptedException {
    String filePath = "../../integration/spark/src/test/resources/jsonFiles/" +
        "data/arrayOfarrayOfarrayOfStruct.json";
    Field[] fields = new Field[3];
    fields[0] = new Field("name", "STRING");
    fields[1] = new Field("age", "INT");

    StructField[] structFields = new StructField[2];
    structFields[0] = new StructField("street", DataTypes.STRING);
    structFields[1] = new StructField("city", DataTypes.STRING);
    Field structType = new Field("structField", "struct", Arrays.asList(structFields));

    List<StructField> arrayField3 = new ArrayList<>();
    arrayField3.add(new StructField("structField", structType.getDataType()
        , Arrays.asList(structFields)));
    Field arrayLevel3 = new Field("arr3", "array", arrayField3);

    List<StructField> arrayField2 = new ArrayList<>();
    arrayField2.add(new StructField("arrayField3", arrayLevel3.getDataType(), arrayField3));
    Field arrayLevel2 = new Field("arr2", "array", arrayField2);

    List<StructField> arrayField = new ArrayList<>();
    arrayField.add(new StructField("arrayField2", arrayLevel2.getDataType(), arrayField2));
    Field arrayLevel1 = new Field("BuildNum", "array", arrayField);

    fields[2] = arrayLevel1;
    Schema schema = new Schema(fields);
    loadJsonFile(filePath, schema);

    CarbonReader reader = CarbonReader.builder("./testLoadJsonFIle", "_temp")
        .projection(new String[]{"name", "age", "BuildNum"}).build();
    int count = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      Assert.assertEquals(row[0], "ajantha");
      Assert.assertEquals(row[1], 26);
      Object[] outerArray = (Object[]) row[2];
      Assert.assertEquals(outerArray.length, 2);
      Object[] middleArray = (Object[]) outerArray[0];
      Assert.assertEquals(middleArray.length, 2);
      Object[] innerArray1 = (Object[]) middleArray[0];
      Assert.assertEquals(innerArray1.length, 3);
      Object[] structField1 = (Object[]) innerArray1[0];
      Assert.assertEquals(structField1.length, 2);
      Assert.assertEquals(structField1[0], "abc");
      Assert.assertEquals(structField1[1], "city1");
      Object[] structField2 = (Object[]) innerArray1[1];
      Assert.assertEquals(structField2.length, 2);
      Assert.assertEquals(structField2[0], "def");
      Assert.assertEquals(structField2[1], "city2");
      count++;
    }
    assert (count == 1);
    reader.close();
  }

  @Test
  public void testMultiplJsonFileLoad() throws IOException,
      InvalidLoadOptionException, InterruptedException {
    String filePath = "./src/test/resources/file/json_files";
    Schema schema = buildSchema();
    loadJsonFile(filePath, schema);
    CarbonReader reader = CarbonReader.builder("./testLoadJsonFIle", "_temp")
        .projection(new String[]{"longField", "boolField", "decimalField", "doubleField"}).build();
    int count = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      Assert.assertEquals(row[0], (long) 1234567);
      Assert.assertEquals(row[1], false);
      Assert.assertEquals(row[2], (new BigDecimal("55.35")).setScale(2, BigDecimal.ROUND_FLOOR));
      Assert.assertEquals(row[3], 23.3333);
      count++;
    }
    assert (count == 6);
    reader.close();
  }

  @Test
  public void testSelectedJsonFileLoadInDirectory() throws IOException,
      InvalidLoadOptionException, InterruptedException {
    String filePath = "./src/test/resources/file/json_files";
    Schema schema = buildSchema();
    CarbonWriterBuilder carbonWriterBuilder = new CarbonWriterBuilder();
    List<String> fileList = new ArrayList<>();
    fileList.add("allPrimitiveType.json");
    fileList.add("allPrimitiveTypeMultipleRows.json");
    CarbonWriter carbonWriter = carbonWriterBuilder.withJsonPath(filePath, fileList).outputPath(path)
        .withJsonInput(schema).writtenBy("JSONCarbonWriterTest").build();
    carbonWriter.write();
    carbonWriter.close();
    File[] dataFiles = new File(path).listFiles();
    assert (Objects.requireNonNull(dataFiles).length == 2);
    CarbonReader reader = CarbonReader.builder("./testLoadJsonFIle", "_temp")
        .projection(new String[]{"longField", "boolField", "decimalField", "doubleField"}).build();
    int count = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      Assert.assertEquals(row[0], (long) 1234567);
      Assert.assertEquals(row[1], false);
      Assert.assertEquals(row[2], (new BigDecimal("55.35")).setScale(2, BigDecimal.ROUND_FLOOR));
      Assert.assertEquals(row[3], 23.3333);
      count++;
    }
    assert (count == 5);
    reader.close();
  }
}