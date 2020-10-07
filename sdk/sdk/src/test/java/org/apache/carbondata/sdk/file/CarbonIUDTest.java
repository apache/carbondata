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
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.Field;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.*;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;

import org.apache.avro.generic.GenericData;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

public class CarbonIUDTest {

  @Test
  public void testDelete() throws Exception {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("doubleField", DataTypes.DOUBLE);
    TestUtil.writeFilesAndVerify(10, new Schema(fields), path);
    CarbonIUD.getInstance().delete(path, "age", "0").commit();
    CarbonIUD.getInstance().delete(path, "age", "1").delete(path, "name", "robot1").commit();
    CarbonIUD.getInstance().delete(path, "age", "2").delete(path, "name", "robot2")
        .delete(path, "doubleField", "1.0").commit();
    CarbonIUD.getInstance().delete(path, "name", "robot3").delete(path, "name", "robot4")
        .delete(path, "name", "robot5").commit();
    CarbonIUD.getInstance().delete(path, "name", "robot6").delete(path, "name", "robot7")
        .delete(path, "name", "robot8").delete(path, "age", "6").delete(path, "age", "7").commit();

    CarbonReader reader =
        CarbonReader.builder(path).projection(new String[] { "name", "age", "doubleField" })
            .build();

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      assert (((String) row[0]).contains("robot8") || ((String) row[0]).contains("robot9"));
      assert (((int) (row[1])) > 7);
      assert ((double) row[2] > 3.5);
      i++;
    }
    Assert.assertEquals(i, 2);
    reader.close();
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testUpdateOnDateType() throws Exception {
    String path = "./testWriteFiles";
    CarbonProperties.getInstance()
            .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                    CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
            .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
                    CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);
    FileUtils.deleteDirectory(new File(path));
    Field[] fields = new Field[3];
    fields[0] = new Field("intField", DataTypes.INT);
    fields[1] = new Field("dateField", DataTypes.DATE);
    fields[2] = new Field("timeField", DataTypes.TIMESTAMP);
    CarbonWriter writer = CarbonWriter.builder()
            .outputPath(path)
            .withCsvInput(new Schema(fields))
            .writtenBy("IUDTest")
            .build();
    for (int i = 0; i < 10; i++) {
      String[] row2 = new String[]{
              String.valueOf(i % 10000),
              "2019-03-02",
              "2019-02-12 03:03:34",
      };
      writer.write(row2);
    }
    writer.close();
    CarbonIUD.getInstance().update(path, "intField", "0", "intField", "20").commit();
    CarbonReader reader =
            CarbonReader.builder(path).projection(new String[] { "intField", "dateField", "timeField" })
                    .build();
    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      assert ((int) row[0] != 0);
      assert (row[1].equals("2019-03-02"));
      assert (row[2].equals("2019-02-12 03:03:34"));
      i++;
    }
    Assert.assertEquals(i, 10);
    reader.close();
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testDeleteWithConditionalExpressions() throws Exception {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("doubleField", DataTypes.DOUBLE);
    TestUtil.writeFilesAndVerify(20, new Schema(fields), path);
    ColumnExpression columnExpression1 = new ColumnExpression("age", DataTypes.INT);
    LessThanExpression lessThanExpression =
        new LessThanExpression(columnExpression1, new LiteralExpression("3", DataTypes.INT));
    CarbonIUD.getInstance().delete(path, lessThanExpression);

    ColumnExpression columnExpression2 = new ColumnExpression("age", DataTypes.INT);
    GreaterThanExpression greaterThanExpression =
        new GreaterThanExpression(columnExpression2, new LiteralExpression("16", DataTypes.INT));
    CarbonIUD.getInstance().delete(path, greaterThanExpression);

    ColumnExpression columnExpression3 = new ColumnExpression("age", DataTypes.INT);
    LessThanEqualToExpression lessThanEqualToExpression =
        new LessThanEqualToExpression(columnExpression3, new LiteralExpression("5", DataTypes.INT));
    CarbonIUD.getInstance().delete(path, lessThanEqualToExpression);

    ColumnExpression columnExpression4 = new ColumnExpression("age", DataTypes.INT);
    GreaterThanEqualToExpression greaterThanEqualToExpression =
        new GreaterThanEqualToExpression(columnExpression4,
            new LiteralExpression("15", DataTypes.INT));
    CarbonIUD.getInstance().delete(path, greaterThanEqualToExpression);

    CarbonReader reader =
        CarbonReader.builder(path).projection(new String[] { "name", "age", "doubleField" })
            .build();

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      assert (!((int) row[1] <= 5 || (int) row[1] >= 15));
      i++;
    }
    Assert.assertEquals(i, 9);
    reader.close();
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testDeleteWithAndFilter() throws Exception {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("doubleField", DataTypes.DOUBLE);
    TestUtil.writeFilesAndVerify(20, new Schema(fields), path);

    ColumnExpression columnExpression = new ColumnExpression("doubleField", DataTypes.DOUBLE);
    LessThanExpression lessThanExpression1 =
        new LessThanExpression(columnExpression, new LiteralExpression("3.5", DataTypes.DOUBLE));

    ColumnExpression columnExpression2 = new ColumnExpression("age", DataTypes.INT);
    LessThanExpression lessThanExpression2 =
        new LessThanExpression(columnExpression2, new LiteralExpression("4", DataTypes.INT));

    AndExpression andExpression = new AndExpression(lessThanExpression1, lessThanExpression2);
    CarbonIUD.getInstance().delete(path, andExpression);

    CarbonReader reader =
        CarbonReader.builder(path).projection(new String[] { "name", "age", "doubleField" })
            .build();

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      assert (!((int) row[1] < 4));
      i++;
    }
    Assert.assertEquals(i, 16);
    reader.close();
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testDeleteWithORFilter() throws Exception {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("doubleField", DataTypes.DOUBLE);
    TestUtil.writeFilesAndVerify(20, new Schema(fields), path);

    ColumnExpression columnExpression = new ColumnExpression("doubleField", DataTypes.DOUBLE);
    LessThanExpression lessThanExpression =
        new LessThanExpression(columnExpression, new LiteralExpression("3.5", DataTypes.DOUBLE));

    ColumnExpression columnExpression2 = new ColumnExpression("age", DataTypes.INT);
    GreaterThanExpression greaterThanExpression =
        new GreaterThanExpression(columnExpression2, new LiteralExpression("11", DataTypes.INT));

    OrExpression orExpression = new OrExpression(lessThanExpression, greaterThanExpression);
    CarbonIUD.getInstance().delete(path, orExpression);

    CarbonReader reader =
        CarbonReader.builder(path).projection(new String[] { "name", "age", "doubleField" })
            .build();

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      assert (!((int) row[1] > 11 || (double) row[2] < 3.5));
      i++;
    }
    Assert.assertEquals(i, 5);
    reader.close();
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testDeleteAtMultiplePaths() throws Exception {
    String path1 = "./testWriteFiles1";
    String path2 = "./testWriteFiles2";
    FileUtils.deleteDirectory(new File(path1));
    FileUtils.deleteDirectory(new File(path2));
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("doubleField", DataTypes.DOUBLE);
    TestUtil.writeFilesAndVerify(10, new Schema(fields), path1);
    TestUtil.writeFilesAndVerify(10, new Schema(fields), path2);
    CarbonIUD.getInstance().delete(path1, "age", "2").delete(path2, "age", "3")
        .delete(path1, "name", "robot2").delete(path2, "name", "robot3").commit();

    CarbonReader reader1 =
        CarbonReader.builder(path1).projection(new String[] { "name", "age", "doubleField" })
            .build();

    int i = 0;
    while (reader1.hasNext()) {
      Object[] row = (Object[]) reader1.readNextRow();
      assert (!(((String) row[0]).contains("robot2")));
      assert (((int) (row[1])) != 2);
      assert ((double) row[2] != 1.0);
      i++;
    }
    Assert.assertEquals(i, 9);
    reader1.close();
    FileUtils.deleteDirectory(new File(path1));

    CarbonReader reader2 =
        CarbonReader.builder(path2).projection(new String[] { "name", "age", "doubleField" })
            .build();

    i = 0;
    while (reader2.hasNext()) {
      Object[] row = (Object[]) reader2.readNextRow();
      assert (!(((String) row[0]).contains("robot3")));
      assert (((int) (row[1])) != 3);
      assert ((double) row[2] != 1.5);
      i++;
    }
    Assert.assertEquals(i, 9);
    reader2.close();
    FileUtils.deleteDirectory(new File(path2));
  }

  @Test
  public void testDeleteInMultipleSegments() throws Exception {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("doubleField", DataTypes.DOUBLE);
    TestUtil.writeFilesAndVerify(10, new Schema(fields), path);
    TestUtil.writeFilesAndVerify(10, new Schema(fields), path);
    TestUtil.writeFilesAndVerify(10, new Schema(fields), path);
    CarbonIUD.getInstance().delete(path, "age", "2").delete(path, "name", "robot2").commit();

    CarbonReader reader =
        CarbonReader.builder(path).projection(new String[] { "name", "age", "doubleField" })
            .build();

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      assert (!(((String) row[0]).contains("robot2")));
      assert (((int) (row[1])) != 2);
      assert ((double) row[2] != 1.0);
      i++;
    }
    Assert.assertEquals(i, 27);
    reader.close();
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testUpdate() throws Exception {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("doubleField", DataTypes.DOUBLE);
    TestUtil.writeFilesAndVerify(20, new Schema(fields), path);
    CarbonIUD.getInstance().update(path, "age", "2", "age", "3").commit();
    CarbonIUD.getInstance().update(path, "name", "robot2", "age", "3")
        .update(path, "age", "12", "name", "robot13").commit();

    ColumnExpression columnExpression1 = new ColumnExpression("age", DataTypes.INT);
    EqualToExpression equalToExpression1 =
        new EqualToExpression(columnExpression1, new LiteralExpression("3", DataTypes.INT));

    CarbonReader reader1 =
        CarbonReader.builder(path).projection(new String[] { "name", "age", "doubleField" })
            .filter(equalToExpression1).build();

    int i = 0;
    while (reader1.hasNext()) {
      Object[] row = (Object[]) reader1.readNextRow();
      assert (((int) (row[1])) == 3);
      i++;
    }
    Assert.assertEquals(i, 3);
    reader1.close();

    CarbonReader reader2 =
        CarbonReader.builder(path).projection(new String[] { "name", "age", "doubleField" })
            .build();

    i = 0;
    while (reader2.hasNext()) {
      Object[] row = (Object[]) reader2.readNextRow();
      assert (((int) (row[1])) != 2);
      i++;
    }
    Assert.assertEquals(i, 20);
    reader2.close();
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testDeleteOnUpdatedRows() throws Exception {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("doubleField", DataTypes.DOUBLE);
    TestUtil.writeFilesAndVerify(20, new Schema(fields), path);
    Map<String, String> updateColumnToValue = new HashMap<>();
    updateColumnToValue.put("name", "robot");
    updateColumnToValue.put("age", "24");

    ColumnExpression columnExpression = new ColumnExpression("age", DataTypes.INT);
    LessThanExpression lessThanExpression =
        new LessThanExpression(columnExpression, new LiteralExpression("10", DataTypes.INT));
    CarbonIUD.getInstance().update(path, lessThanExpression, updateColumnToValue);
    CarbonIUD.getInstance().delete(path, "doubleField", "2.0").commit();

    ColumnExpression columnExpression1 = new ColumnExpression("age", DataTypes.INT);
    EqualToExpression equalToExpression =
        new EqualToExpression(columnExpression1, new LiteralExpression("24", DataTypes.INT));

    CarbonReader reader =
        CarbonReader.builder(path).projection(new String[] { "name", "age", "doubleField" })
            .filter(equalToExpression).build();

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      assert (((String) row[0]).contains("robot"));
      assert (((int) (row[1])) == 24);
      i++;
    }
    Assert.assertEquals(i, 9);
    reader.close();
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testDeleteAndUpdateTogether() throws Exception {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("doubleField", DataTypes.DOUBLE);
    TestUtil.writeFilesAndVerify(20, new Schema(fields), path);
    CarbonIUD.getInstance().delete(path, "name", "robot0").delete(path, "name", "robot1")
        .delete(path, "name", "robot2").delete(path, "age", "0").delete(path, "age", "1")
        .update(path, "name", "robot1", "name", "Karan").update(path, "age", "1", "age", "0")
        .commit();

    CarbonReader reader =
        CarbonReader.builder(path).projection(new String[] { "name", "age", "doubleField" })
            .build();

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      assert (!((String) row[0]).contains("Karan"));
      assert (((int) (row[1])) > 1);
      i++;
    }
    Assert.assertEquals(i, 18);
    reader.close();
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testIUDOnDifferentDataType() throws Exception {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    Field[] fields = new Field[11];
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
    CarbonWriter writer =
        CarbonWriter.builder().outputPath(path).withLoadOption("complex_delimiter_level_1", "#")
            .withCsvInput(new Schema(fields)).writtenBy("IUDTest").build();
    for (int i = 0; i < 10; i++) {
      boolean boolValue = true;
      String decimalValue = "12.345";
      String dateValue = "2019-03-02";
      if (i == 6) {
        boolValue = false;
      }
      if (i == 7) {
        decimalValue = "12.366";
      }
      if (i == 8) {
        dateValue = "2019-03-03";
      }
      String[] row2 =
          new String[] { "robot" + (i % 10), String.valueOf(i % 10000), String.valueOf(i),
              String.valueOf(Long.MAX_VALUE - i), String.valueOf((double) i / 2),
              String.valueOf(boolValue), dateValue, "2019-02-12 03:03:34", decimalValue,
              "varchar" + (i % 10), "Hello#World#From#Carbon" };
      writer.write(row2);
    }
    writer.close();
    CarbonIUD.getInstance().delete(path, "stringField", "robot0").commit();
    CarbonIUD.getInstance().delete(path, "shortField", "1").commit();
    CarbonIUD.getInstance().delete(path, "intField", "2").commit();
    CarbonIUD.getInstance().delete(path, "longField", "9223372036854775804").commit();
    CarbonIUD.getInstance().delete(path, "doubleField", "2.0").commit();
    CarbonIUD.getInstance().delete(path, "varcharField", "varchar5").commit();
    CarbonIUD.getInstance().delete(path, "boolField", "false").commit();
    CarbonIUD.getInstance().delete(path, "decimalField", "12.37").commit();
    CarbonIUD.getInstance().delete(path, "dateField", "2019-03-03").commit();
    CarbonIUD.getInstance().delete(path, "timeField", "2019-02-12 03:03:34").commit();

    File[] indexFiles = new File(path).listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (name == null) {
          return false;
        }
        return name.endsWith("carbonindex");
      }
    });
    if (indexFiles == null || indexFiles.length < 1) {
      throw new RuntimeException("Carbon index file not exists.");
    }
    Schema schema = CarbonSchemaReader.readSchema(indexFiles[0].getAbsolutePath()).asOriginOrder();
    String[] strings = new String[schema.getFields().length];
    for (int i = 0; i < schema.getFields().length; i++) {
      strings[i] = (schema.getFields())[i].getFieldName();
    }
    CarbonReader reader = CarbonReader.builder(path).projection(strings).build();
    int i = 0;
    while (reader.hasNext()) {
      i++;
    }
    Assert.assertEquals(i, 0);
    reader.close();
    try {
      CarbonIUD.getInstance().delete(path, "arrayfield", "{Hello,World,From,Carbon}").commit();
    } catch (RuntimeException e) {
      Assert.assertTrue(
          e.getMessage().contains("IUD operation not supported for Complex data types"));
    }
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testIUDOnComplexType() throws Exception {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("mapRecord", DataTypes.createMapType(DataTypes.STRING, DataTypes.STRING));
    String mySchema =
        "{ " + "  \"name\": \"address\", " + "  \"type\": \"record\", " + "  \"fields\": [ "
            + "    { " + "      \"name\": \"name\", " + "      \"type\": \"string\" " + "    }, "
            + "    { " + "      \"name\": \"age\", " + "      \"type\": \"int\" " + "    }, "
            + "    { " + "      \"name\": \"mapRecord\", " + "      \"type\": { "
            + "        \"type\": \"map\", " + "        \"values\": \"string\" " + "      } "
            + "    } " + "  ] " + "} ";

    String json =
        "{\"name\":\"bob\", \"age\":10, \"mapRecord\": {\"street\": \"k-lane\", \"city\": \"bangalore\"}}";
    org.apache.avro.Schema nn = new org.apache.avro.Schema.Parser().parse(mySchema);
    GenericData.Record record = TestUtil.jsonToAvro(json, mySchema);
    CarbonWriter writer =
        CarbonWriter.builder().outputPath(path).withAvroInput(nn).writtenBy("CarbonIUDTest")
            .build();
    for (int i = 0; i < 10; i++) {
      writer.write(record);
    }
    writer.close();
    try {
      CarbonIUD.getInstance()
          .delete(path, "mapRecord", "{\"street\": \"k-lane\", \"city\": \"bangalore\"}").commit();
    } catch (RuntimeException e) {
      Assert.assertTrue(
          e.getMessage().contains("IUD operation not supported for Complex data types"));
    }
    FileUtils.deleteDirectory(new File(path));
  }
}
