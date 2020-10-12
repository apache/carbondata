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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.Field;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import org.apache.avro.generic.GenericData;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.avro.Schema;

public class AvroCarbonWriterTest {
  private String path = "./AvroCarbonWriterSuiteWriteFiles";
  String DATA_PATH = "./src/test/resources/file/";

  @Before
  @After
  public void cleanTestData() {
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testWriteBasic() throws IOException {
    FileUtils.deleteDirectory(new File(path));

    // Avro schema
    String avroSchema =
        "{" +
            "   \"type\" : \"record\"," +
            "   \"name\" : \"Acme\"," +
            "   \"fields\" : ["
            + "{ \"name\" : \"name\", \"type\" : \"string\" },"
            + "{ \"name\" : \"age\", \"type\" : \"int\" }]" +
        "}";

    String json = "{\"name\":\"bob\", \"age\":10}";

    // conversion to GenericData.Record
    GenericData.Record record = TestUtil.jsonToAvro(json, avroSchema);
    try {
      CarbonWriter writer = CarbonWriter.builder().outputPath(path)
          .withAvroInput(new Schema.Parser().parse(avroSchema)).writtenBy("AvroCarbonWriterTest").build();

      for (int i = 0; i < 100; i++) {
        writer.write(record);
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    File[] dataFiles = new File(path).listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT);
      }
    });
    Assert.assertNotNull(dataFiles);
    Assert.assertEquals(1, dataFiles.length);

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testWriteAllPrimitive() throws IOException {
    FileUtils.deleteDirectory(new File(path));

    // Avro schema
    // Supported Primitive Datatype.
    // 1. Boolean
    // 2. Int
    // 3. long
    // 4. float -> To carbon Internally it is double.
    // 5. double
    // 6. String

    // Not Supported
    // 1.NULL Datatype
    // 2.Bytes

    String avroSchema = "{\n" + "  \"name\" : \"myrecord\",\n"
        + "  \"namespace\": \"org.apache.parquet.avro\",\n" + "  \"type\" : \"record\",\n"
        + "  \"fields\" : [ "
        + " {\n" + "    \"name\" : \"myboolean\",\n" + "    \"type\" : \"boolean\"\n  },"
        + " {\n" + "    \"name\" : \"myint\",\n" + "    \"type\" : \"int\"\n" + "  }, "
        + " {\n    \"name\" : \"mylong\",\n" + "    \"type\" : \"long\"\n" + "  },"
        + " {\n   \"name\" : \"myfloat\",\n" + "    \"type\" : \"float\"\n" + "  }, "
        + " {\n \"name\" : \"mydouble\",\n" + "    \"type\" : \"double\"\n" + "  },"
        + " {\n \"name\" : \"mystring\",\n" + "    \"type\" : \"string\"\n" + "  }\n" + "] }";

    String json = "{"
        + "\"myboolean\":true, "
        + "\"myint\": 10, "
        + "\"mylong\": 7775656565,"
        + " \"myfloat\": 0.2, "
        + "\"mydouble\": 44.56, "
        + "\"mystring\":\"Ajantha\"}";


    // conversion to GenericData.Record
    GenericData.Record record = TestUtil.jsonToAvro(json, avroSchema);

    try {
      CarbonWriter writer = CarbonWriter.builder()
          .outputPath(path)
          
          .withAvroInput(new Schema.Parser().parse(avroSchema)).writtenBy("AvroCarbonWriterTest").build();

      for (int i = 0; i < 100; i++) {
        writer.write(record);
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    File[] dataFiles = new File(path).listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT);
      }
    });

    Assert.assertNotNull(dataFiles);
    Assert.assertEquals(1, dataFiles.length);

    FileUtils.deleteDirectory(new File(path));
  }


  @Test
  public void testWriteNestedRecord() throws IOException {
    FileUtils.deleteDirectory(new File(path));

    String newAvroSchema =
        "{" +
          " \"type\" : \"record\", " +
          "  \"name\" : \"userInfo\", "  +
          "  \"namespace\" : \"my.example\", " +
          "  \"fields\" : [{\"name\" : \"username\", " +
          "  \"type\" : \"string\", " +
          "  \"default\" : \"NONE\"}, " +

       " {\"name\" : \"age\", " +
       " \"type\" : \"int\", " +
       " \"default\" : -1}, " +

    "{\"name\" : \"address\", " +
     "   \"type\" : { " +
      "  \"type\" : \"record\", " +
       "   \"name\" : \"mailing_address\", " +
        "  \"fields\" : [ {" +
      "        \"name\" : \"street\", " +
       "       \"type\" : \"string\", " +
       "       \"default\" : \"NONE\"}, { " +

      " \"name\" : \"city\", " +
        "  \"type\" : \"string\", " +
        "  \"default\" : \"NONE\"}, " +
         "                 ]}, " +
     " \"default\" : {} " +
   " } " +
"}";

    String mySchema =
    "{" +
    "  \"name\": \"address\", " +
    "   \"type\": \"record\", " +
    "    \"fields\": [  " +
    "  { \"name\": \"name\", \"type\": \"string\"}, " +
    "  { \"name\": \"age\", \"type\": \"int\"}, " +
    "  { " +
    "    \"name\": \"address\", " +
    "      \"type\": { " +
    "    \"type\" : \"record\", " +
    "        \"name\" : \"my_address\", " +
    "        \"fields\" : [ " +
    "    {\"name\": \"street\", \"type\": \"string\"}, " +
    "    {\"name\": \"city\", \"type\": \"string\"} " +
    "  ]} " +
    "  } " +
    "] " +
    "}";

   String json = "{\"name\":\"bob\", \"age\":10, \"address\" : {\"street\":\"abc\", \"city\":\"bang\"}}";


    // conversion to GenericData.Record
    Schema nn = new Schema.Parser().parse(mySchema);
    GenericData.Record record = TestUtil.jsonToAvro(json, mySchema);

    try {
      CarbonWriter writer = CarbonWriter.builder().outputPath(path).withAvroInput(nn).writtenBy("AvroCarbonWriterTest").build();
      for (int i = 0; i < 100; i++) {
        writer.write(record);
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    File[] dataFiles = new File(path).listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT);
      }
    });
    Assert.assertNotNull(dataFiles);
    Assert.assertEquals(1, dataFiles.length);

    FileUtils.deleteDirectory(new File(path));
  }


  @Test
  public void testWriteNestedRecordWithMeasure() throws IOException {
    FileUtils.deleteDirectory(new File(path));

    String mySchema =
        "{" +
            "  \"name\": \"address\", " +
            "   \"type\": \"record\", " +
            "    \"fields\": [  " +
            "  { \"name\": \"name\", \"type\": \"string\"}, " +
            "  { \"name\": \"age\", \"type\": \"int\"}, " +
            "  { " +
            "    \"name\": \"address\", " +
            "      \"type\": { " +
            "    \"type\" : \"record\", " +
            "        \"name\" : \"my_address\", " +
            "        \"fields\" : [ " +
            "    {\"name\": \"street\", \"type\": \"string\"}, " +
            "    {\"name\": \"city\", \"type\": \"string\"} " +
            "  ]} " +
            "  } " +
            "] " +
            "}";

    String json = "{\"name\":\"bob\", \"age\":10, \"address\" : {\"street\":\"abc\", \"city\":\"bang\"}}";


    // conversion to GenericData.Record
    Schema nn = new Schema.Parser().parse(mySchema);
    GenericData.Record record = TestUtil.jsonToAvro(json, mySchema);

    try {
      CarbonWriter writer = CarbonWriter.builder().outputPath(path).withAvroInput(nn).writtenBy("AvroCarbonWriterTest").build();
      for (int i = 0; i < 100; i++) {
        writer.write(record);
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    File[] dataFiles = new File(path).listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT);
      }
    });
    Assert.assertNotNull(dataFiles);
    Assert.assertEquals(1, dataFiles.length);

    FileUtils.deleteDirectory(new File(path));
  }


  private void WriteAvroComplexData(String mySchema, String json, String[] sortColumns)
      throws IOException, InvalidLoadOptionException {

    // conversion to GenericData.Record
    Schema nn = new Schema.Parser().parse(mySchema);
    GenericData.Record record = TestUtil.jsonToAvro(json, mySchema);
    try {
      CarbonWriter writer =
          CarbonWriter.builder().outputPath(path).sortBy(sortColumns).withAvroInput(nn).writtenBy("AvroCarbonWriterTest").build();
      for (int i = 0; i < 100; i++) {
        writer.write(record);
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  private void WriteAvroComplexDataAndRead(String mySchema)
      throws IOException, InvalidLoadOptionException, InterruptedException {

    // conversion to GenericData.Record
    Schema nn = new Schema.Parser().parse(mySchema);
    try {
      CarbonWriter writer =
          CarbonWriter.builder()
              .outputPath(path)
              .withAvroInput(mySchema)
              .writtenBy("AvroCarbonWriterTest")
              .build();
      int numOfRows = 100000/100;
      int numOfWrite = 20000;
      int arrayLength = 300;
      for (int i = 0; i < numOfRows; i++) {
        StringBuffer aa1 = new StringBuffer();
        StringBuffer bb1 = new StringBuffer();
        StringBuffer cc1 = new StringBuffer();
        aa1.append("[0.1234567,0.2,-0.3,0.4]");
        bb1.append("[0.2123456]");
        cc1.append("[0.3123456]");
        for (int j = 1; j < arrayLength; j++) {
          aa1.append(",[1" + i + "" + j + ".1234567,1" + i + "" + j + ".2,-1" + i + "" + j + ".3,1" + i + "" + j + ".4]");
          bb1.append(",[2" + i + "" + j + ".2123456,-2" + i + "" + j + ".2]");
          cc1.append(",[3" + i + "" + j + ".3123456]");
        }
        String json = "{\"fileName\":\"bob\", \"id\":10, "
            + "   \"aa1\" : [" + aa1 + "], "
            + "\"bb1\" : [" + bb1 + "], " +
            "\"cc1\" : [" + cc1 + "]}";

        writer.write(json);
        if (i > 0 && i % numOfWrite == 0) {
          writer.close();
          writer =
              CarbonWriter.builder()
                  .outputPath(path)
                  .withAvroInput(mySchema)
                  .writtenBy("AvroCarbonWriterTest")
                  .build();
        }
      }
      writer.close();
      String[] projection = new String[nn.getFields().size()];
      for (int j = 0; j < nn.getFields().size(); j++) {
        projection[j] = nn.getFields().get(j).name();
      }
      CarbonReader carbonReader = CarbonReader.builder().projection(projection).withFolder(path).build();
      int sum = 0;
      while (carbonReader.hasNext()) {
        sum++;
        Object[] row = (Object[]) carbonReader.readNextRow();
        Assert.assertTrue(row.length == 5);
        Object[] aa1 = (Object[]) row[2];
        Assert.assertTrue(aa1.length == arrayLength);
        Object[] aa2 = (Object[]) aa1[1];
        Assert.assertTrue(aa2.length == 4 || aa2.length == 2 || aa2.length == 1);
      }
      Assert.assertTrue(sum == numOfRows);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testWriteComplexRecord() throws IOException, InvalidLoadOptionException {
    FileUtils.deleteDirectory(new File(path));

    String mySchema =
        "{" +
            "  \"name\": \"address\", " +
            "   \"type\": \"record\", " +
            "    \"fields\": [  " +
            "  { \"name\": \"name\", \"type\": \"string\"}, " +
            "  { \"name\": \"age\", \"type\": \"int\"}, " +
            "  { " +
            "    \"name\": \"address\", " +
            "      \"type\": { " +
            "    \"type\" : \"record\", " +
            "        \"name\" : \"my_address\", " +
            "        \"fields\" : [ " +
            "    {\"name\": \"street\", \"type\": \"string\"}, " +
            "    {\"name\": \"city\", \"type\": \"string\"} " +
            "  ]} " +
            "  }, " +
            "  {\"name\" :\"doorNum\", " +
            "   \"type\" : { " +
            "   \"type\" :\"array\", " +
            "   \"items\":{ " +
            "   \"name\" :\"EachdoorNums\", " +
            "   \"type\" : \"int\", " +
            "   \"default\":-1} " +
            "              } " +
            "  }] " +
            "}";

    String json = "{\"name\":\"bob\", \"age\":10, \"address\" : {\"street\":\"abc\", \"city\":\"bang\"}, "
        + "   \"doorNum\" : [1,2,3,4]}";

    WriteAvroComplexData(mySchema, json, null);

    File[] dataFiles = new File(path).listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT);
      }
    });
    Assert.assertNotNull(dataFiles);
    Assert.assertEquals(1, dataFiles.length);

    FileUtils.deleteDirectory(new File(path));
  }


  @Test
  public void testWriteComplexRecordWithSortColumns() throws IOException {
    FileUtils.deleteDirectory(new File(path));

    String mySchema =
        "{" +
            "  \"name\": \"address\", " +
            "   \"type\": \"record\", " +
            "    \"fields\": [  " +
            "  { \"name\": \"name\", \"type\": \"string\"}, " +
            "  { \"name\": \"age\", \"type\": \"int\"}, " +
            "  { " +
            "    \"name\": \"address\", " +
            "      \"type\": { " +
            "    \"type\" : \"record\", " +
            "        \"name\" : \"my_address\", " +
            "        \"fields\" : [ " +
            "    {\"name\": \"street\", \"type\": \"string\"}, " +
            "    {\"name\": \"city\", \"type\": \"string\"} " +
            "  ]} " +
            "  }, " +
            "  {\"name\" :\"doorNum\", " +
            "   \"type\" : { " +
            "   \"type\" :\"array\", " +
            "   \"items\":{ " +
            "   \"name\" :\"EachdoorNums\", " +
            "   \"type\" : \"int\", " +
            "   \"default\":-1} " +
            "              } " +
            "  }] " +
            "}";

    String json = "{\"name\":\"bob\", \"age\":10, \"address\" : {\"street\":\"abc\", \"city\":\"bang\"}, "
        + "   \"doorNum\" : [1,2,3,4]}";

    try {
      WriteAvroComplexData(mySchema, json, new String[] { "doorNum" });
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(true);
    }
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testWriteArrayArrayFloat() throws IOException {
    FileUtils.deleteDirectory(new File(path));

    String mySchema =
        "{" +
            "  \"name\": \"address\", " +
            "   \"type\": \"record\", " +
            "    \"fields\": [  " +
            "  { \"name\": \"fileName\", \"type\": \"string\"}, " +
            "  { \"name\": \"id\", \"type\": \"int\"}, " +
            "  {\"name\" :\"aa1\", " +
            "   \"type\" : { " +
            "   \"type\" :\"array\", " +
            "   \"items\":{ " +
            "   \"name\" :\"aa2\", " +
            "   \"type\" :\"array\", " +
            "   \"items\":{ " +
            "   \"name\" :\"f1\", " +
            "   \"type\" : \"float\", " +
            "   \"default\":-1}}}}," +
            "  {\"name\" :\"bb1\", " +
            "   \"type\" : { " +
            "   \"type\" :\"array\", " +
            "   \"items\":{ " +
            "   \"name\" :\"bb2\", " +
            "   \"type\" :\"array\", " +
            "   \"items\":{ " +
            "   \"name\" :\"f2\", " +
            "   \"type\" : \"float\", " +
            "   \"default\":-1}}}}," +
            "  {\"name\" :\"cc1\", " +
            "   \"type\" : { " +
            "   \"type\" :\"array\", " +
            "   \"items\":{ " +
            "   \"name\" :\"cc2\", " +
            "   \"type\" :\"array\", " +
            "   \"items\":{ " +
            "   \"name\" :\"f3\", " +
            "   \"type\" : \"float\", " +
            "   \"default\":-1}}}}" +
            "] " +
            "}";

    try {
      WriteAvroComplexDataAndRead(mySchema);
      Assert.assertTrue(true);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testExceptionForDuplicateColumns() throws IOException, InvalidLoadOptionException {
    Field[] field = new Field[2];
    field[0] = new Field("name", DataTypes.STRING);
    field[1] = new Field("name", DataTypes.STRING);
    CarbonWriterBuilder writer = CarbonWriter.builder()
        .uniqueIdentifier(System.currentTimeMillis()).outputPath(path);

    try {
      writer.withCsvInput(new org.apache.carbondata.sdk.file.Schema(field)).writtenBy("AvroCarbonWriterTest").build();
      Assert.fail();
    } catch (Exception e) {
      assert(e.getMessage().contains("Duplicate column name found in table schema"));
    }
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testExceptionForInvalidDate() throws IOException, InvalidLoadOptionException {
    Field[] field = new Field[2];
    field[0] = new Field("name", DataTypes.STRING);
    field[1] = new Field("date", DataTypes.DATE);
    CarbonWriterBuilder writer = CarbonWriter.builder()
        .uniqueIdentifier(System.currentTimeMillis()).outputPath(path);

    try {
      Map<String, String> loadOptions = new HashMap<String, String>();
      loadOptions.put("bad_records_action", "fail");
      CarbonWriter carbonWriter =
          writer.withLoadOptions(loadOptions).withCsvInput(new org.apache.carbondata.sdk.file.Schema(field)).writtenBy("AvroCarbonWriterTest").build();
      carbonWriter.write(new String[] { "k", "20-02-2233" });
      carbonWriter.close();
      Assert.fail();
    } catch (Exception e) {
      assert(e.getMessage().contains("Data load failed due to bad record"));
    }
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testWriteBasicForFloat() throws IOException {
    FileUtils.deleteDirectory(new File(path));

    // Avro schema
    String avroSchema =
        "{" + "   \"type\" : \"record\"," + "   \"name\" : \"Acme\"," + "   \"fields\" : ["
            + "{ \"name\" : \"name\", \"type\" : \"string\" },"
            + "{ \"name\" : \"age\", \"type\" : \"int\" }," + "{ \"name\" : \"salary\", \"type\" "
            + ": \"float\" }]" + "}";

    String json = "{\"name\":\"bob\", \"age\":10, \"salary\":10.100}";

    // conversion to GenericData.Record
    GenericData.Record record = TestUtil.jsonToAvro(json, avroSchema);
    try {
      CarbonWriter writer = CarbonWriter.builder().outputPath(path)
          .withAvroInput(new Schema.Parser().parse(avroSchema)).writtenBy("AvroCarbonWriterTest").build();

      for (int i = 0; i < 100; i++) {
        writer.write(record);
      }
      writer.close();
      TableInfo tableInfo = SchemaReader.inferSchema(AbsoluteTableIdentifier.from(path, "",
          ""), false);
      List<String> dataTypes = new ArrayList<>();
      for(ColumnSchema columnSchema: tableInfo.getFactTable().getListOfColumns()) {
        dataTypes.add(columnSchema.getDataType().toString());
      }
      assert(dataTypes.contains("FLOAT"));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  private void loadAvroFile(String filePath) throws IOException, InvalidLoadOptionException {
    CarbonWriterBuilder carbonWriterBuilder = new CarbonWriterBuilder();
    CarbonWriter carbonWriter = carbonWriterBuilder.withAvroPath(filePath)
        .outputPath(path).writtenBy("AvroCarbonWriterTest").build();
    carbonWriter.write();
    carbonWriter.close();
    File[] dataFiles = new File(path).listFiles();
    assert (Objects.requireNonNull(dataFiles).length == 2);
  }

  @Test
  public void testAvroFileLoadWithNestedSchema() {
    String filePath = DATA_PATH + "nested_schema.avro";
    try {
      loadAvroFile(filePath);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  private void assertFetchedRow(Object[] row, int sum) {
    Assert.assertTrue(row.length == 3);
    if (sum % 2 != 0) {
      Assert.assertEquals(row[0], "Alyssa");
      Assert.assertNull(row[1]);
    } else {
      Assert.assertEquals(row[0], "Ben");
      Assert.assertEquals(((Object[]) row[1])[0], "red");
    }
  }

  @Test
  public void testLoadingAvroFileAndReadingCarbonFile() throws IOException {
    String filePath = DATA_PATH + "avro_files/users.avro";
    CarbonReader carbonReader = null;
    try {
      loadAvroFile(filePath);
      carbonReader = CarbonReader.builder().withFolder(path).build();
      int sum = 0;
      while (carbonReader.hasNext()) {
        sum++;
        Object[] row = (Object[]) carbonReader.readNextRow();
        assertFetchedRow(row, sum);
      }
      Assert.assertTrue(sum == 2);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    } finally {
      if (carbonReader != null) {
        carbonReader.close();
      }
    }
  }

  @Test
  public void testMultipleAvroFileLoad() throws IOException {
    String filePath = DATA_PATH + "avro_files";
    CarbonReader carbonReader = null;
    try {
      loadAvroFile(filePath);
      carbonReader = CarbonReader.builder().withFolder(path).build();
      int sum = 0;
      while (carbonReader.hasNext()) {
        sum++;
        Object[] row = (Object[]) carbonReader.readNextRow();
        assertFetchedRow(row, sum);
      }
      Assert.assertTrue(sum == 6);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    } finally {
      if (carbonReader != null) {
        carbonReader.close();
      }
    }
  }

  @Test
  public void testSelectedAvroFileLoadInDirectory() throws IOException {
    String filePath = DATA_PATH + "avro_files";
    CarbonReader carbonReader = null;
    try {
      CarbonWriterBuilder carbonWriterBuilder = new CarbonWriterBuilder();
      List<String> fileList = new ArrayList<>();
      fileList.add("users_2.avro");
      fileList.add("users.avro");
      CarbonWriter carbonWriter = carbonWriterBuilder.withAvroPath(filePath, fileList)
          .outputPath(path).writtenBy("AvroCarbonWriterTest").build();
      carbonWriter.write();
      carbonWriter.close();
      File[] dataFiles = new File(path).listFiles();
      assert (Objects.requireNonNull(dataFiles).length == 2);
      carbonReader = CarbonReader.builder().withFolder(path).build();
      int sum = 0;
      while (carbonReader.hasNext()) {
        sum++;
        Object[] row = (Object[]) carbonReader.readNextRow();
        assertFetchedRow(row, sum);
      }
      Assert.assertTrue(sum == 4);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    } finally {
      if (carbonReader != null) {
        carbonReader.close();
      }
    }
  }
}
