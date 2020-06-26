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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.ArrayType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalType;
import org.apache.carbondata.core.metadata.datatype.Field;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.datatype.StructType;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonFooterReaderV3;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.FileFooter3;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.carbondata.sdk.file.utils.SDKUtil.readObjects;

/**
 * Test suite for {@link CSVCarbonWriter}
 */
public class CSVCarbonWriterTest {
  String path = "./testCsvFileLoad";

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
  public void testWriteFiles() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(new Schema(fields), path);

    FileUtils.deleteDirectory(new File(path));
  }

  // [CARBONDATA-3688]: compressor name is added in data file name
  @Test
  public void testFileName() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(new Schema(fields), path);

    File[] dataFiles = new File(path).listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith(
            CarbonCommonConstants.DEFAULT_COMPRESSOR + CarbonCommonConstants.FACT_FILE_EXT);
      }
    });

    Assert.assertTrue(dataFiles.length > 0);

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testWriteFilesJsonSchema() throws IOException {
    String path = "./testWriteFilesJsonSchema";
    FileUtils.deleteDirectory(new File(path));

    String schema = new StringBuilder()
        .append("[ \n")
        .append("   {\"name\":\"string\"},\n")
        .append("   {\"age\":\"int\"},\n")
        .append("   {\"height\":\"double\"}\n")
        .append("]")
        .toString();

    TestUtil.writeFilesAndVerify(Schema.parseJson(schema), path);

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testWriteJsonSchemaWithDefaultDecimal() {
    String jsonSchema = new StringBuilder()
        .append("[ \n")
        .append("   {\"name\":\"string\"},\n")
        .append("   {\"age\":\"int\"},\n")
        .append("   {\"height\":\"double\"},\n")
        .append("   {\"decimalField\":\"decimal\"}\n")
        .append("]")
        .toString();
    Schema schema = Schema.parseJson(jsonSchema);
    assert (10 == ((DecimalType) schema.getFields()[3].getDataType()).getPrecision());
    assert (2 == ((DecimalType) schema.getFields()[3].getDataType()).getScale());
  }

  @Test
  public void testWriteJsonSchemaWithCustomDecimal() {
    String jsonSchema = new StringBuilder()
        .append("[ \n")
        .append("   {\"name\":\"string\"},\n")
        .append("   {\"age\":\"int\"},\n")
        .append("   {\"height\":\"double\"},\n")
        .append("   {\"decimalField\":\"decimal(17,3)\"}\n")
        .append("]")
        .toString();
    Schema schema = Schema.parseJson(jsonSchema);
    assert (17 == ((DecimalType) schema.getFields()[3].getDataType()).getPrecision());
    assert (3 == ((DecimalType) schema.getFields()[3].getDataType()).getScale());
  }

  @Test
  public void testWriteJsonSchemaWithCustomDecimalAndSpace() {
    String jsonSchema = new StringBuilder()
        .append("[ \n")
        .append("   {\"name\":\"string\"},\n")
        .append("   {\"age\":\"int\"},\n")
        .append("   {\"height\":\"double\"},\n")
        .append("   {\"decimalField\":\"decimal( 17, 3)\"}\n")
        .append("]")
        .toString();
    Schema schema = Schema.parseJson(jsonSchema);
    assert (17 == ((DecimalType) schema.getFields()[3].getDataType()).getPrecision());
    assert (3 == ((DecimalType) schema.getFields()[3].getDataType()).getScale());
  }

  @Test
  public void testWriteJsonSchemaWithImproperDecimal() {
    String jsonSchema = new StringBuilder()
        .append("[ \n")
        .append("   {\"name\":\"string\"},\n")
        .append("   {\"age\":\"int\"},\n")
        .append("   {\"height\":\"double\"},\n")
        .append("   {\"decimalField\":\"decimal( 17, )\"}\n")
        .append("]")
        .toString();
    try {
      Schema.parseJson(jsonSchema);
      assert (false);
    } catch (Exception e) {
      assert (e.getMessage().contains("unsupported data type: decimal( 17, ). " +
          "Please use decimal or decimal(precision,scale), " +
          "precision can be 10 and scale can be 2"));
    }
  }

  @Test
  public void testWriteFilesBuildWithJsonSchema() throws IOException, InvalidLoadOptionException, InterruptedException {
    String path = "./testWriteFilesJsonSchema";
    FileUtils.deleteDirectory(new File(path));

    String schema = "[{name:string},{age:int},{height:double}]";
    CarbonWriterBuilder builder = CarbonWriter
        .builder()
        .outputPath(path)
        .withCsvInput(schema)
        .writtenBy("testWriteFilesBuildWithJsonSchema");

    CarbonWriter writer = builder.build();
    for (int i = 0; i < 10; i++) {
      writer.write(new String[]{
          "robot" + (i % 10), String.valueOf(i % 3000000), String.valueOf((double) i / 2)});
    }
    writer.close();

    CarbonReader carbonReader = CarbonReader.builder(path).build();
    int i = 0;
    while (carbonReader.hasNext()) {
      Object[] row = (Object[]) carbonReader.readNextRow();
      Assert.assertEquals(row[0], "robot" + i % 10);
      System.out.println();
      i++;
    }
    carbonReader.close();
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testAllPrimitiveDataType() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[9];
    fields[0] = new Field("stringField", DataTypes.STRING);
    fields[1] = new Field("intField", DataTypes.INT);
    fields[2] = new Field("shortField", DataTypes.SHORT);
    fields[3] = new Field("longField", DataTypes.LONG);
    fields[4] = new Field("doubleField", DataTypes.DOUBLE);
    fields[5] = new Field("boolField", DataTypes.BOOLEAN);
    fields[6] = new Field("dateField", DataTypes.DATE);
    fields[7] = new Field("timeField", DataTypes.TIMESTAMP);
    fields[8] = new Field("decimalField", DataTypes.createDecimalType(8, 2));

    try {
      CarbonWriterBuilder builder = CarbonWriter.builder().outputPath(path);
      CarbonWriter writer = builder.withCsvInput(new Schema(fields)).writtenBy("CSVCarbonWriterTest").build();

      for (int i = 0; i < 100; i++) {
        Object[] row = new Object[]{
            "robot" + (i % 10),
            i,
            i,
            (Long.MAX_VALUE - i),
            ((double) i / 2),
            true,
            "2019-03-02",
            "2019-02-12 03:03:34",
            "1.234567"
        };
        writer.write(row);
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
    Assert.assertTrue(dataFiles.length > 0);

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void test2Blocklet() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(1000 * 1000, new Schema(fields), path, null, 1, 100);

    // TODO: implement reader to verify the number of blocklet in the file

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void test2Block() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(1000 * 1000, new Schema(fields), path, null, 2, 2);
    File[] dataFiles = new File(path).listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT);
      }
    });
    Assert.assertNotNull(dataFiles);
    Assert.assertEquals(2, dataFiles.length);

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testSortColumns() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(new Schema(fields), path, new String[]{"name"});

    // TODO: implement reader and verify the data is sorted

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testPartitionOutput() {
    // TODO: test write data with partition
  }

  @Test(expected = IOException.class)
  public void testWhenWriterthrowsError() throws IOException{
    CarbonWriter carbonWriter = null;
    String path = "./testWriteFiles";

    FileUtils.deleteDirectory(new File(path));
    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    try {
      carbonWriter = CarbonWriter.builder().
          outputPath(path).withCsvInput(new Schema(fields)).writtenBy("CSVCarbonWriterTest").build();
    } catch (InvalidLoadOptionException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
    carbonWriter.write("babu,1");
    carbonWriter.close();

  }
  @Test
  public void testWrongSchemaFieldsValidation() throws IOException{
    CarbonWriter carbonWriter = null;
    String path = "./testWriteFiles";

    FileUtils.deleteDirectory(new File(path));
    Field[] fields = new Field[3]; // supply 3 size fields but actual Field array value given is 2
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    try {
      carbonWriter = CarbonWriter.builder().
          outputPath(path).withCsvInput(new Schema(fields)).writtenBy("CSVCarbonWriterTest").build();
    } catch (InvalidLoadOptionException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
    carbonWriter.write(new String[]{"babu","1"});
    carbonWriter.close();

  }

  @Test
  public void testTaskNo() throws IOException {
    // TODO: write all data type and read by CarbonRecordReader to verify the content
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("stringField", DataTypes.STRING);
    fields[1] = new Field("intField", DataTypes.INT);


    try {
      CarbonWriterBuilder builder = CarbonWriter.builder()
          .taskNo(5)
          .outputPath(path);

      CarbonWriter writer = builder.withCsvInput(new Schema(fields)).writtenBy("CSVCarbonWriterTest").build();

      for (int i = 0; i < 2; i++) {
        String[] row = new String[]{
            "robot" + (i % 10),
            String.valueOf(i)
        };
        writer.write(row);
      }
      writer.close();

      File[] dataFiles = new File(path).listFiles(new FileFilter() {
        @Override
        public boolean accept(File pathname) {
          return pathname.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT);
        }
      });
      Assert.assertNotNull(dataFiles);
      Assert.assertTrue(dataFiles.length > 0);
      String taskNo = CarbonTablePath.DataFileUtil.getTaskNo(dataFiles[0].getName());
      String taskID = CarbonTablePath.DataFileUtil.getTaskIdFromTaskNo(taskNo);
      Assert.assertEquals("Task Id is not matched", taskID, "5");
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    } finally {
      FileUtils.deleteDirectory(new File(path));
    }
  }

  // validate number of blocklets in one block
  @Test
  public void testBlocklet() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(1000000, new Schema(fields), path, new String[]{"name"}, 3, 8);

    // read footer and verify number of blocklets
    CarbonFile folder = FileFactory.getCarbonFile(path);
    List<CarbonFile> files = folder.listFiles(true);
    List<CarbonFile> dataFiles = new LinkedList<>();
    for (CarbonFile file : files) {
      if (file.getName().endsWith(CarbonTablePath.CARBON_DATA_EXT)) {
        dataFiles.add(file);
      }
    }
    for (CarbonFile dataFile : dataFiles) {
      FileReader fileReader = FileFactory.getFileHolder(FileFactory.getFileType(dataFile.getPath()));
      ByteBuffer buffer = fileReader.readByteBuffer(FileFactory.getUpdatedFilePath(
          dataFile.getPath()), dataFile.getSize() - 8, 8);
      fileReader.finish();
      CarbonFooterReaderV3 footerReader =
          new CarbonFooterReaderV3(dataFile.getAbsolutePath(), buffer.getLong());
      FileFooter3 footer = footerReader.readFooterVersion3();
      Assert.assertEquals(2, footer.blocklet_index_list.size());
      Assert.assertEquals(2, footer.blocklet_info_list3.size());
    }
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testFloatDataType() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[3];
    fields[0] = new Field("stringField", DataTypes.STRING);
    fields[1] = new Field("floatField", DataTypes.FLOAT);
    fields[2] = new Field("doubleField", DataTypes.DOUBLE);

    try {
      CarbonWriterBuilder builder = CarbonWriter.builder().taskNo(5).outputPath(path);
      CarbonWriter writer = builder.withCsvInput(new Schema(fields)).writtenBy("CSVCarbonWriterTest").build();
      for (int i = 0; i < 15; i++) {
        String[] row = new String[] { "robot" + (i % 10), String.valueOf(i + "." + i),
            String.valueOf(i + "." + i) };
        writer.write(row);
      }
      writer.close();
      TableInfo tableInfo = SchemaReader.inferSchema(AbsoluteTableIdentifier.from(path, "",
          ""), false);
      List<String> dataTypes = new ArrayList<>();
      for(ColumnSchema columnSchema: tableInfo.getFactTable().getListOfColumns()) {
          dataTypes.add(columnSchema.getDataType().toString());
      }
      assert(dataTypes.contains("STRING"));
      assert(dataTypes.contains("DOUBLE"));
      assert(dataTypes.contains("FLOAT"));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      FileUtils.deleteDirectory(new File(path));
    }
  }

  @Test
  public void testByteDataType() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("stringField", DataTypes.STRING);
    fields[1] = new Field("byteField", DataTypes.BYTE);

    try {
      CarbonWriterBuilder builder = CarbonWriter.builder().taskNo(5).outputPath(path);
      CarbonWriter writer = builder.withCsvInput(new Schema(fields)).writtenBy("CSVCarbonWriterTest").build();
      for (int i = 0; i < 15; i++) {
        String[] row = new String[] { "robot" + (i % 10),  "" + i };
        writer.write(row);
      }
      writer.close();
      TableInfo tableInfo = SchemaReader.inferSchema(AbsoluteTableIdentifier.from(path, "",
          ""), false);
      List<String> dataTypes = new ArrayList<>();
      for(ColumnSchema columnSchema: tableInfo.getFactTable().getListOfColumns()) {
        dataTypes.add(columnSchema.getDataType().toString());
      }
      assert(dataTypes.contains("STRING"));
      assert(dataTypes.contains("BYTE"));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      FileUtils.deleteDirectory(new File(path));
    }
  }

  @Test
  public void testReadingOfByteAndFloatWithCarbonReader() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[3];
    fields[0] = new Field("stringField", DataTypes.STRING);
    fields[1] = new Field("byteField", DataTypes.BYTE);
    fields[2] = new Field("floatField", DataTypes.FLOAT);

    try {
      CarbonWriterBuilder builder = CarbonWriter.builder().taskNo(5).outputPath(path);
      CarbonWriter writer = builder.withCsvInput(new Schema(fields)).writtenBy("CSVCarbonWriterTest").build();
      for (int i = 0; i < 15; i++) {
        String[] row = new String[] { "robot" + (i % 10), "" + i, i + "." + i };
        writer.write(row);
      }
      writer.close();
      CarbonReader carbonReader =
          new CarbonReaderBuilder(path, "table1").build();
      int i = 0;
      while(carbonReader.hasNext()) {
        Object[] actualRow = (Object[]) carbonReader.readNextRow();
        String[] expectedRow = new String[] { "robot" + (i % 10), "" + i, i + "." + i };
        for (int j = 0; j < 3; j++) {
          actualRow[j].toString().equalsIgnoreCase(expectedRow[j]);
        }
        assert(actualRow[1] instanceof Byte);
        assert(actualRow[2] instanceof Float);
        i++;
      }
      carbonReader.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      FileUtils.deleteDirectory(new File(path));
    }
  }

  @Test
  public void testWritingAndReadingStructOfFloat() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    StructField[] fields = new StructField[3];
    fields[0] = new StructField("stringField", DataTypes.STRING);
    fields[1] = new StructField("byteField", DataTypes.BYTE);
    fields[2] = new StructField("floatField", DataTypes.FLOAT);

    Field structType = new Field("structField", "struct", Arrays.asList(fields));

    try {
      CarbonWriterBuilder builder = CarbonWriter.builder().taskNo(5).outputPath(path);
      CarbonWriter writer = builder.withCsvInput(new Schema(new Field[] {structType})).writtenBy("CSVCarbonWriterTest").build();
      for (int i = 0; i < 15; i++) {
        String[] row = new String[] { "robot" + (i % 10)+"\001" + i+ "\001" + i + "." + i };
        writer.write(row);
      }
      writer.close();
      //TODO: CarbonReader has a bug which does not allow reading complex. Once it is fixed below validation can be enabled
//      CarbonReader carbonReader =
//          new CarbonReaderBuilder(path, "table121").projection(new String[]{"structfield"}).build(TestUtil.configuration);
//      for (int i = 0; i < 15; i++) {
//        Object[] actualRow = (Object[])(carbonReader.readNextRow());
//        String[] expectedRow = new String[] { "robot" + (i % 10), "" + i, i + "." + i };
//        for (int j = 0; j < 3; j++) {
//          ((Object[])actualRow[0])[j].toString().equalsIgnoreCase(expectedRow[j]);
//        }
//      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      FileUtils.deleteDirectory(new File(path));
    }
  }

  @Test
  public void testWritingAndReadingArrayOfFloatAndByte() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    StructField[] fields = new StructField[1];
    fields[0] = new StructField("floatField", DataTypes.FLOAT);

    Field structType1 = new Field("floatarray", "array", Arrays.asList(fields));
    StructField[] fields2 = new StructField[1];
    fields2[0] = new StructField("byteField", DataTypes.BYTE);
    Field structType2 = new Field("bytearray", "array", Arrays.asList(fields2));

    try {
      CarbonWriterBuilder builder = CarbonWriter.builder().taskNo(5).outputPath(path);
      CarbonWriter writer = builder.withCsvInput(new Schema(new Field[] {structType1, structType2})).writtenBy("CSVCarbonWriterTest").build();
      for (int i = 0; i < 15; i++) {
        String[] row = new String[] { "1.0\0012.0\0013.0", "1\0012\0013" };
        writer.write(row);
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      FileUtils.deleteDirectory(new File(path));
    }
  }

  @Test
  public void testWithTableProperties() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    try {
      CarbonWriter writer = CarbonWriter
          .builder()
          .taskNo(5)
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("CSVCarbonWriterTest")
          .withTableProperty("sort_columns", "name")
          .build();
      writer.write(new String[]{"name3", "21"});
      writer.write(new String[]{"name1", "7"});
      writer.write(new String[]{"name2", "18"});
      writer.close();

      CarbonReader reader = CarbonReader.builder(path, "test").build();
      int i = 0;
      while (reader.hasNext()) {
        i++;
        Object[] row = (Object[]) reader.readNextRow();
        Assert.assertTrue(("name" + i).equalsIgnoreCase(row[0].toString()));
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      FileUtils.deleteDirectory(new File(path));
    }
  }

  @Test
  public void testWritingAndReadingArrayString() throws IOException {
    String path = "./testWriteFilesArrayString";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[4];
    fields[0] = new Field("id", DataTypes.STRING);
    fields[1] = new Field("source", DataTypes.STRING);
    fields[2] = new Field("usage", DataTypes.STRING);

    StructField[] stringFields = new StructField[1];
    stringFields[0] = new StructField("stringField", DataTypes.STRING);

    Field arrayType = new Field("annotations", "array", Arrays.asList(stringFields));
    fields[3] = arrayType;
    try {
      CarbonWriterBuilder builder = CarbonWriter.builder().taskNo(5).outputPath(path);
      CarbonWriter writer = builder.withCsvInput(new Schema(fields)).writtenBy("CSVCarbonWriterTest").build();
      for (int i = 0; i < 15; i++) {
        String[] row = new String[]{
          "robot" + (i % 10),
          String.valueOf(i),
          i + "." + i,
          "sunflowers" + (i % 10) + "\002" + "modelarts/image_classification" + "\002" + "2019-03-30 17:22:31" + "\002" + "{\"@modelarts:start_index\":0,\"@modelarts:end_index\":5}"
            + "\001" +
            "roses" + (i % 10) + "\002" + "modelarts/image_classification" + "\002" + "2019-03-30 17:22:32" + "\002" + "{\"@modelarts:start_index\":0,\"@modelarts:end_index\":5}"};
        writer.write(row);
      }
      writer.close();

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }

    Schema schema = CarbonSchemaReader
      .readSchema(path)
      .asOriginOrder();

    assert (4 == schema.getFieldsLength());
    Field[] fields1 = schema.getFields();
    boolean flag = false;
    for (int i = 0; i < fields1.length; i++) {
      if (DataTypes.isArrayType(fields1[i].getDataType())) {
        ArrayType arrayType1 = (ArrayType) fields1[i].getDataType();
        assert ("annotations.stringField" .equalsIgnoreCase(arrayType1.getElementName()));
        assert (DataTypes.STRING.equals(fields1[i].getChildren().get(0).getDataType()));
        flag = true;
      }
    }
    assert (flag);

    // Read again
    CarbonReader reader = null;
    try {
      reader = CarbonReader
        .builder(path)
        .projection(new String[]{"id", "source", "usage", "annotations"})
        .build();
      int i = 0;
      while (reader.hasNext()) {
        Object[] row = (Object[]) reader.readNextRow();
        assert (4 == row.length);
        assert (((String) row[0]).contains("robot"));
        int value = Integer.valueOf((String) row[1]);
        Float value2 = Float.valueOf((String) row[2]);
        assert (value > -1 || value < 15);
        assert (value2 > -1 || value2 < 15);
        Object[] annotations = (Object[]) row[3];
        for (int j = 0; j < annotations.length; j++) {
          assert (((String) annotations[j]).contains("\u0002modelarts/image_classification\u00022019-03-30 17:22:31\u0002{\"@modelarts:start_index\":0,\"@modelarts:end_index\":5}")
            || ((String) annotations[j]).contains("\u0002modelarts/image_classification\u00022019-03-30 17:22:32\u0002{\"@modelarts:start_index\":0,\"@modelarts:end_index\":5}"));
        }
        i++;
      }
      assert (15 == i);
      reader.close();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      FileUtils.deleteDirectory(new File(path));
    }
  }

  @Test
  public void testWritingAndReadingArrayStruct() throws IOException {
    String path = "./testWriteFilesArrayStruct";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[4];
    fields[0] = new Field("id", DataTypes.STRING);
    fields[1] = new Field("source", DataTypes.STRING);
    fields[2] = new Field("usage", DataTypes.STRING);

    List<StructField> structFieldsList = new ArrayList<>();
    structFieldsList.add(new StructField("name", DataTypes.STRING));
    structFieldsList.add(new StructField("type", DataTypes.STRING));
    structFieldsList.add(new StructField("creation-time", DataTypes.STRING));
    structFieldsList.add(new StructField("property", DataTypes.STRING));
    StructField structTypeByList =
      new StructField("annotation", DataTypes.createStructType(structFieldsList), structFieldsList);

    List<StructField> list = new ArrayList<>();
    list.add(structTypeByList);

    Field arrayType = new Field("annotations", "array", list);
    fields[3] = arrayType;
    try {
      CarbonWriterBuilder builder = CarbonWriter.builder().taskNo(5).outputPath(path);
      CarbonWriter writer = builder.withCsvInput(new Schema(fields)).writtenBy("CSVCarbonWriterTest").build();
      for (int i = 0; i < 15; i++) {
        String[] row = new String[]{
          "robot" + (i % 10),
          String.valueOf(i),
          i + "." + i,
          "sunflowers" + (i % 10) + "\002" + "modelarts/image_classification" + "\002" + "2019-03-30 17:22:31" + "\002" + "{\"@modelarts:start_index\":0,\"@modelarts:end_index\":5}"
            + "\001" +
            "roses" + (i % 10) + "\002" + "modelarts/image_classification" + "\002" + "2019-03-30 17:22:32" + "\002" + "{\"@modelarts:start_index\":0,\"@modelarts:end_index\":5}"};
        writer.write(row);
      }
      writer.close();

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }

    Schema schema = CarbonSchemaReader
      .readSchema(path)
      .asOriginOrder();

    assert (4 == schema.getFieldsLength());
    Field[] fields1 = schema.getFields();
    boolean flag = false;
    for (int i = 0; i < fields1.length; i++) {
      if (DataTypes.isArrayType(fields1[i].getDataType())) {
        ArrayType arrayType1 = (ArrayType) fields1[i].getDataType();
        assert ("annotations.annotation" .equalsIgnoreCase(arrayType1.getElementName()));
        assert (DataTypes.isStructType(fields1[i].getChildren().get(0).getDataType()));
        assert (4 == (((StructType) fields1[i].getChildren().get(0).getDataType()).getFields()).size());
        flag = true;
      }
    }
    assert (flag);

    // Read again
    CarbonReader reader = null;
    try {
      reader = CarbonReader
        .builder(path)
        .projection(new String[]{"id", "source", "usage", "annotations"})
        .build();
      int i = 0;
      while (reader.hasNext()) {
        Object[] row = (Object[]) reader.readNextRow();
        assert (4 == row.length);
        assert (((String) row[0]).contains("robot"));
        int value = Integer.valueOf((String) row[1]);
        Float value2 = Float.valueOf((String) row[2]);
        assert (value > -1 || value < 15);
        assert (value2 > -1 || value2 < 15);
        Object[] annotations = (Object[]) row[3];
        for (int j = 0; j < annotations.length; j++) {
          Object[] annotation = (Object[]) annotations[j];
          assert (((String) annotation[0]).contains("sunflowers")
            || ((String) annotation[0]).contains("roses"));

          assert (((String) annotation[1]).contains("modelarts/image_classification"));
          assert (((String) annotation[2]).contains("2019-03-30 17:22:3"));
          assert (((String) annotation[3]).contains("{\"@modelarts:start_index\":0,\"@modelarts:end_index\":5}"));

          Object[] annotation1 = readObjects(annotations, j);
          assert (((String) annotation1[0]).contains("sunflowers")
            || ((String) annotation1[0]).contains("roses"));

          assert (((String) annotation1[1]).contains("modelarts/image_classification"));
          assert (((String) annotation1[2]).contains("2019-03-30 17:22:3"));
          assert (((String) annotation1[3]).contains("{\"@modelarts:start_index\":0,\"@modelarts:end_index\":5}"));
        }
        i++;
      }
      assert (15 == i);
      reader.close();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      FileUtils.deleteDirectory(new File(path));
    }
  }

  private void loadCsvFile(String filePath, Schema schema, Map<String, String> options)
      throws IOException, InvalidLoadOptionException {
    CarbonWriterBuilder carbonWriterBuilder = new CarbonWriterBuilder().withCsvInput(schema).
        withCsvPath(filePath).outputPath(path).writtenBy("CSVCarbonWriter");
    CarbonWriter carbonWriter = null;
    if (options == null) {
      carbonWriter = carbonWriterBuilder.build();
    } else {
      carbonWriter = carbonWriterBuilder.withLoadOptions(options).build();
    }
    carbonWriter.write();
    carbonWriter.close();
    File[] dataFiles = new File(path).listFiles();
    assert (Objects.requireNonNull(dataFiles).length == 2);
  }

  @Test
  public void testCsvLoadAndCarbonReadWithPrimitiveType() throws IOException, InvalidLoadOptionException, InterruptedException {
    String filePath = "./src/test/resources/file/csv_files/primitive_data.csv";
    Field fields[] = new Field[4];
    fields[0] = new Field("id", "INT");
    fields[1] = new Field("country", "STRING");
    fields[2] = new Field("name", "STRING");
    fields[3] = new Field("salary", "INT");
    Schema schema = new Schema(fields);
    loadCsvFile(filePath, schema, null);
    CarbonReader reader = CarbonReader.builder("./testCsvFileLoad", "_temp")
            .projection(new String[]{"id", "country", "name", "salary"}).build();
    int rowCount = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      rowCount++;
      Assert.assertEquals(row[0], rowCount);
      Assert.assertEquals(row[1], "china");
      Assert.assertEquals(row[2], "aaa" + rowCount);
      Assert.assertEquals(row[3], 14999 + rowCount);
    }
    assert (rowCount == 10);
    reader.close();
  }

  @Test
  public void testCsvLoadAndCarbonReadWithComplexType() throws IOException, InterruptedException, InvalidLoadOptionException {
    String filePath = "../../examples/spark/src/main/resources/data.csv";
    Field fields[] = new Field[11];
    fields[0] = new Field("shortField", "SHORT");
    fields[1] = new Field("intField", "INT");
    fields[2] = new Field("bigintField", "LONG");
    fields[3] = new Field("doubleField", "DOUBLE");
    fields[4] = new Field("stringField", "STRING");
    fields[5] = new Field("timestampfield", "TIMESTAMP");
    fields[6] = new Field("decimalField", "DECIMAL");
    fields[7] = new Field("datefield", "DATE");
    fields[8] = new Field("charField", "VARCHAR");
    fields[9] = new Field("floatField", "FLOAT");

    StructField[] structFields = new StructField[3];
    structFields[0] = new StructField("fooField", DataTypes.STRING);
    structFields[1] = new StructField("barField", DataTypes.STRING);
    structFields[2] = new StructField("worldField", DataTypes.STRING);
    Field structType = new Field("structField", "struct", Arrays.asList(structFields));

    fields[10] = structType;
    Map<String, String> options = new HashMap<>();
    options.put("timestampformat", "yyyy/MM/dd HH:mm:ss");
    options.put("dateformat", "yyyy/MM/dd");
    options.put("complex_delimiter_level_1", "#");
    Schema schema = new Schema(fields);
    loadCsvFile(filePath, schema, options);

    CarbonReader reader = CarbonReader.builder("./testCsvFileLoad", "_temp")
            .projection(new String[]{"structfield"}).build();
    int rowCount = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      Object[] structCol = (Object[]) row[0];
      assert (structCol.length == 3);
      Assert.assertEquals(structCol[0], "'foo'");
      Assert.assertEquals(structCol[1], "'bar'");
      Assert.assertEquals(structCol[2], "'world'");
      rowCount++;
    }
    assert (rowCount == 10);
    reader.close();
  }

  @Test
  public void testMultipleCsvFileLoad() throws IOException, InvalidLoadOptionException, InterruptedException {
    String filePath = "./src/test/resources/file/csv_files";
    Field fields[] = new Field[4];
    fields[0] = new Field("id", "INT");
    fields[1] = new Field("country", "STRING");
    fields[2] = new Field("name", "STRING");
    fields[3] = new Field("salary", "INT");
    Schema schema = new Schema(fields);
    loadCsvFile(filePath, schema, null);

    CarbonReader reader = CarbonReader.builder("./testCsvFileLoad", "_temp")
            .projection(new String[]{"id", "country", "name", "salary"}).build();
    int rowCount = 0;
    List<Object[]> rowDatas = new ArrayList<>();
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      rowDatas.add(row);
    }
    rowDatas.sort((row1, row2) -> {
      if((int)row1[0] == (int)row2[0]){
        return 0;
      }
      return ((int)row1[0] < (int)row2[0]) ? -1 : 1;
    });
    for(Object[] row: rowDatas) {
      rowCount++;
      Assert.assertEquals(row[0], rowCount);
      Assert.assertEquals(row[1], "china");
      Assert.assertEquals(row[2], "aaa" + rowCount);
      Assert.assertEquals(row[3], 14999 + rowCount);
    }
    assert (rowCount == 30);
    reader.close();
  }

  @Test
  public void testSelectedCsvFileLoadInDirectory() throws IOException,
      InvalidLoadOptionException, InterruptedException {
    String filePath = "./src/test/resources/file/csv_files";
    CarbonWriterBuilder carbonWriterBuilder = new CarbonWriterBuilder();
    Field fields[] = new Field[4];
    fields[0] = new Field("id", "INT");
    fields[1] = new Field("country", "STRING");
    fields[2] = new Field("name", "STRING");
    fields[3] = new Field("salary", "INT");
    List<String> fileList = new ArrayList<>();
    fileList.add("primitive_data_2.csv");
    fileList.add("primitive_data_3.csv");
    CarbonWriter carbonWriter = carbonWriterBuilder.withCsvInput(new Schema(fields)).
        withCsvPath(filePath, fileList).outputPath(path).writtenBy("CSVCarbonWriter").build();
    carbonWriter.write();
    carbonWriter.close();
    File[] dataFiles = new File(path).listFiles();
    assert (Objects.requireNonNull(dataFiles).length == 2);

    CarbonReader reader = CarbonReader.builder("./testCsvFileLoad", "_temp")
            .projection(new String[]{"id", "country", "name", "salary"}).build();
    int id = 10;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      id++;
      Assert.assertEquals(row[0], id);
      Assert.assertEquals(row[1], "china");
      Assert.assertEquals(row[2], "aaa" + id);
      Assert.assertEquals(row[3], 14999 + id);
    }
    assert (id == 30);
    reader.close();
  }
}
