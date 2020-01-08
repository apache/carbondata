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
import java.util.LinkedList;
import java.util.List;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalType;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonFooterReaderV3;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.FileFooter3;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test suite for {@link CSVCarbonWriter}
 */
public class CSVCarbonWriterTest {

  @Before
  public void cleanFile() {
    String path = null;
    try {
      path = new File(CSVCarbonWriterTest.class.getResource("/").getPath() + "../")
          .getCanonicalPath().replaceAll("\\\\", "/");
    } catch (IOException e) {
      assert (false);
    }
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_SYSTEM_FOLDER_LOCATION, path)
        .addProperty(CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE,
            String.valueOf(CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE_DEFAULT));
    assert (TestUtil.cleanMdtFile());
  }

  @After
  public void verifyDMFile() {
    assert (!TestUtil.verifyMdtFile());
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

}
