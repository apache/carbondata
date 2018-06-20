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

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.DiskBasedDMSchemaStorageProvider;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;

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
        .addProperty(CarbonCommonConstants.CARBON_SYSTEM_FOLDER_LOCATION, path);
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
  public void testAllPrimitiveDataType() throws IOException {
    // TODO: write all data type and read by CarbonRecordReader to verify the content
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
      CarbonWriterBuilder builder = CarbonWriter.builder()
          .isTransactionalTable(true)
          .outputPath(path);

      CarbonWriter writer = builder.buildWriterForCSVInput(new Schema(fields));

      for (int i = 0; i < 100; i++) {
        String[] row = new String[]{
            "robot" + (i % 10),
            String.valueOf(i),
            String.valueOf(i),
            String.valueOf(Long.MAX_VALUE - i),
            String.valueOf((double) i / 2),
            String.valueOf(true),
            "2019-03-02",
            "2019-02-12 03:03:34"
        };
        writer.write(row);
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    File segmentFolder = new File(CarbonTablePath.getSegmentPath(path, "null"));
    Assert.assertTrue(segmentFolder.exists());

    File[] dataFiles = segmentFolder.listFiles(new FileFilter() {
      @Override public boolean accept(File pathname) {
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

    TestUtil.writeFilesAndVerify(1000 * 1000, new Schema(fields), path, null, false, 1, 100, false);

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

    TestUtil.writeFilesAndVerify(1000 * 1000, new Schema(fields), path, null, false, 2, 2, true);

    File segmentFolder = new File(CarbonTablePath.getSegmentPath(path, "null"));
    File[] dataFiles = segmentFolder.listFiles(new FileFilter() {
      @Override public boolean accept(File pathname) {
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

  @Test
  public void testSchemaPersistence() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(100, new Schema(fields), path, true);

    String schemaFile = CarbonTablePath.getSchemaFilePath(path);
    Assert.assertTrue(new File(schemaFile).exists());

    FileUtils.deleteDirectory(new File(path));
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
      carbonWriter = CarbonWriter.builder().isTransactionalTable(false).
          outputPath(path).buildWriterForCSVInput(new Schema(fields));
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
      carbonWriter = CarbonWriter.builder().isTransactionalTable(false).
          outputPath(path).buildWriterForCSVInput(new Schema(fields));
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
          .isTransactionalTable(true).taskNo(5)
          .outputPath(path);

      CarbonWriter writer = builder.buildWriterForCSVInput(new Schema(fields));

      for (int i = 0; i < 2; i++) {
        String[] row = new String[]{
            "robot" + (i % 10),
            String.valueOf(i)
        };
        writer.write(row);
      }
      writer.close();

      File segmentFolder = new File(CarbonTablePath.getSegmentPath(path, "null"));
      Assert.assertTrue(segmentFolder.exists());

      File[] dataFiles = segmentFolder.listFiles(new FileFilter() {
        @Override public boolean accept(File pathname) {
          return pathname.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT);
        }
      });
      Assert.assertNotNull(dataFiles);
      Assert.assertTrue(dataFiles.length > 0);
      String taskNo = CarbonTablePath.DataFileUtil.getTaskNo(dataFiles[0].getName());
      long taskID = CarbonTablePath.DataFileUtil.getTaskIdFromTaskNo(taskNo);
      Assert.assertEquals("Task Id is not matched", taskID, 5);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  finally {
      FileUtils.deleteDirectory(new File(path));
    }
  }

}
