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
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test suite for {@link CSVCarbonWriter}
 */
public class CSVCarbonWriterTest {

  @Test
  public void testWriteFiles() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    writeFilesAndVerify(new Schema(fields), path);

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

    writeFilesAndVerify(Schema.parseJson(schema), path);

    FileUtils.deleteDirectory(new File(path));
  }

  private void writeFilesAndVerify(Schema schema, String path) {
    writeFilesAndVerify(schema, path, null);
  }

  private void writeFilesAndVerify(Schema schema, String path, String[] sortColumns) {
    writeFilesAndVerify(100, schema, path, sortColumns, false, -1, -1);
  }

  private void writeFilesAndVerify(Schema schema, String path, boolean persistSchema) {
    writeFilesAndVerify(100, schema, path, null, persistSchema, -1, -1);
  }

  /**
   * Invoke CarbonWriter API to write carbon files and assert the file is rewritten
   * @param rows number of rows to write
   * @param schema schema of the file
   * @param path local write path
   * @param sortColumns sort columns
   * @param persistSchema true if want to persist schema file
   * @param blockletSize blockletSize in the file, -1 for default size
   * @param blockSize blockSize in the file, -1 for default size
   */
  private void writeFilesAndVerify(int rows, Schema schema, String path, String[] sortColumns,
      boolean persistSchema, int blockletSize, int blockSize) {
    try {
      CarbonWriterBuilder builder = CarbonWriter.builder()
          .withSchema(schema)
          .outputPath(path);
      if (sortColumns != null) {
        builder = builder.sortBy(sortColumns);
      }
      if (persistSchema) {
        builder = builder.persistSchemaFile(true);
      }
      if (blockletSize != -1) {
        builder = builder.withBlockletSize(blockletSize);
      }
      if (blockSize != -1) {
        builder = builder.withBlockSize(blockSize);
      }

      CarbonWriter writer = builder.buildWriterForCSVInput();

      for (int i = 0; i < rows; i++) {
        writer.write(new String[]{"robot" + (i % 10), String.valueOf(i), String.valueOf((double) i / 2)});
      }
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    } catch (InvalidLoadOptionException l) {
      l.printStackTrace();
      Assert.fail(l.getMessage());
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
          .withSchema(new Schema(fields))
          .outputPath(path);

      CarbonWriter writer = builder.buildWriterForCSVInput();

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

    writeFilesAndVerify(1000 * 1000, new Schema(fields), path, null, false, 1, 100);

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

    writeFilesAndVerify(1000 * 1000, new Schema(fields), path, null, false, 2, 2);

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

    writeFilesAndVerify(new Schema(fields), path, new String[]{"name"});

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

    writeFilesAndVerify(new Schema(fields), path, true);

    String schemaFile = CarbonTablePath.getSchemaFilePath(path);
    Assert.assertTrue(new File(schemaFile).exists());

    FileUtils.deleteDirectory(new File(path));
  }

}
