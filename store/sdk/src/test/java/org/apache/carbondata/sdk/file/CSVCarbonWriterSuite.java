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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test suite for {@link CSVCarbonWriter}
 */
public class CSVCarbonWriterSuite {

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
    try {
      CarbonWriter writer = CarbonWriter.builder()
          .withSchema(schema)
          .outputPath(path)
          .buildWriterForCSVInput();

      for (int i = 0; i < 100; i++) {
        writer.write(new String[]{"robot" + i, String.valueOf(i), String.valueOf((double) i / 2)});
      }
      writer.close();
    } catch (IOException e) {
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
    Assert.assertEquals(1, dataFiles.length);
  }

  @Test
  public void testAllPrimitiveDataType() {
    // TODO: write all data type and read by CarbonRecordReader to verify the content
  }

  @Test
  public void test2Blocklet() {
    // TODO: write data with more than one blocklet
  }

  @Test
  public void test2Block() {
    // TODO: write data with more than one block
  }

  @Test
  public void testSortColumns() {
    // TODO: test sort column
  }

  @Test
  public void testPartitionOutput() {
    // TODO: test write data with partition
  }

  @Test
  public void testSchemaPersistence() {
    // TODO: verify schema file is persisted in specified location
  }

}
