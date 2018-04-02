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
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.junit.Assert;

class TestUtil {

  static void writeFilesAndVerify(Schema schema, String path) {
    writeFilesAndVerify(schema, path, null);
  }

  static void writeFilesAndVerify(Schema schema, String path, String[] sortColumns) {
    writeFilesAndVerify(100, schema, path, sortColumns, false, -1, -1);
  }

  static void writeFilesAndVerify(Schema schema, String path, boolean persistSchema) {
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
  static void writeFilesAndVerify(int rows, Schema schema, String path, String[] sortColumns,
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
}
