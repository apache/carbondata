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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test suite for {@link ParquetCarbonWriter}
 */
public class ParquetCarbonWriterTest {
  String DATA_PATH = "./src/test/resources/file/";
  String outputPath = "./testWriteFiles";

  @Before
  @After
  public void cleanTestData() {
    try {
      FileUtils.deleteDirectory(new File(outputPath));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void loadParquetFile(String filePath) throws IOException, InvalidLoadOptionException {
    CarbonWriterBuilder carbonWriterBuilder = new CarbonWriterBuilder();
    CarbonWriter carbonWriter = carbonWriterBuilder.withParquetPath(filePath).outputPath(outputPath)
        .writtenBy("ParquetCarbonWriterTest").build();
    carbonWriter.write();
    carbonWriter.close();
    File[] dataFiles = new File(outputPath).listFiles();
    assert (Objects.requireNonNull(dataFiles).length == 2);
  }

  @Test
  public void testParquetFileLoadWithNestedMap() throws IOException, InvalidLoadOptionException {
    String filePath = DATA_PATH + "NestedMap.parquet";
    loadParquetFile(filePath);
  }

  @Test
  public void testParquetFileLoadWithMixSchema() throws IOException, InvalidLoadOptionException {
    String filePath = DATA_PATH + "parquet_files/file1.parquet";
   loadParquetFile(filePath);
  }

  @Test
  public void testParquetFileWithInt96() throws IOException, InvalidLoadOptionException {
    String filePath = DATA_PATH + "userdata1.parquet";
    CarbonWriterBuilder carbonWriterBuilder = new CarbonWriterBuilder();
    try {
      carbonWriterBuilder.withParquetPath(filePath).outputPath(outputPath)
          .writtenBy("ParquetFileLoader").build();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("INT96 not implemented and is deprecated", e.getMessage());
    }
  }

  @Test
  public void testParquetFileWithRepeatedSchema() throws IOException, InvalidLoadOptionException {
    String filePath = DATA_PATH + "repeated-schema.parquet";
    CarbonWriterBuilder carbonWriterBuilder = new CarbonWriterBuilder();
    try {
      carbonWriterBuilder.withParquetPath(filePath).outputPath(outputPath)
          .writtenBy("ParquetCarbonWriterTest").build();
    } catch (UnsupportedOperationException e) {
      assert (e.getMessage().contains("REPEATED not supported outside LIST or MAP."));
    }
  }

  private int buildCarbonReaderAndFetchRecord() throws IOException, InterruptedException {
    CarbonReader reader = CarbonReader
        .builder(outputPath, "_temp")
        .projection(new String[]{"file"})
        .build();
    int rowCount = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      Assert.assertEquals(((Object[]) row[0])[0], "file1.parquet");
      rowCount++;
    }
    reader.close();
    return rowCount;
  }

  @Test
  public void testParquetLoadAndCarbonRead() throws Exception {
    String filePath = DATA_PATH + "parquet_files/file1.parquet";
    loadParquetFile(filePath);
    int i = buildCarbonReaderAndFetchRecord();
    Assert.assertEquals(i, 1);
  }

  @Test
  public void testMultipeParquetFileLoad() throws IOException, InvalidLoadOptionException, InterruptedException {
    String filePath = DATA_PATH + "parquet_files";
    loadParquetFile(filePath);
    int numberOfRow = buildCarbonReaderAndFetchRecord();
    Assert.assertEquals(numberOfRow, 3);
  }

  @Test
  public void testSelectedParquetFileLoadInDirectory() throws IOException, InvalidLoadOptionException, InterruptedException {
    String filePath = DATA_PATH + "parquet_files";
    CarbonWriterBuilder carbonWriterBuilder = new CarbonWriterBuilder();
    List<String> fileList = new ArrayList<>();
    fileList.add("file1.parquet");
    fileList.add("file3.parquet");
    CarbonWriter carbonWriter = carbonWriterBuilder.withParquetPath(filePath, fileList)
        .outputPath(outputPath).writtenBy("ParquetCarbonWriterTest").build();
    carbonWriter.write();
    carbonWriter.close();
    File[] dataFiles = new File(outputPath).listFiles();
    assert (Objects.requireNonNull(dataFiles).length == 2);

    int numberOfRow = buildCarbonReaderAndFetchRecord();
    Assert.assertEquals(numberOfRow, 2);
  }
}
