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
 * Test suite for {@link ORCCarbonWriter}
 */
public class ORCCarbonWriterTest {
  String DATA_PATH = "./src/test/resources/file/";
  String outputPath = "./testloadORCFiles";

  @Before
  @After
  public void cleanTestData() {
    try {
      FileUtils.deleteDirectory(new File(outputPath));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void loadOrcFile(String path) throws IOException, InvalidLoadOptionException {
    CarbonWriterBuilder carbonWriterBuilder = new CarbonWriterBuilder();
    CarbonWriter carbonWriter = carbonWriterBuilder.withOrcPath(path)
        .outputPath(outputPath).writtenBy("ORCCarbonWriter")
        .withLoadOption("bad_records_action", "force").build();
    carbonWriter.write();
    carbonWriter.close();
  }

  @Test
  public void testORCFileLoadWithPrimitiveType() throws IOException, InvalidLoadOptionException {
    String filePath = DATA_PATH + "userdata1_orc";
    loadOrcFile(filePath);
  }

  @Test
  public void testORCFileLoadWithoutStructSchema() throws IOException, InvalidLoadOptionException {
    String filePath = DATA_PATH + "testTimestamp.orc";
    loadOrcFile(filePath);
  }

  @Test
  public void testORCFileLoadWithComplexSchema() throws IOException,
      InvalidLoadOptionException, InterruptedException {
    String filePath = DATA_PATH + "orc_files/sample.orc";
    loadOrcFile(filePath);
    CarbonReader reader = CarbonReader.builder(outputPath, "_temp")
        .projection(new String[]{"string1", "list"}).build();
    int count = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      count++;
      if (count == 1) {
        assertFetcedRow(row);
      }
    }
    assert (count == 2);
    reader.close();
  }

  private void assertFetcedRow(Object[] row) {
    Assert.assertEquals(row[0], "hi");
    Object[] list = (Object[]) row[1];
    assert (list.length == 2);
    Object[] first = (Object[]) list[0];
    Assert.assertEquals(first[1], "good");
    Assert.assertEquals(first[0], 3);
    Object[] second = (Object[]) list[1];
    Assert.assertEquals(second[1], "bad");
    Assert.assertEquals(second[0], 4);
  }

  @Test
  public void testMultipleORCFileLoad() throws IOException,
      InvalidLoadOptionException, InterruptedException {
    String filePath = DATA_PATH + "orc_files";
    loadOrcFile(filePath);
    CarbonReader reader = CarbonReader.builder(outputPath, "_temp")
        .projection(new String[]{"string1", "list"}).build();
    int count = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      count++;
      if (count % 2 == 1) {
        assertFetcedRow(row);
      }
    }
    assert (count == 6);
    reader.close();
  }

  @Test
  public void testOrcFileLoadAndCarbonRead() throws IOException,
      InterruptedException, InvalidLoadOptionException {
    String filePath = DATA_PATH + "orc_files";
    CarbonWriterBuilder carbonWriterBuilder = new CarbonWriterBuilder();
    List<String> fileList = new ArrayList<>();
    fileList.add("sample.orc");
    fileList.add("sample_3.orc");
    CarbonWriter carbonWriter = carbonWriterBuilder.withOrcPath(filePath, fileList).
        outputPath(outputPath).writtenBy("ORCCarbonWriter")
        .withLoadOption("bad_records_action", "force").build();
    carbonWriter.write();
    carbonWriter.close();
    File[] dataFiles = new File(outputPath).listFiles();
    assert (Objects.requireNonNull(dataFiles).length == 2);
    CarbonReader reader = CarbonReader.builder(outputPath, "_temp")
        .projection(new String[]{"string1", "list"}).build();
    int count = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      count++;
      if (count % 2 == 1) {
        assertFetcedRow(row);
      }
    }
    assert (count == 4);
    reader.close();
  }
}
