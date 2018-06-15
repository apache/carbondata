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

import junit.framework.TestCase;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class JsonReaderTest extends TestCase {

  private String thisFilePath;

  {
    try {
      thisFilePath = new File(this.getClass().getResource("/").getPath()).getCanonicalPath()
          .replaceAll("\\\\", "/");
    } catch (IOException e) {
      System.out.println("Unable to form the filePath");
    }
  }

  // Read  one Json row in single line record
  @Test public void testWriteAndReadFilesSingleJsonRowInSignleLine()
      throws IOException, InterruptedException {
    String path = thisFilePath + "../../../../../integration/spark-common-test/src/test/resources/"
        + "jsonFiles/data/similarSchemaFiles/JsonReaderTest/SingleRowSingleLineJson.json";
    JsonReader reader = JsonReader.builder(path).build();

    int i = 0;
    while (reader.hasNext()) {
      Text text = (Text) reader.readNextRow();
      System.out.println(text.toString());
      i++;
    }
    System.out.println("total rows " + i + "\n");
    Assert.assertEquals(i, 1);
    reader.close();
  }

  // Read  many Json row in single line record
  @Test public void testWriteAndReadFilesManyJsonRowInSingleLine()
      throws IOException, InterruptedException {
    String path = thisFilePath + "../../../../../integration/spark-common-test/src/test/resources/"
        + "jsonFiles/data/similarSchemaFiles/JsonReaderTest/MultipleRowSingleLineJson.json";
    JsonReader reader = JsonReader.builder(path).build();

    int i = 0;
    while (reader.hasNext()) {
      Text text = (Text) reader.readNextRow();
      System.out.println(text.toString());
      i++;
    }
    System.out.println("total rows " + i + "\n");
    Assert.assertEquals(i, 4);
    reader.close();
  }

  // Read  Single Json row in multiple line record
  @Test public void testWriteAndReadFilesSingleJsonRowWithRecordIdentifier()
      throws IOException, InterruptedException {
    String path = thisFilePath + "../../../../../integration/spark-common-test/src/test/resources/"
        + "jsonFiles/data/similarSchemaFiles/"
        + "JsonReaderTest/withRecordIdentifier/SingleRowMultipleLineJsonWithRecordIdentifier.json";
    JsonReader reader = JsonReader.builder(path, "jsonData").build();

    int i = 0;
    while (reader.hasNext()) {
      Text text = (Text) reader.readNextRow();
      System.out.println(text.toString());
      i++;
    }
    System.out.println("total rows " + i + "\n");
    Assert.assertEquals(i, 1);
    reader.close();
  }

  // Read  Multiple Json row in multiple line record
  @Test public void testWriteAndReadFilesMultipleJsonRowWithRecordIdentifier()
      throws IOException, InterruptedException {
    String path = thisFilePath + "../../../../../integration/spark-common-test/src/test/resources/"
        + "jsonFiles/data/similarSchemaFiles/"
        + "JsonReaderTest/withRecordIdentifier/MultipleRowMultipleLineJsonWithRecordIdentifier.json";
    JsonReader reader = JsonReader.builder(path, "jsonData").build();

    int i = 0;
    while (reader.hasNext()) {
      Text text = (Text) reader.readNextRow();
      System.out.println(text.toString());
      i++;
    }
    System.out.println("total rows " + i + "\n");
    Assert.assertEquals(i, 4);
    reader.close();
  }

  // Read  Multiple Json row in multiple line record
  @Test public void testWriteAndReadFilesSingleJsonRowSingleLineWithRecordIdentifier()
      throws IOException, InterruptedException {
    String path = thisFilePath + "../../../../../integration/spark-common-test/src/test/resources/"
        + "jsonFiles/data/similarSchemaFiles/"
        + "JsonReaderTest/withRecordIdentifier/SingleRowSingleLineJsonWithRecordIdentifier.json";
    JsonReader reader = JsonReader.builder(path, "jsonField").build();

    int i = 0;
    while (reader.hasNext()) {
      Text text = (Text) reader.readNextRow();
      System.out.println(text.toString());
      i++;
    }
    System.out.println("total rows " + i + "\n");
    Assert.assertEquals(i, 1);
    reader.close();
  }

  // Read json files from folder
  @Test public void testWriteAndReadFilesFromFolder() throws IOException, InterruptedException {
    String path = thisFilePath + "../../../../../integration/spark-common-test/src/test/resources/"
        + "jsonFiles/data/similarSchemaFiles/" + "JsonReaderTest/withRecordIdentifier";
    JsonReader reader = JsonReader.builder(path, "jsonData").build();

    int i = 0;
    while (reader.hasNext()) {
      Text text = (Text) reader.readNextRow();
      System.out.println(text.toString());
      i++;
    }
    System.out.println("total rows " + i + "\n");
    reader.close();
  }

  // Test json reader when no json file is present
  @Test public void testJsonReaderNoFile() throws IOException, InterruptedException {
    String path =
        thisFilePath + "../../../../../integration/spark-common-test/src/test/resources/" + "join";
    try {
      JsonReader reader = JsonReader.builder(path, "jsonField").build();
      // should not execute below statement due to exception
      Assert.assertTrue(false);
    } catch (Exception ex) {
    }
  }

}
