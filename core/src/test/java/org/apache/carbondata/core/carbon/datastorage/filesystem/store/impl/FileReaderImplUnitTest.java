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
package org.apache.carbondata.core.carbon.datastorage.filesystem.store.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.carbondata.core.datastore.impl.FileReaderImpl;

import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class FileReaderImplUnitTest {

  private static FileReaderImpl fileHolder;
  private static FileReaderImpl fileHolderWithCapacity;
  private static String fileName;
  private static String fileNameWithEmptyContent;
  private static File file;
  private static File fileWithEmptyContent;

  @BeforeClass public static void setup() {
    fileHolder = new FileReaderImpl();
    fileHolderWithCapacity = new FileReaderImpl(50);
    file = new File("Test.carbondata");
    fileWithEmptyContent = new File("TestEXception.carbondata");

    if (!file.exists()) try {
      file.createNewFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (!fileWithEmptyContent.exists()) try {
      fileWithEmptyContent.createNewFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      FileOutputStream of = new FileOutputStream(file, true);
      BufferedWriter br = new BufferedWriter(new OutputStreamWriter(of, "UTF-8"));
      br.write("Hello World");
      br.close();
    } catch (Exception e) {
      e.getMessage();
    }
    fileName = file.getAbsolutePath();
    fileNameWithEmptyContent = fileWithEmptyContent.getAbsolutePath();
  }

  @AfterClass public static void tearDown() throws IOException {
    file.delete();
    fileWithEmptyContent.delete();
    fileHolder.finish();
  }

  @Test public void testReadByteArray() throws IOException  {
    byte[] result = fileHolder.readByteArray(fileName, 1);
    byte[] expected_result = new byte[] { 72 };
    assertThat(result, is(equalTo(expected_result)));
  }

  @Test public void testReadByteArrayWithFilePath() throws IOException  {
    byte[] result = fileHolder.readByteArray(fileName, 2L, 2);
    byte[] expected_result = { 108, 108 };
    assertThat(result, is(equalTo(expected_result)));
  }

  @Test public void testReadLong() throws IOException  {
    long actualResult = fileHolder.readLong(fileName, 1L);
    long expectedResult = 7308335519855243122L;
    assertThat(actualResult, is(equalTo(expectedResult)));
  }

  @Test public void testReadLongForIoException() throws IOException {
    fileHolder.readLong(fileNameWithEmptyContent, 1L);
  }

  @Test public void testReadIntForIoException() throws IOException {
    fileHolder.readInt(fileNameWithEmptyContent, 1L);
  }

  @Test public void testReadInt() throws IOException  {
    int actualResult = fileHolder.readInt(fileName, 1L);
    int expectedResult = 1701604463;
    assertThat(actualResult, is(equalTo(expectedResult)));
  }

  @Test public void testReadIntWithFileName() throws IOException  {
    int actualResult = fileHolder.readInt(fileName);
    int expectedResult = 1701604463;
    assertThat(actualResult, is(equalTo(expectedResult)));
  }

  @Test public void testReadIntWithFileNameForIOException() throws IOException  {
    fileHolder.readInt(fileNameWithEmptyContent);

  }

  @Test public void testDouble() throws IOException  {
    double actualResult = fileHolder.readDouble(fileName, 1L);
    double expectedResult = 7.3083355198552433E18;
    assertThat(actualResult, is(equalTo(expectedResult)));
  }

  @Test public void testDoubleForIoException() throws IOException {
    fileHolder.readDouble(fileNameWithEmptyContent, 1L);

  }

  @Test public void testDoubleForIoExceptionwithUpdateCache() throws Exception {
    new MockUp<FileSystem>() {
      @SuppressWarnings("unused") @Mock public FSDataInputStream open(Path file)
          throws IOException {
        throw new IOException();
      }

    };
    try {
      fileHolder.readDouble(fileName, 1L);
    } catch (Exception e) {
      assertNull(e.getMessage());
    }

  }

}
