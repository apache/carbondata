/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.core.datastorage.store.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class MemoryMappedFileHolderImplUnitTest {

  private static MemoryMappedFileHolderImpl memoryMappedFileHolder = null;
  private static MemoryMappedFileHolderImpl memoryMappedFileHolderWithCapacity = null;
  private static String fileName = null;
  private static String fileWithEmptyContentName = null;
  private static File file = null;
  private static File fileWithEmptyContent = null;

  @BeforeClass public static void setup() {
    memoryMappedFileHolder = new MemoryMappedFileHolderImpl();
    memoryMappedFileHolderWithCapacity = new MemoryMappedFileHolderImpl(50);
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
    fileWithEmptyContentName = fileWithEmptyContent.getAbsolutePath();

  }

  @AfterClass public static void tearDown() {
    file.delete();
    fileWithEmptyContent.delete();
    memoryMappedFileHolder.finish();
  }

  @Test public void testReadByteArray() {
    byte[] result = memoryMappedFileHolder.readByteArray(fileName, 1);
    byte[] expected_result = new byte[] { 72 };
    assertThat(result, is(equalTo(expected_result)));
  }

  @Test public void testReadByteArrayWithFilePath() {

    final byte[] result = memoryMappedFileHolder.readByteArray(fileName, 0L, 6);
    final byte[] expected_result = { 32, 87, 111, 114, 108, 100 };

    assertThat(result, is(equalTo(expected_result)));
  }

  @Test public void testReadLong() {

    long actualResult = memoryMappedFileHolderWithCapacity.readLong(fileName, 0L);
    long expectedResult = 5216694956355245935L;
    assertThat(actualResult, is(equalTo(expectedResult)));
  }

  @Test public void testReadInt() {
    memoryMappedFileHolder = new MemoryMappedFileHolderImpl();

    int actualResult = memoryMappedFileHolder.readInt(fileName, 0L);
    int expectedResult = 1214606444;
    assertThat(actualResult, is(equalTo(expectedResult)));
  }

  @Test public void testReadIntWithFileName() {

    int actualResult = memoryMappedFileHolder.readInt(fileName);
    int expectedResult = 1701604463;
    assertThat(actualResult, is(equalTo(expectedResult)));
  }

  @Test public void testDouble() {
    memoryMappedFileHolder = new MemoryMappedFileHolderImpl();

    double actualResult = memoryMappedFileHolder.readDouble(fileName, 0L);
    double expectedResult = 5.2166949563552461E18;
    assertThat(actualResult, is(equalTo(expectedResult)));
  }

  @Test public void testDoubleForIoExceptionwithUpdateCache() throws Exception {

    String expected = null;
    try {
      memoryMappedFileHolder.readDouble(fileName, 0L);
    } catch (Exception e) {
      assertEquals(e.getMessage(), expected);
    }

  }

}
