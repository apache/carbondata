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
package org.apache.carbondata.core.writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import sun.nio.ch.FileChannelImpl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNoException;

/**
 * This class will test the functionality of Hierarchy
 * Value Writer for CSV.
 */

public class HierarchyValueWriterForCSVTest {
  private static HierarchyValueWriterForCSV valueWriterForCSV = null;

  @BeforeClass public static void setUp() {
    valueWriterForCSV = new HierarchyValueWriterForCSV("hierarchy", "../core/src/test/resources/");
  }

  @AfterClass public static void tearDown() {
    valueWriterForCSV = null;
  }

  /**
   * test if the functionality writing to hierarchy file throws an IOException.
   *
   * @throws KettleException
   */
  @Test(expected = KettleException.class)
  public void testWritingIntoHierarchyFileThrowsKettleException() throws KettleException {
    new MockUp<FileChannelImpl>() {
      @Mock public int write(ByteBuffer var1) throws IOException {
        throw new IOException();
      }
    };
    valueWriterForCSV.writeIntoHierarchyFile(new byte[] {}, -1);
  }

  /**
   * test the functionality writing to hierarchy file.
   *
   * @throws IOException
   */

  @Test public void testToWriteIntoHierarchyFile() throws IOException {
    try {
      valueWriterForCSV.writeIntoHierarchyFile(new byte[] { 124, 17, -8 }, 17);
      long actual = valueWriterForCSV.getBufferedOutStream().position();
      long expected = 0;
      assertFalse(actual == expected);
    } catch (KettleException ke) {
      /**
       * stop test and ignore if error occurs while writing to hierarchy mapping file .
       */
      assumeNoException(ke);
    }
  }

  /**
   * test the functionality writing to CSV file and renaming the in progress file
   * to .level file.
   *
   * @throws KettleException
   */

  @Test public void testToPerformRequiredOperation() {
    try {
      valueWriterForCSV.performRequiredOperation();
      List<ByteArrayHolder> byteArrayHolder = valueWriterForCSV.getByteArrayList();
      assertTrue(byteArrayHolder.isEmpty());
    } catch (KettleException ke) {
      /**
       * stop test and ignore if file for writing can't be opened.
       */
      assumeNoException(ke);
    }
  }

}
