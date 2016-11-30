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

import java.io.File;
import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNoException;

/**
 * This class will test the functionality of writing
 * Thrift objects to disk.
 */
public class ThriftWriterTest {

  private static ThriftWriter thriftWriter = null;
  private static File file = null;

  @BeforeClass public static void setUp() throws IOException {
    file = new File("../core/src/test/resources/testWriterFile.txt");
    thriftWriter = new ThriftWriter("testWriterFile.txt", true);
  }

  @AfterClass public static void tearDown() {
    thriftWriter = null;
    file.delete();
  }

  /**
   * test thriftWriters Functionality if stream and binary out is closed.
   */
  @Test public void testToCheckIfStreamAndBinaryOutIsClosed() {
    boolean result = thriftWriter.isOpen();
    assertFalse(result);
  }

  /**
   * test thriftWriters Functionality if stream and binary out is opened.
   */
  @Test public void testToCheckIfStreamAndBinaryOutIsOpen() throws IOException {
    thriftWriter.open();
    boolean result = thriftWriter.isOpen();
    assertTrue(result);
  }

}
