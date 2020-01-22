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

package org.apache.carbondata.core.reader;

import java.io.IOException;

import org.apache.carbondata.format.IndexHeader;

import mockit.Mock;
import mockit.MockUp;
import org.apache.thrift.TBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CarbonIndexFileReaderTest {
  private static CarbonIndexFileReader carbonIndexFileReader = null;

  @BeforeClass public static void setUp() throws IOException {
    carbonIndexFileReader = new CarbonIndexFileReader();
    new MockUp<ThriftReader>() {
      @Mock public void open() throws IOException {

      }

    };
    carbonIndexFileReader.openThriftReader("TestFile.Carbon");
  }

  @AfterClass public static void cleanUp() {
    carbonIndexFileReader.closeThriftReader();
  }

  @Test public void testreadIndexHeader() throws IOException {
    new MockUp<ThriftReader>() {
      @Mock public TBase read(ThriftReader.TBaseCreator creator) throws IOException {
        return new IndexHeader();

      }

    };

    assertNotNull(carbonIndexFileReader.readIndexHeader());
  }

  @Test public void testHasNext() throws IOException {
    new MockUp<ThriftReader>() {
      @Mock public boolean hasNext() throws IOException {

        return true;

      }

    };
    assertTrue(carbonIndexFileReader.hasNext());
  }

  @Test public void testReadBlockInfo() throws IOException {
    new MockUp<ThriftReader>() {
      @Mock public TBase read(ThriftReader.TBaseCreator creator) throws IOException {
        return new org.apache.carbondata.format.BlockIndex();

      }

    };
    assertNotNull(carbonIndexFileReader.readBlockIndexInfo());
  }
}
