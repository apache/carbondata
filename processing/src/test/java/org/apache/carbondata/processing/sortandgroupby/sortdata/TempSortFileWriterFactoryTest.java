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
package org.apache.carbondata.processing.sortandgroupby.sortdata;

import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;

public class TempSortFileWriterFactoryTest {
  private static TempSortFileWriterFactory tempSortFileWriterFactory;
  private static boolean isCompressionEnabled;
  private static int dimensionCount, complexDimensionCount, measureCount, noDictionaryCount,
      writeBufferSize;

  @BeforeClass public static void setUp() {
    isCompressionEnabled = true;
    dimensionCount = 1;
    complexDimensionCount = 1;
    measureCount = 1;
    noDictionaryCount = 1;
    writeBufferSize = 1;
    tempSortFileWriterFactory = TempSortFileWriterFactory.getInstance();
  }

  @Test public void testGetTempSortFileWriterWithCompressionEnabled() {

    assertNotNull(tempSortFileWriterFactory
        .getTempSortFileWriter(isCompressionEnabled, dimensionCount, complexDimensionCount,
            measureCount, noDictionaryCount, writeBufferSize));
  }

  @Test public void testGetTempSortFileWriterWithCompressionDisabled() {
    assertNotNull(tempSortFileWriterFactory
        .getTempSortFileWriter(false, dimensionCount, complexDimensionCount, measureCount,
            noDictionaryCount, writeBufferSize));
  }
}
