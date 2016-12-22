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

import static org.junit.Assert.assertEquals;

public class RowComparatorForNormalDimsTest {
  private static RowComparatorForNormalDims rowComparatorForNormalDims;
  private static int dimensionCount;

  @BeforeClass public static void setUp() {
    dimensionCount = 1;
    rowComparatorForNormalDims = new RowComparatorForNormalDims(dimensionCount);
  }

  @Test public void testCompare() {
    Integer[] intArray = new Integer[] { 1 };
    Object[] rowA = new Object[] { intArray };
    Object[] rowB = new Object[] { intArray };
    int actualResult = rowComparatorForNormalDims.compare(rowA, rowB);
    int expectedResult = 0;
    assertEquals(expectedResult, actualResult);
  }
}
