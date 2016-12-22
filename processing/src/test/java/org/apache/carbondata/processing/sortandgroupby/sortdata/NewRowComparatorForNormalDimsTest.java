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

public class NewRowComparatorForNormalDimsTest {
  private static NewRowComparatorForNormalDims newRowComparatorForNormalDims;
  private static int dimensionCount;

  @BeforeClass public static void setUp() {
    dimensionCount = 1;
    newRowComparatorForNormalDims = new NewRowComparatorForNormalDims(dimensionCount);
  }

  @Test public void testCompareWithSameValues() {
    Object[] rowA = new Integer[] { 1 };
    Object[] rowB = new Integer[] { 1 };
    int actualValue = newRowComparatorForNormalDims.compare(rowA, rowB);
    int expectedValue = 0;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testCompareWithDifferentValues() {
    Object[] rowA = new Integer[] { 1 };
    Object[] rowB = new Integer[] { 2 };
    int actualValue = newRowComparatorForNormalDims.compare(rowA, rowB);
    int expectedValue = -1;
    assertEquals(expectedValue, actualValue);
  }
}
