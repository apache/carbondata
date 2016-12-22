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

import org.apache.carbondata.processing.util.RemoveDictionaryUtil;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RowComparatorTest {
  private static RowComparator rowComparator;
  private static boolean[] noDictionaryColMaping;
  private static int noDictionaryCount;
  private static int counter;

  @BeforeClass public static void setUp() {
    counter = 0;
    noDictionaryCount = 1;
    noDictionaryColMaping = new boolean[] { true };
    rowComparator = new RowComparator(noDictionaryColMaping, noDictionaryCount);
  }

  @Test public void testCompareWithSameValue() {
    byte[] byteArrayFirst = new byte[] { 0, 0, 0, 4 };
    byte[] byteArraySecond = new byte[] { 0, 0, 0, 4 };
    Object[] rowA = new Object[] { byteArrayFirst, byteArraySecond };
    Object[] rowB = new Object[] { byteArrayFirst, byteArraySecond };
    int actualValue = rowComparator.compare(rowA, rowB);
    int expectedValue = 0;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testCompareWithoutSameValue() {
    byte[] byteArrayFirst = new byte[] { 0, 0, 0, 2 };
    byte[] byteArraySecond = new byte[] { 0, 0, 0, 4 };
    Object[] rowA = new Object[] { byteArrayFirst, byteArraySecond };
    Object[] rowB = new Object[] { byteArraySecond, byteArrayFirst };
    int actualValue = rowComparator.compare(rowA, rowB);
    int expectedValue = -2;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testCompareWithSameValueAndNoDictionaryFalse() {
    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Integer getDimension(int index, Object[] row) {
        return 4;
      }
    };
    RowComparator rowComparatorWithFalse =
        new RowComparator(new boolean[] { false }, noDictionaryCount);
    byte[] byteArrayFirst = new byte[] { 0, 0, 0, 4 };
    byte[] byteArraySecond = new byte[] { 0, 0, 0, 4 };
    Object[] rowA = new Object[] { byteArrayFirst, byteArraySecond };
    Object[] rowB = new Object[] { byteArrayFirst, byteArraySecond };

    int actualValue = rowComparatorWithFalse.compare(rowA, rowB);
    int expectedValue = 0;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testCompareWithoutSameValueAndNoDictionaryFalse() {

    RowComparator rowComparatorWithFalse =
        new RowComparator(new boolean[] { false }, noDictionaryCount);
    byte[] byteArrayFirst = new byte[] { 0, 0, 0, 2 };
    byte[] byteArraySecond = new byte[] { 0, 0, 0, 4 };
    Object[] rowA = new Object[] { byteArrayFirst, byteArraySecond };
    Object[] rowB = new Object[] { byteArrayFirst, byteArrayFirst };
    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Integer getDimension(int index, Object[] rowB) {
        if (counter == 0) {
          counter++;
          return 1;
        } else {
          return 2;
        }
      }
    };
    int actualValue = rowComparatorWithFalse.compare(rowA, rowA);
    int expectedValue = -1;
    assertEquals(expectedValue, actualValue);
  }
}
