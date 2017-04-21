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

public class NewRowComparatorTest {
  private static NewRowComparator newRowComparator;
  private static boolean[] noDictionaryColMapping;

  @BeforeClass public static void setUp() {
    noDictionaryColMapping = new boolean[] { true };
    newRowComparator = new NewRowComparator(noDictionaryColMapping);

  }

  @Test public void testCompareWithIsNoDictionaryTrueAndSameValue() {
    byte[] byteArray = new byte[] { 1 };
    Object[] rowA = new Object[] { byteArray };
    Object[] rowB = new Object[] { byteArray };
    int actualValue = newRowComparator.compare(rowA, rowB);
    int expectedValue = 0;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testCompareWithIsNoDictionaryTrueAndWithoutSameValue() {
    byte[] byteArrayFirst = new byte[] { 1 };
    byte[] byteArraySecond = new byte[] { 2 };
    Object[] rowA = new Object[] { byteArrayFirst };
    Object[] rowB = new Object[] { byteArraySecond };
    int actualValue = newRowComparator.compare(rowA, rowB);
    int expectedValue = -1;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testCompareWithIsNoDictionaryFalse() {
    boolean[] noDictionaryColMapingWithFalse = new boolean[] { false };
    NewRowComparator newRowComparatorWithFalse =
        new NewRowComparator(noDictionaryColMapingWithFalse);
    int value = 1;
    Object[] rowA = new Object[] { value };
    Object[] rowB = new Object[] { value };
    int actualValue = newRowComparatorWithFalse.compare(rowA, rowB);
    int expectedValue = 0;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testCompareWithIsNoDictionaryFalseAndWithoutSameValue() {
    boolean[] noDictionaryColMappingWithFalse = new boolean[] { false };
    NewRowComparator newRowComparatorWithFalse =
        new NewRowComparator(noDictionaryColMappingWithFalse);
    int firstValue = 1;
    int secondValue = 2;

    Object[] rowA = new Object[] { firstValue };
    Object[] rowB = new Object[] { secondValue };
    int actualValue = newRowComparatorWithFalse.compare(rowA, rowB);
    int expectedValue = -1;
    assertEquals(expectedValue, actualValue);
  }
}
