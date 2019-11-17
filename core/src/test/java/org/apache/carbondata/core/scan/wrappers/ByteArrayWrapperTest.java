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

package org.apache.carbondata.core.scan.wrappers;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotSame;
import static junit.framework.TestCase.assertTrue;

public class ByteArrayWrapperTest {
  private static ByteArrayWrapper byteArrayWrapper;
  byte[] dictionaryKey = new byte[] { 1 };
  byte[][] complexTypesKeys = { { 1 }, { 1 } };
  byte[][] noDictionaryKeys = new byte[][] { { 1 }, { 1 } };
  byte[] dictionaryKey1 = new byte[] { 2 };
  byte[][] complexTypesKeys1 = { { 2 }, { 1 } };
  byte[][] noDictionaryKeys1 = new byte[][] { { 2 }, { 1 } };

  @BeforeClass

  public static void setUp() {
    byteArrayWrapper = new ByteArrayWrapper();
  }

  @AfterClass public static void tearDown() {
    byteArrayWrapper = null;
  }

  @Test public void testHashCodeValue() {
    byteArrayWrapper.setDictionaryKey(dictionaryKey);
    byteArrayWrapper.setComplexTypesKeys(complexTypesKeys);
    byteArrayWrapper.setNoDictionaryKeys(noDictionaryKeys);

    int result = byteArrayWrapper.hashCode();
    Assert.assertTrue(29583456 == result);
  }

  @Test public void testEqualsWithOtherAsInstanceOfByteArrayWrapper() {
    ByteArrayWrapper other;
    other = null;
    boolean result = byteArrayWrapper.equals(other);
    assertFalse(result);
  }

  @Test public void testEqualsWithNoDictionaryKeysOtherNotEqualNoDictionaryKeys() {
    byte[][] noDictionaryKeysOther = { { 1 } };
    byteArrayWrapper.setNoDictionaryKeys(noDictionaryKeys);
    ByteArrayWrapper other = new ByteArrayWrapper();
    other.setNoDictionaryKeys(noDictionaryKeysOther);
    boolean result = byteArrayWrapper.equals(other);
    assertFalse(result);
  }

  @Test public void testEqualsWithComplexTypesKeysOtherNotEqualComplexTypesKeys() {
    byte[][] complexTypesKeysOther = { { 1 } };
    byteArrayWrapper.setComplexTypesKeys(complexTypesKeys);
    byteArrayWrapper.setNoDictionaryKeys(noDictionaryKeys);
    ByteArrayWrapper other = new ByteArrayWrapper();
    other.setComplexTypesKeys(complexTypesKeysOther);
    other.setNoDictionaryKeys(noDictionaryKeys);
    boolean result = byteArrayWrapper.equals(other);
    assertFalse(result);
  }

  @Test

  public void testEqualsForFirstElementComplexTypesKeysAndOther() {
    ByteArrayWrapper other = new ByteArrayWrapper();
    other.setComplexTypesKeys(complexTypesKeys);
    other.setDictionaryKey(dictionaryKey);
    other.setNoDictionaryKeys(noDictionaryKeys);
    byteArrayWrapper.setComplexTypesKeys(complexTypesKeys);
    byteArrayWrapper.setDictionaryKey(dictionaryKey);
    byteArrayWrapper.setNoDictionaryKeys(noDictionaryKeys);
    boolean result = byteArrayWrapper.equals(other);
    assertTrue(result);
  }

  @Test

  public void testEqualsForFirstElementComplexTypesKeysAndOther1() {
    ByteArrayWrapper other = new ByteArrayWrapper();
    other.setComplexTypesKeys(complexTypesKeys);
    other.setDictionaryKey(dictionaryKey);
    other.setNoDictionaryKeys(noDictionaryKeys);
    byteArrayWrapper.setComplexTypesKeys(complexTypesKeys);
    byteArrayWrapper.setDictionaryKey(dictionaryKey);
    byteArrayWrapper.setNoDictionaryKeys(noDictionaryKeys1);
    boolean result = byteArrayWrapper.equals(other);
    assertFalse(result);
  }

  @Test
  public void testEqualsForFirstElementComplexTypesKeysAndOther2() {
    ByteArrayWrapper other = new ByteArrayWrapper();
    other.setComplexTypesKeys(complexTypesKeys);
    other.setDictionaryKey(dictionaryKey);
    other.setNoDictionaryKeys(noDictionaryKeys);
    byteArrayWrapper.setComplexTypesKeys(complexTypesKeys1);
    byteArrayWrapper.setDictionaryKey(dictionaryKey);
    byteArrayWrapper.setNoDictionaryKeys(noDictionaryKeys);
    boolean result = byteArrayWrapper.equals(other);
    assertFalse(result);
  }

  @Test public void testCompareTo() {
    byteArrayWrapper.setDictionaryKey(dictionaryKey);
    ByteArrayWrapper other = new ByteArrayWrapper();
    other.setDictionaryKey(dictionaryKey);
    other.setNoDictionaryKeys(noDictionaryKeys);
    other.setComplexTypesKeys(complexTypesKeys);
    byteArrayWrapper.setNoDictionaryKeys(noDictionaryKeys);
    byteArrayWrapper.setComplexTypesKeys(complexTypesKeys);
    int actualResult = byteArrayWrapper.compareTo(other);
    int expectedResult = 0;
    assertEquals(expectedResult, actualResult);
  }

  @Test public void testCompareTo1() {
    byteArrayWrapper.setDictionaryKey(dictionaryKey1);
    ByteArrayWrapper other = new ByteArrayWrapper();
    other.setDictionaryKey(dictionaryKey1);
    other.setNoDictionaryKeys(noDictionaryKeys);
    other.setComplexTypesKeys(complexTypesKeys);
    byteArrayWrapper.setNoDictionaryKeys(noDictionaryKeys);
    byteArrayWrapper.setComplexTypesKeys(complexTypesKeys1);
    int actualResult = byteArrayWrapper.compareTo(other);
    int expectedResult = 0;
    assertNotSame(expectedResult, actualResult);
  }

  @Test public void testCompareTo2() {
    byteArrayWrapper.setDictionaryKey(dictionaryKey);
    ByteArrayWrapper other = new ByteArrayWrapper();
    other.setDictionaryKey(dictionaryKey);
    other.setNoDictionaryKeys(noDictionaryKeys);
    other.setComplexTypesKeys(complexTypesKeys);
    byteArrayWrapper.setNoDictionaryKeys(noDictionaryKeys1);
    byteArrayWrapper.setComplexTypesKeys(complexTypesKeys1);
    int actualResult = byteArrayWrapper.compareTo(other);
    int expectedResult = 0;
    assertNotSame(expectedResult, actualResult);
  }
}
