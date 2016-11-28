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

import java.util.Arrays;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

/**
 * this class will test the functionality of ByteArrayHolder
 */

public class ByteArrayHolderTest {
  private ByteArrayHolder byteArrayHolder;

  @Test public void testEqualCaseForCompareTo() {
    byteArrayHolder = new ByteArrayHolder(new byte[] { 1, 2 }, 1);
    byte[] mdKey = { 1, 2 };
    ByteArrayHolder testByteArrayHolder = new ByteArrayHolder(mdKey, 1);
    assertTrue(byteArrayHolder.compareTo(testByteArrayHolder) == 0);
  }

  @Test public void testLessThanCaseForCompareTo() {
    byteArrayHolder = new ByteArrayHolder(new byte[] { 1, 2 }, 2);
    byte[] mdKey = { 1, 7 };
    ByteArrayHolder testByteArrayHolder = new ByteArrayHolder(mdKey, 1);
    assertTrue(byteArrayHolder.compareTo(testByteArrayHolder) == -5);
  }

  @Test public void testGreaterThanCaseForCompareTo() {
    byteArrayHolder = new ByteArrayHolder(new byte[] { 1, 7 }, 2);
    byte[] mdKey = { 1, 2 };
    ByteArrayHolder testByteArrayHolder = new ByteArrayHolder(mdKey, 1);
    assertTrue(byteArrayHolder.compareTo(testByteArrayHolder) == 5);
  }

  @Test public void testEqualsTrueCase() {
    byteArrayHolder = new ByteArrayHolder(new byte[] { 1, 2 }, 1);
    ByteArrayHolder testByteArrayHolder = new ByteArrayHolder(new byte[] { 1, 2 }, 1);
    assertTrue(byteArrayHolder.equals(testByteArrayHolder) == true);
  }

  @Test public void testEqualsFalseCase() {
    byteArrayHolder = new ByteArrayHolder(new byte[] { 1, 8 }, 1);
    ByteArrayHolder testByteArrayHolder = new ByteArrayHolder(new byte[] { 1, 2 }, 1);
    assertTrue(byteArrayHolder.equals(testByteArrayHolder) == false);
  }

  @Test public void testHashcode() {
    byteArrayHolder = new ByteArrayHolder(new byte[] { 1, 8 }, 1);
    new MockUp<Arrays>() {
      @Mock public int hashCode(byte[] a) {
        return 1;
      }
    };
    assertTrue(byteArrayHolder.hashCode() == 62);
  }

}