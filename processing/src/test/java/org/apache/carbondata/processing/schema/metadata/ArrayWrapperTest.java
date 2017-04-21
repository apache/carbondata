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
package org.apache.carbondata.processing.schema.metadata;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ArrayWrapperTest {
  private static ArrayWrapper arrayWrapper;
  private static int[] data;

  @BeforeClass public static void setUp() {
    data = new int[] { 1, 2, 3, 4 };
    arrayWrapper = new ArrayWrapper(data);
  }

  @Test public void testEqualsWithArrayWrapperInstance() {
    arrayWrapper.setData(data);
    int[] dataObject = arrayWrapper.getData();
    ArrayWrapper other = new ArrayWrapper(dataObject);
    boolean actualValue = arrayWrapper.equals(other);
    boolean expectedValue = true;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testEqualsWithoutArrayWrapperInstance() {
    Object other = "test";
    boolean actualValue = arrayWrapper.equals(other);
    boolean expectedValue = false;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testHashCode() {
    int actualValue = arrayWrapper.hashCode();
    int expectedValue = 955331;
    assertEquals(expectedValue, actualValue);
  }

  @Test(expected = Exception.class) public void testEqualsWithException() {
    ArrayWrapper object = new ArrayWrapper(null);
    object.equals(null);
  }

}
