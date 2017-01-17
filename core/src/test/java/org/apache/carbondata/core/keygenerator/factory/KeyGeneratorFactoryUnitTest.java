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

package org.apache.carbondata.core.keygenerator.factory;

import org.apache.carbondata.core.keygenerator.KeyGenerator;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

import static org.apache.carbondata.core.keygenerator.factory.KeyGeneratorFactory.getKeyGenerator;

public class KeyGeneratorFactoryUnitTest {

  @Test public void testGetKeyGenerator() throws Exception {

    int expected = 3;
    int[] dimension = new int[] { 1, 2, 3 };
    KeyGenerator result = getKeyGenerator(dimension);
    assertEquals(expected, result.getDimCount());
  }

  /**
   * Return 0 when we provide empty int[] in method.
   *
   * @throws Exception
   */

  @Test public void testGetKeyGeneratorNegative() throws Exception {

    int expected = 0;
    int[] dimension = new int[] {};
    KeyGenerator result = getKeyGenerator(dimension);
    assertEquals(expected, result.getDimCount());
  }

  @Test public void testGetKeyGenerato() throws Exception {

    int expected = 9;
    int[] dimCardinality = new int[] { 10, 20, 30, 11, 26, 52, 85, 65, 12 };
    int[] columnSplits = new int[] { 2 };
    KeyGenerator result = getKeyGenerator(dimCardinality, columnSplits);
    assertEquals(expected, result.getDimCount());
  }

}
