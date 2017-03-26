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
package org.apache.carbondata.core.scan.complextypes;

import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ArrayQueryTypeTest {

  private static ArrayQueryType arrayQueryType;

  @BeforeClass public static void setUp() {
    String name = "test";
    String parentName = "testParent";
    int blockIndex = 1;
    arrayQueryType = new ArrayQueryType(name, parentName, blockIndex);
  }

  @Test public void testGetDataBasedOnDataTypeFromSurrogatesForNull() {
    ByteBuffer surrogateData = ByteBuffer.allocate(10);
    surrogateData.put(0, (byte) 0xFF);
    surrogateData.put(1, (byte) 0xFF);
    surrogateData.put(2, (byte) 0xFF);
    surrogateData.put(3, (byte) 0xFF);
    assertNull(arrayQueryType.getDataBasedOnDataTypeFromSurrogates(surrogateData));
  }

  @Test public void testGetDataBasedOnDataTypeFromSurrogates() {
    ByteBuffer surrogateData = ByteBuffer.allocate(10);
    surrogateData.put(3, (byte) 1);
    arrayQueryType.setName("testName");
    arrayQueryType.setParentname("testName");
    arrayQueryType.addChildren(arrayQueryType);
    assertNotNull(arrayQueryType.getDataBasedOnDataTypeFromSurrogates(surrogateData));
  }
}
