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
import java.util.ArrayList;
import java.util.List;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StructQueryTypeTest {
  private static StructQueryType structQueryType;
  private static ArrayQueryType arrayQueryType;

  @BeforeClass public static void setUp() {
    String name = "testName";
    String parentName = "testName";
    int blockIndex = 5;
    structQueryType = new StructQueryType(name, parentName, blockIndex);
    arrayQueryType = new ArrayQueryType(name, parentName, blockIndex);
  }

  @Test public void testGetDataBasedOnDataTypeFromSurrogates() {
    ByteBuffer surrogateData = ByteBuffer.allocate(10);
    surrogateData.put(3, (byte) 1);
    structQueryType.addChildren(arrayQueryType);
    List children = new ArrayList();
    children.add(arrayQueryType);
    assertNotNull(structQueryType.getDataBasedOnDataType(surrogateData));
  }

  @Test public void testGetColsCount() {
    structQueryType.setName("testName");
    structQueryType.setParentName("testName");
    structQueryType.addChildren(arrayQueryType);
    new MockUp<ArrayQueryType>() {
      @Mock int getColsCount() {
        return 1;
      }
    };
    int actualValue = structQueryType.getColsCount();
    int expectedValue = 2;
    assertEquals(expectedValue, actualValue);
  }

}
