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
package org.apache.carbondata.core.cache.dictionary;

import mockit.Mock;
import mockit.MockUp;

import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.ColumnIdentifier;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.*;

public class DictionaryColumnUniqueIdentifierTest {

  private static DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier1;
  private static DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier2;
  private static DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier3;

  @BeforeClass public static void setUp() throws Exception {
    CarbonTableIdentifier carbonTableIdentifier1 =
        new CarbonTableIdentifier("testDatabase", "testTable", "1");
    CarbonTableIdentifier carbonTableIdentifier2 =
        new CarbonTableIdentifier("testDatabase", "testTable", "2");
    Map<String, String> properties = new HashMap<>();
    ColumnIdentifier columnIdentifier = new ColumnIdentifier("2", properties, DataType.STRING);
    ColumnIdentifier columnIdentifier2 = new ColumnIdentifier("1", properties, DataType.INT);
    dictionaryColumnUniqueIdentifier1 =
        new DictionaryColumnUniqueIdentifier(carbonTableIdentifier1, columnIdentifier,
            DataType.MAP);
    dictionaryColumnUniqueIdentifier2 =
        new DictionaryColumnUniqueIdentifier(carbonTableIdentifier2, columnIdentifier2,
            DataType.MAP);
    dictionaryColumnUniqueIdentifier3 =
        new DictionaryColumnUniqueIdentifier(carbonTableIdentifier2, columnIdentifier,
            DataType.MAP);
  }

  @Test public void testToGetDataType() {
    assertEquals(dictionaryColumnUniqueIdentifier1.getDataType(), DataType.MAP);
  }

  @Test public void testForEqualsWithDifferentObjectsWithDifferentColumnIdentifier() {
    assertTrue(!dictionaryColumnUniqueIdentifier1.equals(dictionaryColumnUniqueIdentifier2));
  }

  @Test public void testForEqualsWithDifferentObjectsWithSameCarbonTableIdentifier() {
    assertTrue(!dictionaryColumnUniqueIdentifier3.equals(dictionaryColumnUniqueIdentifier2));
  }

  @Test public void testForEquals() {
    assertTrue(dictionaryColumnUniqueIdentifier1.equals(dictionaryColumnUniqueIdentifier1));
  }

  @Test public void testForEqualsWithNull() {
    assertNotNull(dictionaryColumnUniqueIdentifier1);
  }

  @Test public void testForEqualsWithDifferentClass() {
    assertTrue(!dictionaryColumnUniqueIdentifier1.equals(""));
  }

  @Test public void testToGetHashCode() {
    new MockUp<CarbonTableIdentifier>() {
      @SuppressWarnings("unused") @Mock public int hashCode() {
        return 1;
      }
    };
    new MockUp<ColumnIdentifier>() {
      @SuppressWarnings("unused") @Mock public int hashCode() {
        return 2;
      }
    };
    assertEquals(dictionaryColumnUniqueIdentifier1.hashCode(), 33);
  }
}