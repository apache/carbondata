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

package org.apache.carbondata.core.carbon;

import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class ColumnIdentifierTest {

  static ColumnIdentifier columnIdentifier;
  static Map<String, String> columnProperties;

  @BeforeClass public static void setup() {
    columnProperties = new HashMap<String, String>();
    columnProperties.put("key", "value");
    columnIdentifier = new ColumnIdentifier("columnId", columnProperties, DataTypes.INT);
  }

  @Test public void hashCodeTest() {
    int res = columnIdentifier.hashCode();
    int expectedResult = -623419600;
    assertEquals(res, expectedResult);
  }

  @Test public void equalsTestwithSameObject() {
    Boolean res = columnIdentifier.equals(columnIdentifier);
    Assert.assertTrue(res);
  }

  @Test public void equalsTestwithSimilarObject() {
    ColumnIdentifier columnIdentifierTest =
        new ColumnIdentifier("columnId", columnProperties, DataTypes.INT);
    Boolean res = columnIdentifier.equals(columnIdentifierTest);
    Assert.assertTrue(res);
  }

  @Test public void equalsTestwithNullObject() {
    Boolean res = columnIdentifier.equals(null);
    Assert.assertTrue(!res);
  }

  @Test public void equalsTestwithStringObject() {
    Boolean res = columnIdentifier.equals("String Object");
    Assert.assertTrue(!res);
  }

  @Test public void equalsTestwithNullColumnId() {
    ColumnIdentifier columnIdentifierTest =
        new ColumnIdentifier(null, columnProperties, DataTypes.INT);
    Boolean res = columnIdentifierTest.equals(columnIdentifier);
    Assert.assertTrue(!res);
  }

  @Test public void equalsTestwithDiffColumnId() {
    ColumnIdentifier columnIdentifierTest =
        new ColumnIdentifier("diffColumnId", columnProperties, DataTypes.INT);
    Boolean res = columnIdentifierTest.equals(columnIdentifier);
    Assert.assertTrue(!res);
  }

  @Test public void toStringTest() {
    String res = columnIdentifier.toString();
    Assert.assertTrue(res.equals("ColumnIdentifier [columnId=columnId]"));
  }

  @Test public void getColumnPropertyTest() {
    ColumnIdentifier columnIdentifierTest =
        new ColumnIdentifier("diffColumnId", null, DataTypes.INT);
    String res = columnIdentifierTest.getColumnProperty("key");
    assertEquals(res, null);
  }

  @Test public void getColumnPropertyTestwithNull() {
    Assert.assertTrue(columnIdentifier.getColumnProperty("key").equals("value"));
  }
}
