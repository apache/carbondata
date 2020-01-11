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

import org.apache.carbondata.core.metadata.CarbonTableIdentifier;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CarbonTableIdentifierTest {

  static CarbonTableIdentifier carbonTableIdentifier;
  static CarbonTableIdentifier carbonTableIdentifier2;

  @BeforeClass public static void setup() {
    carbonTableIdentifier = new CarbonTableIdentifier("DatabseName", "tableName", "tableId");

  }

  @Test public void equalsTestWithSameObject() {
    Boolean res = carbonTableIdentifier.equals(carbonTableIdentifier);
    Assert.assertTrue(res);
  }

  @Test public void equalsTestWithSimilarObject() {
    CarbonTableIdentifier carbonTableIdentifierTest =
        new CarbonTableIdentifier("DatabseName", "tableName", "tableId");
    Boolean res = carbonTableIdentifier.equals(carbonTableIdentifierTest);
    Assert.assertTrue(res);
  }

  @Test public void equalsTestWithNullrObject() {
    Boolean res = carbonTableIdentifier.equals(carbonTableIdentifier2);
    Assert.assertTrue(!res);
  }

  @Test public void equalsTestWithStringrObject() {
    Boolean res = carbonTableIdentifier.equals("different class object");
    Assert.assertTrue(!res);
  }

  @Test public void equalsTestWithoutDatabaseName() {
    CarbonTableIdentifier carbonTableIdentifierTest =
        new CarbonTableIdentifier(null, "tableName", "tableId");
    Boolean res = carbonTableIdentifierTest.equals(carbonTableIdentifier);
    Assert.assertTrue(!res);
  }

  @Test public void equalsTestWithoutTableId() {
    CarbonTableIdentifier carbonTableIdentifierTest =
        new CarbonTableIdentifier("DatabseName", "tableName", null);
    Boolean res = carbonTableIdentifierTest.equals(carbonTableIdentifier);
    Assert.assertTrue(!res);
  }

  @Test public void equalsTestWithDifferentTableId() {
    CarbonTableIdentifier carbonTableIdentifierTest =
        new CarbonTableIdentifier("DatabseName", "tableName", "diffTableId");
    Boolean res = carbonTableIdentifierTest.equals(carbonTableIdentifier);
    Assert.assertTrue(!res);
  }

  @Test public void equalsTestWithNullTableName() {
    CarbonTableIdentifier carbonTableIdentifierTest =
        new CarbonTableIdentifier("DatabseName", null, "tableId");
    Boolean res = carbonTableIdentifierTest.equals(carbonTableIdentifier);
    Assert.assertTrue(!res);
  }

  @Test public void equalsTestWithDifferentTableName() {
    CarbonTableIdentifier carbonTableIdentifierTest =
        new CarbonTableIdentifier("DatabseName", "diffTableName", "tableId");
    Boolean res = carbonTableIdentifierTest.equals(carbonTableIdentifier);
    Assert.assertTrue(!res);
  }

  @Test public void toStringTest() {
    String res = carbonTableIdentifier.toString();
    System.out.printf("sfdsdf " + res);
    Assert.assertTrue(res.equalsIgnoreCase("DatabseName_tableName"));
  }
}
