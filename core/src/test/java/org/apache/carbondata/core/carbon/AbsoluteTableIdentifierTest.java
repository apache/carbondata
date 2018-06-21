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

import java.util.UUID;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class AbsoluteTableIdentifierTest {
  static AbsoluteTableIdentifier absoluteTableIdentifier;
  static AbsoluteTableIdentifier absoluteTableIdentifier1;
  static AbsoluteTableIdentifier absoluteTableIdentifier2;
  static AbsoluteTableIdentifier absoluteTableIdentifier3;
  static AbsoluteTableIdentifier absoluteTableIdentifier4;

  @BeforeClass public static void setup() {
    absoluteTableIdentifier = AbsoluteTableIdentifier.from("storePath/databaseName/tableName",
        new CarbonTableIdentifier("databaseName", "tableName", "tableId"));
    absoluteTableIdentifier1 = AbsoluteTableIdentifier.from("dummy", null);
    absoluteTableIdentifier2 = AbsoluteTableIdentifier.from("dumgfhmy", null);
    absoluteTableIdentifier3 =
        AbsoluteTableIdentifier.from("duhgmmy/dumy/dmy/",
            new CarbonTableIdentifier("dummy", "dumy", "dmy"));
    absoluteTableIdentifier4 = AbsoluteTableIdentifier.from("storePath/databaseName/tableName",
        new CarbonTableIdentifier("databaseName", "tableName", "tableId"));
  }

  @Test public void equalsTestWithSameInstance() {
    Boolean res = absoluteTableIdentifier.equals("wrong data");
    Assert.assertTrue(!res);
  }

  @Test public void equalsTestWithNullObject() {
    Boolean res = absoluteTableIdentifier.equals(null);
    Assert.assertTrue(!res);
  }

  @Test public void equalsTestWithotherObject() {
    Boolean res = absoluteTableIdentifier1.equals(absoluteTableIdentifier);
    Assert.assertTrue(!res);
  }

  @Test public void equalsTestWithSameObj() {
    Boolean res = absoluteTableIdentifier.equals(absoluteTableIdentifier);
    Assert.assertTrue(res);
  }

  @Test public void equalsTestWithNullColumnIdentifier() {
    Boolean res = absoluteTableIdentifier1.equals(absoluteTableIdentifier2);
    Assert.assertTrue(!res);
  }

  @Test public void equalsTestWithEqualColumnIdentifier() {
    Boolean res = absoluteTableIdentifier3.equals(absoluteTableIdentifier4);
    Assert.assertTrue(!res);
  }

  @Test public void equalsTestWithEqualAbsoluteTableIdentifier() {
    Boolean res = absoluteTableIdentifier.equals(absoluteTableIdentifier4);
    Assert.assertTrue(res);
  }

  @Test public void hashCodeTest() {
    int res = absoluteTableIdentifier4.hashCode();
    int expectedResult = 233212069;
    assertEquals(res, expectedResult);
  }

  @Test public void gettablePathTest() {
    String res = absoluteTableIdentifier4.getTablePath();
    Assert.assertTrue(res.equals("storePath/databaseName/tableName"));
  }

  @Test public void fromTablePathTest() {
    AbsoluteTableIdentifier absoluteTableIdentifierTest =
        AbsoluteTableIdentifier.from("storePath/databaseName/tableName", "databaseName", "tableName",
            UUID.randomUUID().toString());
    Assert.assertTrue(absoluteTableIdentifierTest.getTablePath()
        .equals(absoluteTableIdentifier4.getTablePath()));
  }

//  @Test(expected = IllegalArgumentException.class) public void fromTablePathWithExceptionTest() {
//    AbsoluteTableIdentifier absoluteTableIdentifierTest =
//        AbsoluteTableIdentifier.fromTablePath("storePath/databaseName");
//  }
}
