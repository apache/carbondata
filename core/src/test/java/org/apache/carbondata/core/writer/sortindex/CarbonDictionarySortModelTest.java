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
package org.apache.carbondata.core.writer.sortindex;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This class tests the Dictionary sort model class that holds the member byte value and corresponding key value.
 */
public class CarbonDictionarySortModelTest {

  private CarbonDictionarySortModel carbonDictionarySortModel = null;

  @Test public void testCompareToForDataTypeDoubleCase() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataTypes.DOUBLE, "7234");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataTypes.DOUBLE, "5678");
    int expectedResult = 1;
    int actualResult = carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel);
    assertEquals(actualResult, expectedResult);
  }

  @Test public void testCompareToForDataTypeDoubleExceptionCase() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataTypes.DOUBLE, "double");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataTypes.DOUBLE, "@NU#LL$!");
    int expectedResult = -1;
    int actualResult = carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel);
    assertEquals(actualResult, expectedResult);
  }

  @Test public void testCompareToForDataTypeDoubleExceptionCaseForOtherObject() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataTypes.DOUBLE, "1234");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataTypes.DOUBLE, "@NU#LL$!");
    int expectedResult = -1;
    int actualResult = carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel);
    assertEquals(actualResult, expectedResult);
  }

  @Test public void testCompareToForDataTypeBooleanCase() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataTypes.BOOLEAN, "memberValue");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataTypes.DOUBLE, "value");
    int expectedResult = -9;
    int actualResult = carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel);
    assertEquals(actualResult, expectedResult);
  }

  @Test public void testCompareToForDataTypeDecimalCase() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataTypes.createDefaultDecimalType(), "72.34");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataTypes.createDefaultDecimalType(), "56.78");
    int expectedResult = 1;
    int actualResult = carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel);
    assertEquals(actualResult, expectedResult);
  }

  @Test public void testCompareToForDataTypeDecimalExceptionCase() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataTypes.createDefaultDecimalType(), "decimal");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataTypes.createDefaultDecimalType(), "@NU#LL$!");
    int expectedResult = -1;
    int actualResult = carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel);
    assertEquals(actualResult, expectedResult);
  }

  @Test public void testCompareToForDataTypeDecimalExceptionCaseForOtherObject() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataTypes.createDefaultDecimalType(), "15.24");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataTypes.createDefaultDecimalType(), "@NU#LL$!");
    int expectedResult = -1;
    int actualResult = carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel);
    assertEquals(actualResult, expectedResult);
  }

  @Test public void testCompareToForDataTypeTimestampCase() {
    carbonDictionarySortModel =
        new CarbonDictionarySortModel(1, DataTypes.TIMESTAMP, "2014-09-22 12:00:00");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataTypes.TIMESTAMP, "2015-09-22 12:08:49");
    int expectedResult = -1;
    int actualResult = carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel);
    assertEquals(actualResult, expectedResult);
  }

  @Test public void testCompareToForDataTypeTimestampExceptionCase() {
    carbonDictionarySortModel =
        new CarbonDictionarySortModel(1, DataTypes.TIMESTAMP, "2014-09 12:00:00");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataTypes.TIMESTAMP, "@NU#LL$!");
    int expectedResult = -1;
    int actualResult = carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel);
    assertEquals(actualResult, expectedResult);
  }

  @Test public void testCompareToForDataTypeTimestampExceptionCaseForOtherObject() {
    carbonDictionarySortModel =
        new CarbonDictionarySortModel(1, DataTypes.TIMESTAMP, "2014-09-22 12:00:00");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataTypes.TIMESTAMP, "2014-09-22 12");
    int expectedResult = -1;
    int actualResult = carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel);
    assertEquals(actualResult, expectedResult);
  }

  @Test public void testHashCode() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataTypes.createDefaultDecimalType(), "15.24");
    int actualResult = carbonDictionarySortModel.hashCode();
    int expectedResult = 46877260;
    assertTrue(actualResult == expectedResult);
  }

  @Test public void testHashCodeNullCaseForMemberValue() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataTypes.createDefaultDecimalType(), null);
    int actualResult = carbonDictionarySortModel.hashCode();
    int expectedResult = 0;
    assertEquals(actualResult, expectedResult);
  }

  @Test public void testEquals() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataTypes.createDefaultDecimalType(), "15.24");
    CarbonDictionarySortModel testCarbonDictionarySortModel = carbonDictionarySortModel;
    boolean result = carbonDictionarySortModel.equals(testCarbonDictionarySortModel);
    assertTrue(result);
  }

  @Test public void testEqualsMemberValueNullCase() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataTypes.createDefaultDecimalType(), null);
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(1, DataTypes.BOOLEAN, "false");
    boolean result = carbonDictionarySortModel.equals(testCarbonDictionarySortModel);
    assertFalse(result);
  }

  @Test public void testEqualsWhenMemberValueDiffers() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataTypes.createDefaultDecimalType(), "12.45");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(1, DataTypes.BOOLEAN, "false");
    boolean result = carbonDictionarySortModel.equals(testCarbonDictionarySortModel);
    assertFalse(result);
  }

  @Test public void testEqualsWhenMemberValueIsSame() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataTypes.createDefaultDecimalType(), "12.45");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(1, DataTypes.createDefaultDecimalType(), "12.45");
    boolean result = carbonDictionarySortModel.equals(testCarbonDictionarySortModel);
    assertTrue(result);
  }

  @Test public void testEqualsForDifferentObjects() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataTypes.createDefaultDecimalType(), "12.45");
    Object testCarbonDictionarySortModel = new Object();
    boolean result = carbonDictionarySortModel.equals(testCarbonDictionarySortModel);
    assertFalse(result);
  }

  @Test public void testCompareToForDataTypeDoubleExceptionCaseForDifferentObject() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataTypes.DOUBLE, "double");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataTypes.DOUBLE, "1234");
    int expectedResult = 1;
    int actualResult = carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel);
    assertEquals(actualResult, expectedResult);
  }

  @Test public void testCompareToForDataTypeDecimalExceptionCaseForDifferentObject() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataTypes.createDefaultDecimalType(), "12.il");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataTypes.createDefaultDecimalType(), "12.89");
    int expectedResult = 1;
    int actualResult = carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel);
    assertEquals(actualResult, expectedResult);
  }

  @Test public void testCompareToForDataTypeTimestampExceptionCaseForDifferentObject() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataTypes.TIMESTAMP, "2014-09");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataTypes.TIMESTAMP, "2014-09-22 12:00:00");
    int expectedResult = 1;
    int actualResult = carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel);
    assertEquals(actualResult, expectedResult);
  }

}
