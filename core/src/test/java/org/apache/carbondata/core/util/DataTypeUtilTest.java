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

package org.apache.carbondata.core.util;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import org.junit.Assert;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.apache.carbondata.core.util.DataTypeUtil.bigDecimalToByte;
import static org.apache.carbondata.core.util.DataTypeUtil.byteToBigDecimal;
import static org.apache.carbondata.core.util.DataTypeUtil.getMeasureValueBasedOnDataType;
import static org.apache.carbondata.core.util.DataTypeUtil.normalizeIntAndLongValues;

public class DataTypeUtilTest {

  @Test public void testGetColumnDataTypeDisplayName() {
    String expected = DataTypes.INT.getName();
    String result = "INT";
    assertEquals(expected, result);

  }

  @Test public void testByteToBigDecimal() {
    byte[] byteArr = { 0, 0 };
    byte[] unscale = new byte[byteArr.length - 1];
    BigInteger bigInteger = new BigInteger(unscale);
    BigDecimal expected = new BigDecimal(bigInteger, 0);
    BigDecimal result = byteToBigDecimal(byteArr);
    assertEquals(expected, result);

  }

  @Test public void testBigDecimalToByte() {
    byte[] result = bigDecimalToByte(BigDecimal.ONE);
    assertTrue(result == result);
  }

  @Test public void testGetMeasureValueBasedOnDataType() {
    ColumnSchema columnSchema = new ColumnSchema();
    CarbonMeasure carbonMeasure = new CarbonMeasure(columnSchema, 1);
    Object resultInt = getMeasureValueBasedOnDataType("1", DataTypes.INT, carbonMeasure.getScale(), carbonMeasure.getPrecision());
    Object expectedInt = Double.valueOf(1).intValue();
    assertEquals(expectedInt, resultInt);
    Object resultLong = getMeasureValueBasedOnDataType("1", DataTypes.LONG, carbonMeasure.getScale(), carbonMeasure.getPrecision());
    Object expectedLong = Long.valueOf(1);
    assertEquals(expectedLong, resultLong);
    Object resultDefault = getMeasureValueBasedOnDataType("1", DataTypes.DOUBLE, carbonMeasure.getScale(), carbonMeasure.getPrecision());
    Double expectedDefault = Double.valueOf(1);
    assertEquals(expectedDefault, resultDefault);

  }

  @Test public void testNormalizeIntAndLongValues() throws NumberFormatException {
    assertEquals(null, normalizeIntAndLongValues("INT", DataTypes.INT));
    assertEquals("1", normalizeIntAndLongValues("1", DataTypes.STRING));

  }

  @Test public void testGetDataBasedOnDataTypeForNoDictionaryColumn() {
    Object result = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(new byte[0],
        DataTypes.INT);
    Assert.assertTrue(result == null);
    result = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(new byte[0],
        DataTypes.SHORT);
    Assert.assertTrue(result == null);
    result = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(new byte[0],
        DataTypes.LONG);
    Assert.assertTrue(result == null);
    result = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(new byte[0],
        DataTypes.TIMESTAMP);
    Assert.assertTrue(result == null);
    result = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(new byte[0],
        DataTypes.STRING);
    Assert.assertTrue(result != null);
  }

}


