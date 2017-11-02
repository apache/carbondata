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
import org.apache.carbondata.core.metadata.datatype.DecimalType;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.apache.carbondata.core.util.DataTypeUtil.bigDecimalToByte;
import static org.apache.carbondata.core.util.DataTypeUtil.byteToBigDecimal;
import static org.apache.carbondata.core.util.DataTypeUtil.getDataBasedOnDataType;
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

  @Test public void testGetDataBasedOnDataType() throws NumberFormatException {
    String data = " ";
    if (data.isEmpty()) {
      assertEquals(getDataBasedOnDataType(data, DataTypes.INT), null);
    }
    assertEquals(getDataBasedOnDataType("1", DataTypes.INT), 1);
    assertEquals(getDataBasedOnDataType(" ", DataTypes.INT), null);
    assertEquals(getDataBasedOnDataType("0", DataTypes.DOUBLE), 0.0d);
    assertEquals(getDataBasedOnDataType("0", DataTypes.LONG), 0L);
    java.math.BigDecimal javaDecVal = new java.math.BigDecimal(1);
    scala.math.BigDecimal scalaDecVal = new scala.math.BigDecimal(javaDecVal);
    assertEquals(getDataBasedOnDataType("1", DataTypes.createDefaultDecimalType()),
        DataTypeUtil.getDataTypeConverter().convertToDecimal(scalaDecVal));
    assertEquals(getDataBasedOnDataType("default", DataTypes.NULL),
        DataTypeUtil.getDataTypeConverter().convertFromStringToUTF8String("default"));
    assertEquals(getDataBasedOnDataType((String) null, DataTypes.NULL), null);
  }

  @Test public void testGetMeasureValueBasedOnDataType() {
    ColumnSchema columnSchema = new ColumnSchema();
    CarbonMeasure carbonMeasure = new CarbonMeasure(columnSchema, 1);
    Object resultInt = getMeasureValueBasedOnDataType("1", DataTypes.INT, carbonMeasure);
    Object expectedInt = Double.valueOf(1).intValue();
    assertEquals(expectedInt, resultInt);
    Object resultLong = getMeasureValueBasedOnDataType("1", DataTypes.LONG, carbonMeasure);
    Object expectedLong = Long.valueOf(1);
    assertEquals(expectedLong, resultLong);
    Object resultDefault = getMeasureValueBasedOnDataType("1", DataTypes.DOUBLE, carbonMeasure);
    Double expectedDefault = Double.valueOf(1);
    assertEquals(expectedDefault, resultDefault);

  }

  @Test public void testNormalizeIntAndLongValues() throws NumberFormatException {
    assertEquals(null, normalizeIntAndLongValues("INT", DataTypes.INT));
    assertEquals("1", normalizeIntAndLongValues("1", DataTypes.STRING));

  }

}


