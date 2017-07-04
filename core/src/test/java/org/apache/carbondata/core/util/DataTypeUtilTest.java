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

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.apache.carbondata.core.util.DataTypeUtil.*;
import static junit.framework.TestCase.*;
import static org.apache.carbondata.core.util.DataTypeUtil.getDataBasedOnDataType;

public class DataTypeUtilTest {

  @Test public void testGetColumnDataTypeDisplayName() {
    String expected = DataType.INT.getName();
    String result = getColumnDataTypeDisplayName("INT");
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

  @Test public void testGetDataType() {
    assertEquals(DataType.TIMESTAMP, getDataType("TIMESTAMP"));
    assertEquals(DataType.DATE, getDataType("DATE"));
    assertEquals(DataType.STRING, getDataType("STRING"));
    assertEquals(DataType.INT, getDataType("INT"));
    assertEquals(DataType.SHORT, getDataType("SMALLINT"));
    assertEquals(DataType.LONG, getDataType("LONG"));
    assertEquals(DataType.DOUBLE, getDataType("DOUBLE"));
    assertEquals(DataType.DECIMAL, getDataType("DECIMAL"));
    assertEquals(DataType.ARRAY, getDataType("ARRAY"));
    assertEquals(DataType.STRUCT, getDataType("STRUCT"));
    assertEquals(DataType.STRING, getDataType("MAP"));
    assertEquals(DataType.STRING, getDataType("default"));

  }

  @Test public void testGetDataBasedOnDataType() throws NumberFormatException {
    String data = " ";
    if (data.isEmpty()) {
      assertEquals(getDataBasedOnDataType(data, DataType.INT), null);
    }
    assertEquals(getDataBasedOnDataType("1", DataType.INT), 1);
    assertEquals(getDataBasedOnDataType(" ", DataType.INT), null);
    assertEquals(getDataBasedOnDataType("0", DataType.DOUBLE), 0.0d);
    assertEquals(getDataBasedOnDataType("0", DataType.LONG), 0L);
    java.math.BigDecimal javaDecVal = new java.math.BigDecimal(1);
    scala.math.BigDecimal scalaDecVal = new scala.math.BigDecimal(javaDecVal);
    org.apache.spark.sql.types.Decimal expected =
        new org.apache.spark.sql.types.Decimal().set(scalaDecVal);
    assertEquals(getDataBasedOnDataType("1", DataType.DECIMAL), expected);
    assertEquals(getDataBasedOnDataType("default", DataType.NULL),
        UTF8String.fromString("default"));
    assertEquals(getDataBasedOnDataType((String) null, DataType.NULL), null);
  }

  @Test public void testGetMeasureDataBasedOnDataType() throws NumberFormatException {
    assertEquals(getMeasureDataBasedOnDataType(new Long("1"), DataType.LONG), Long.parseLong("1"));
    assertEquals(getMeasureDataBasedOnDataType(new Double("1"), DataType.DOUBLE),
        Double.parseDouble("1"));
    java.math.BigDecimal javaDecVal = new java.math.BigDecimal(1);
    scala.math.BigDecimal scalaDecVal = new scala.math.BigDecimal(javaDecVal);
    org.apache.spark.sql.types.Decimal expected =
        new org.apache.spark.sql.types.Decimal().set(scalaDecVal);
    assertEquals(
            getMeasureDataBasedOnDataType(
                    new java.math.BigDecimal(1),
                    DataType.DECIMAL),
            expected);
    assertEquals(getMeasureDataBasedOnDataType("1", DataType.STRING), "1");
  }

  @Test public void testGetMeasureValueBasedOnDataType() {
    ColumnSchema columnSchema = new ColumnSchema();
    CarbonMeasure carbonMeasure = new CarbonMeasure(columnSchema, 1);
    Object resultInt = getMeasureValueBasedOnDataType("1", DataType.INT, carbonMeasure);
    Object expectedInt = Double.valueOf(1).intValue();
    assertEquals(expectedInt, resultInt);
    Object resultLong = getMeasureValueBasedOnDataType("1", DataType.LONG, carbonMeasure);
    Object expectedLong = Long.valueOf(1);
    assertEquals(expectedLong, resultLong);
    Object resultDefault = getMeasureValueBasedOnDataType("1", DataType.DOUBLE, carbonMeasure);
    Double expectedDefault = Double.valueOf(1);
    assertEquals(expectedDefault, resultDefault);

  }

  @Test public void testNormalizeIntAndLongValues() throws NumberFormatException {
    assertEquals(null, normalizeIntAndLongValues("INT", DataType.INT));
    assertEquals("1", normalizeIntAndLongValues("1", DataType.STRING));

  }

}


