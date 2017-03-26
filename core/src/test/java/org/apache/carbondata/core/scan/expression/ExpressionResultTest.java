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
package org.apache.carbondata.core.scan.expression;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;

import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class ExpressionResultTest {
  private static final double DELTA = 1e-15;
  private static ExpressionResult expressionResult;

  @BeforeClass public static void setUp() {
    expressionResult = new ExpressionResult(DataType.INT, null);
  }

  @Test public void testGetIntForNull() throws FilterIllegalMemberException {
    assertNull(expressionResult.getInt());
  }

  @Test public void testGetIntForString() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForString = new ExpressionResult(DataType.STRING, "5");
    int actualValue = expressionResultForString.getInt();
    int expectedValue = 5;
    assertEquals(expectedValue, actualValue);
  }

  @Test(expected = FilterIllegalMemberException.class)
  public void testGetIntForNumberFormatException() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForString = new ExpressionResult(DataType.STRING, "FOOBAR");
    expressionResultForString.getInt();
  }

  @Test public void testGetIntForDouble() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForDouble = new ExpressionResult(DataType.DOUBLE, 5.0);
    int actualValue = expressionResultForDouble.getInt();
    int expectedValue = 5;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testGetIntForInt() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForInt = new ExpressionResult(DataType.INT, 5);
    int actualValue = expressionResultForInt.getInt();
    int expectedValue = 5;
    assertEquals(expectedValue, actualValue);
  }

  @Test(expected = FilterIllegalMemberException.class)
  public void testGetIntForIllegalMemberException() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForInt = new ExpressionResult(DataType.FLOAT, 25.36);
    expressionResultForInt.getInt();
  }

  @Test(expected = FilterIllegalMemberException.class) public void testGetIntForClassCastException()
      throws FilterIllegalMemberException {
    ExpressionResult expressionResultForInt = new ExpressionResult(DataType.INT, "STRING");
    expressionResultForInt.getInt();
  }

  @Test public void testGetShortForNull() throws FilterIllegalMemberException {
    assertNull(expressionResult.getShort());
  }

  @Test public void testGetShortForString() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForString = new ExpressionResult(DataType.STRING, "5");
    short actualValue = expressionResultForString.getShort();
    short expectedValue = 5;
    assertEquals(expectedValue, actualValue);
  }

  @Test(expected = FilterIllegalMemberException.class)
  public void testGetShortForNumberFormatException() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForString = new ExpressionResult(DataType.STRING, "FOOBAR");
    expressionResultForString.getShort();
  }

  @Test public void testGetShortForDouble() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForDouble = new ExpressionResult(DataType.DOUBLE, 5.0);
    short actualValue = expressionResultForDouble.getShort();
    short expectedValue = 5;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testGetShortForInt() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForInt = new ExpressionResult(DataType.INT, 5);
    short actualValue = expressionResultForInt.getShort();
    short expectedValue = 5;
    assertEquals(expectedValue, actualValue);
  }

  @Test(expected = FilterIllegalMemberException.class)
  public void testGetShortForIllegalMemberException() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForInt = new ExpressionResult(DataType.FLOAT, 25.36);
    expressionResultForInt.getShort();
  }

  @Test(expected = FilterIllegalMemberException.class)
  public void testGetShortForClassCastException() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForInt = new ExpressionResult(DataType.INT, "STRING");
    expressionResultForInt.getShort();
  }

  @Test public void testGetStringForNull() throws FilterIllegalMemberException {
    assertNull(expressionResult.getString());
  }

  @Test public void testGetStringForString() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForString = new ExpressionResult(DataType.STRING, "5");
    String actualValue = expressionResultForString.getString();
    String expectedValue = "5";
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testGetStringForException() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForString = new ExpressionResult(DataType.INT, "5");
    String actualValue = expressionResultForString.getString();
    String expectedValue = "5";
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testGetDoubleForNull() throws FilterIllegalMemberException {
    assertNull(expressionResult.getDouble());
  }

  @Test public void testGetDoubleForString() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForString = new ExpressionResult(DataType.STRING, "5");
    double actualValue = expressionResultForString.getDouble();
    double expectedValue = 5;
    assertEquals(expectedValue, actualValue, DELTA);
  }

  @Test(expected = FilterIllegalMemberException.class)
  public void testGetDoubleForNumberFormatException() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForString = new ExpressionResult(DataType.STRING, "FOOBAR");
    expressionResultForString.getDouble();
  }

  @Test public void testGetDoubleForInt() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForDouble = new ExpressionResult(DataType.INT, 5);
    double actualValue = expressionResultForDouble.getDouble();
    double expectedValue = 5.0;
    assertEquals(expectedValue, actualValue, DELTA);
  }

  @Test public void testGetDoubleForDouble() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForDouble = new ExpressionResult(DataType.DOUBLE, 5.0);
    double actualValue = expressionResultForDouble.getDouble();
    double expectedValue = 5;
    assertEquals(expectedValue, actualValue, DELTA);
  }

  @Test(expected = FilterIllegalMemberException.class)
  public void testGetDoubleForIllegalMemberException() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForDouble = new ExpressionResult(DataType.FLOAT, 25.36);
    expressionResultForDouble.getDouble();
  }

  @Test(expected = FilterIllegalMemberException.class)
  public void testGetDoubleForClassCastException() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForDouble = new ExpressionResult(DataType.DOUBLE, "STRING");
    expressionResultForDouble.getDouble();
  }

  @Test public void testGetLongForNull() throws FilterIllegalMemberException {
    assertNull(expressionResult.getLong());
  }

  @Test public void testGetLongForString() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForString = new ExpressionResult(DataType.STRING, "5");
    long actualValue = expressionResultForString.getLong();
    long expectedValue = 5;
    assertEquals(expectedValue, actualValue);
  }

  @Test(expected = FilterIllegalMemberException.class)
  public void testGetLongForNumberFormatException() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForString = new ExpressionResult(DataType.STRING, "FOOBAR");
    expressionResultForString.getLong();
  }

  @Test(expected = FilterIllegalMemberException.class)
  public void testGetLongForIllegalMemberException() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForLong = new ExpressionResult(DataType.FLOAT, 25.36);
    expressionResultForLong.getLong();
  }

  @Test(expected = FilterIllegalMemberException.class)
  public void testGetLongForClassCastException() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForLong = new ExpressionResult(DataType.LONG, "STRING");
    expressionResultForLong.getLong();
  }

  @Test public void testGetDecimalForNull() throws FilterIllegalMemberException {
    assertNull(expressionResult.getDecimal());
  }

  @Test public void testGetDecimalForString() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForString = new ExpressionResult(DataType.STRING, "5");
    BigDecimal actualValue = expressionResultForString.getDecimal();
    BigDecimal expectedValue = new BigDecimal(5.00);
    assertEquals(expectedValue, actualValue);
  }

  @Test(expected = FilterIllegalMemberException.class)
  public void testGetDecimalForNumberFormatException() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForString = new ExpressionResult(DataType.STRING, "FOOBAR");
    expressionResultForString.getDecimal();
  }

  @Test public void testGetDecimalForInt() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForInt = new ExpressionResult(DataType.INT, 5);
    BigDecimal actualValue = expressionResultForInt.getDecimal();
    BigDecimal expectedValue = new BigDecimal(5);
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testGetDecimalForDouble() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForDouble = new ExpressionResult(DataType.DOUBLE, 5);
    BigDecimal actualValue = expressionResultForDouble.getDecimal();
    BigDecimal expectedValue = new BigDecimal(5);
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testGetDecimalForDecimal() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForDecimal = new ExpressionResult(DataType.DECIMAL, 5);
    BigDecimal actualValue = expressionResultForDecimal.getDecimal();
    BigDecimal expectedValue = new BigDecimal(5);
    assertEquals(expectedValue, actualValue);
  }

  @Test(expected = FilterIllegalMemberException.class)
  public void testGetDecimalForIllegalMemberException() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForFloat = new ExpressionResult(DataType.FLOAT, 25.36);
    expressionResultForFloat.getDecimal();
  }

  @Test(expected = FilterIllegalMemberException.class)
  public void testGetDecimalForClassCastException() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForException = new ExpressionResult(DataType.LONG, "STRING");
    expressionResultForException.getDecimal();
  }

  @Test public void testGetTimeForNull() throws FilterIllegalMemberException {
    assertNull(expressionResult.getTime());
  }

  @Test public void testGetTimeForString() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForString =
        new ExpressionResult(DataType.STRING, "2016-11-07 10:15:09");
    long actualValue = expressionResultForString.getTime();
    long expectedValue = getTime("2016-11-07 10:15:09");
    assertEquals(expectedValue, actualValue);
  }

  public Long getTime(String value) throws FilterIllegalMemberException {
    if (value == null) {
      return null;
    }
    SimpleDateFormat parser =
        new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
    Date dateToStr;
    try {
      dateToStr = parser.parse(value.toString());
      return dateToStr.getTime();
    } catch (ParseException e) {
      throw new FilterIllegalMemberException("Cannot convert value to Time/Long type value");
    }
  }

  @Test(expected = FilterIllegalMemberException.class) public void testGetTimeForParseException()
      throws FilterIllegalMemberException {
    ExpressionResult expressionResultForParseException =
        new ExpressionResult(DataType.STRING, "FOOBAR");
    expressionResultForParseException.getTime();
  }

  @Test(expected = FilterIllegalMemberException.class)
  public void testGetTimeForIllegalMemberException() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForFloat = new ExpressionResult(DataType.FLOAT, 25.36);
    expressionResultForFloat.getTime();
  }

  @Test(expected = FilterIllegalMemberException.class)
  public void testGetTimeForClassCastException() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForException = new ExpressionResult(DataType.LONG, "STRING");
    expressionResultForException.getTime();
  }

  @Test public void testGetBooleanForNull() throws FilterIllegalMemberException {
    assertNull(expressionResult.getBoolean());
  }

  @Test public void testGetBooleanForString() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForString = new ExpressionResult(DataType.STRING, "true");
    boolean actualValue = expressionResultForString.getBoolean();
    boolean expectedValue = true;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testGetBooleanForBoolean() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForString = new ExpressionResult(DataType.BOOLEAN, "true");
    boolean actualValue = expressionResultForString.getBoolean();
    boolean expectedValue = true;
    assertEquals(expectedValue, actualValue);
  }

  @Test(expected = FilterIllegalMemberException.class)
  public void testGetBooleanForIllegalMemberException() throws FilterIllegalMemberException {
    ExpressionResult expressionResultForFloat = new ExpressionResult(DataType.FLOAT, 25.36);
    expressionResultForFloat.getBoolean();
  }

  @Test public void testGetList() {
    ExpressionResult obj = new ExpressionResult(DataType.STRING, "test");
    List<ExpressionResult> actualValue = obj.getList();
    List<ExpressionResult> expected = new ArrayList<>();
    expected.add(0, obj);
    assertEquals(expected, actualValue);
  }

  @Test public void testHashCode() {
    ExpressionResult obj = new ExpressionResult(DataType.INT, 1);
    int actualValue = obj.hashCode();
    int expectedValue = 80;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testHashCodeForNull() {
    ExpressionResult obj = new ExpressionResult(null);
    int actualValue = obj.hashCode();
    int expectedValue = 31;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testHashCodeForNul() {
    ExpressionResult obj = new ExpressionResult(DataType.DOUBLE, null);
    int actualValue = obj.hashCode();
    int expectedValue = 31;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testEqualsForObjNOtInstanceOfExpressionResult() {
    ExpressionResult expressionResult = new ExpressionResult(DataType.INT, 5);
    List obj = new ArrayList();
    obj.add(0, 0);
    obj.add(1, 1);
    boolean result = expressionResult.equals(obj);
    assertFalse(result);
  }

  @Test public void testEqualsForString() {
    ExpressionResult expressionResult = new ExpressionResult(DataType.STRING, "5");
    ExpressionResult objToCompare = new ExpressionResult(DataType.STRING, "6");
    boolean result = expressionResult.equals(objToCompare);
    assertFalse(result);
  }

  @Test public void testEqualsForShort() {
    ExpressionResult expressionResult = new ExpressionResult(DataType.SHORT, 5);
    ExpressionResult objToCompare = new ExpressionResult(DataType.SHORT, 6);
    boolean result = expressionResult.equals(objToCompare);
    assertFalse(result);
  }

  @Test public void testEqualsForInt() {
    ExpressionResult expressionResult = new ExpressionResult(DataType.INT, 5);
    ExpressionResult objToCompare = new ExpressionResult(DataType.INT, 6);
    boolean result = expressionResult.equals(objToCompare);
    assertFalse(result);
  }

  @Test public void testEqualsForDecimal() {
    ExpressionResult expressionResult = new ExpressionResult(DataType.DECIMAL, 5);
    ExpressionResult objToCompare = new ExpressionResult(DataType.DECIMAL, 6);
    boolean result = expressionResult.equals(objToCompare);
    assertFalse(result);
  }

  @Test public void testEqualsForDouble() {
    ExpressionResult expressionResult = new ExpressionResult(DataType.DOUBLE, 5.89);
    ExpressionResult objToCompare = new ExpressionResult(DataType.DOUBLE, 6.90);
    boolean result = expressionResult.equals(objToCompare);
    assertFalse(result);
  }

  @Test public void testEqualsForException() {
    ExpressionResult expressionResult =
        new ExpressionResult(DataType.TIMESTAMP, "2016-11-07 10:15:09");
    ExpressionResult objToCompare = new ExpressionResult(DataType.DOUBLE, "2016-11-06 10:15:09");
    boolean result = expressionResult.equals(objToCompare);
    assertFalse(result);
  }

  @Test public void testEqualsForLongAndTimeStamp() {
    ExpressionResult expressionResult =
        new ExpressionResult(DataType.TIMESTAMP, new Long(11111111111111111L));
    ExpressionResult objToCompare =
        new ExpressionResult(DataType.LONG, new Long(11111111111111111L));
    boolean result = expressionResult.equals(objToCompare);
    assertTrue(result);
  }

  @Test public void compareToForInt() {
    ExpressionResult obj = new ExpressionResult(DataType.INT, 5);
    ExpressionResult expressionResult = new ExpressionResult(DataType.INT, 6);
    int actualValue = expressionResult.compareTo(obj);
    int expectedValue = 1;
    assertEquals(expectedValue, actualValue);

  }

  @Test public void compareToForDecimal() {
    ExpressionResult obj = new ExpressionResult(DataType.DECIMAL, 5);
    ExpressionResult expressionResult = new ExpressionResult(DataType.DECIMAL, 6);
    int actualValue = expressionResult.compareTo(obj);
    int expectedValue = 1;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void compareToForException() {
    ExpressionResult obj = new ExpressionResult(DataType.INT, 5);
    ExpressionResult expressionResult = new ExpressionResult(DataType.DECIMAL, 6);
    int actualValue = expressionResult.compareTo(obj);
    int expectedValue = -1;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void compareToForTimestamp() {
    ExpressionResult obj = new ExpressionResult(DataType.TIMESTAMP, "2016-11-07 10:15:09");
    ExpressionResult expressionResult =
        new ExpressionResult(DataType.TIMESTAMP, "2016-11-07 10:15:09");
    int actualValue = expressionResult.compareTo(obj);
    int expectedValue = 0;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testGetListAsString() throws FilterIllegalMemberException {
    ExpressionResult obj = new ExpressionResult(DataType.INT, 6);
    List<String> actualValue = obj.getListAsString();
    List<String> expectedValue = new ArrayList<>();
    expectedValue.add("6");
    assertThat(actualValue, is(equalTo(expectedValue)));
  }

  @Test public void compareToForString() {
    ExpressionResult obj = new ExpressionResult(DataType.STRING, "2016");
    ExpressionResult expressionResult = new ExpressionResult(DataType.STRING, "2016");
    int actualValue = expressionResult.compareTo(obj);
    int expectedValue = 0;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testIsNullForNull() {
    ExpressionResult obj = new ExpressionResult(DataType.STRING, null);
    boolean actualValue = obj.isNull();
    assertTrue(actualValue);
  }

  @Test public void testIsNullForNotNull() {
    ExpressionResult obj = new ExpressionResult(DataType.STRING, "test");
    boolean actualValue = obj.isNull();
    assertFalse(actualValue);
  }
}
