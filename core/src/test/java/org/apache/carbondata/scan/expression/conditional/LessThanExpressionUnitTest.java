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
package org.apache.carbondata.scan.expression.conditional;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.scan.expression.ColumnExpression;
import org.apache.carbondata.scan.expression.ExpressionResult;
import org.apache.carbondata.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.scan.filter.intf.RowImpl;

import mockit.Mock;
import mockit.MockUp;
import org.apache.spark.sql.types.Decimal;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LessThanExpressionUnitTest {
  static LessThanExpression lessThanExpression;

  @Test public void testEvaluateForLessThanExpressionWithStringDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("left_name", DataType.STRING);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_name", DataType.STRING);
    right.setColIndex(1);
    lessThanExpression = new LessThanExpression(left, right);
    RowImpl value = new RowImpl();
    String[] row = { "First String Value" };
    String[] row1 = { "string1" };
    Object objectRow[] = { row, row1 };
    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public String getString() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return "First String Value";

        } else {
          return "string1";

        }

      }
    };
    value.setValues(objectRow);
    ExpressionResult result = lessThanExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForLessThanExpressionWithShortDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("id", DataType.SHORT);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("id", DataType.SHORT);
    left.setColIndex(1);
    lessThanExpression = new LessThanExpression(left, right);
    RowImpl value = new RowImpl();
    Short[] row = { 7052 };
    Short[] row1 = { 7450 };
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public Short getShort() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return 7052;

        } else {
          return 7450;

        }

      }
    };

    ExpressionResult result = lessThanExpression.evaluate(value);
    assertTrue(result.getBoolean());

  }

  @Test public void testEvaluateForLessThanExpressionWithDoubleDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("right_contact", DataType.DOUBLE);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("left_contact", DataType.DOUBLE);
    left.setColIndex(1);
    lessThanExpression = new LessThanExpression(left, right);
    RowImpl value = new RowImpl();
    Double[] row = { 2087D };
    Double[] row1 = { 4454D };
    Object objectRow[] = { row1, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public Double getDouble() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return 2087D;

        } else {
          return 4454D;

        }

      }
    };

    ExpressionResult result = lessThanExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForLessThanExpressionWithIntDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("right_number", DataType.INT);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("left_number", DataType.INT);
    left.setColIndex(1);
    lessThanExpression = new LessThanExpression(left, right);
    RowImpl value = new RowImpl();
    Integer[] row = { 1550 };
    Integer[] row1 = { 1420 };
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public Integer getInt() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return 1420;

        } else {
          return 1550;

        }

      }
    };

    ExpressionResult result = lessThanExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

 @Test public void testEvaluateForLessThanExpressionWithTimestampDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    try {
      ColumnExpression left = new ColumnExpression("timestamp", DataType.TIMESTAMP);
      left.setColIndex(0);
      ColumnExpression right = new ColumnExpression("timestamp", DataType.TIMESTAMP);
      right.setColIndex(1);

      lessThanExpression = new LessThanExpression(left, right);

      RowImpl value = new RowImpl();

      DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");

      Date date = dateFormat.parse("23/09/2007");
      long time = date.getTime();
      Timestamp[] row = { new Timestamp(time) };

      Date date1 = dateFormat.parse("24/09/2007");
      long time1 = date1.getTime();
      Timestamp[] row1 = { new Timestamp(time1) };

      Object objectRow[] = { row, row1 };
      value.setValues(objectRow);

      new MockUp<ExpressionResult>() {
        Boolean returnMockFlag = true;

        @Mock public Long getTime() {
          if (returnMockFlag) {
            returnMockFlag = false;
            return 1190505600L;
          } else {
            return 1190592000L;
          }
        }
      };

      ExpressionResult result = lessThanExpression.evaluate(value);
      assertTrue(result.getBoolean());
    } catch (ParseException e) {
      System.out.println("Error while parsing " + e.getMessage());
    }
  }

 @Test public void testEvaluateForLessThanExpressionWithLongDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataType.LONG);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("contact", DataType.LONG);
    left.setColIndex(1);
    lessThanExpression = new LessThanExpression(left, right);
    RowImpl value = new RowImpl();
    Long[] row = { 14523656L };
    Long[] row1 = { 12456325L };
    Object objectRow[] = { row1, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public Long getLong() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return 12456325L;
        } else {
          return 14523656L;
        }
      }
    };

    ExpressionResult result = lessThanExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

 @Test public void testEvaluateForLessThanExpressionWithDecimalDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataType.DECIMAL);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("contact", DataType.DECIMAL);
    left.setColIndex(1);
    lessThanExpression = new LessThanExpression(left, right);
    RowImpl value = new RowImpl();
    Decimal[] row = new Decimal[] { Decimal.apply(256324.0) };
    Decimal[] row1 = new Decimal[] { Decimal.apply(123451245.0) };
    Object objectRow[] = { row1, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public BigDecimal getDecimal() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return new BigDecimal(256324.0);
        } else {
          return new BigDecimal(123451245.0);
        }
      }
    };

    ExpressionResult result = lessThanExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test(expected = FilterUnsupportedException.class) public void testForLessThanExpressionWithDefaultCase()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataType.BOOLEAN);
    right.setColIndex(0);
    lessThanExpression = new LessThanExpression(right, right);
    RowImpl value = new RowImpl();
    Boolean[] row = { true };
    Object objectRow[] = { row };
    value.setValues(objectRow);
    lessThanExpression.evaluate(value);
  }

  @Test public void testEvaluateForLessThanExpressionWithIsNullReturnTrue()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("id", DataType.SHORT);
    right.setColIndex(0);
    lessThanExpression = new LessThanExpression(right, right);
    RowImpl value = new RowImpl();
    Short[] row = { 15 };
    Object objectRow[] = { row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public boolean isNull() {
        return true;
      }
    };

    new MockUp<ExpressionResult>() {
      @Mock public Short getShort() {
        return 15;
      }
    };

    ExpressionResult result = lessThanExpression.evaluate(value);
    assertFalse(result.getBoolean());

  }

  @Test public void testEvaluateForLessThanExpressionWithLeftAndRightDifferentDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("name", DataType.STRING);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("number", DataType.INT);
    right.setColIndex(1);
    lessThanExpression = new LessThanExpression(left, right);
    RowImpl value = new RowImpl();
    String[] row = { "S" };
    Integer[] row1 = { 1864 };
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public Integer getInt() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return 84;
        } else {
          return 1864;

        }

      }
    };

    ExpressionResult result = lessThanExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

 @Test public void testForLessThanExpressionWithGetString() throws Exception {
    ColumnExpression right = new ColumnExpression("right_name", DataType.STRING);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("left_name", DataType.STRING);
    left.setColIndex(0);
    lessThanExpression = new LessThanExpression(left, right);
    String expected_result = "LessThan(ColumnExpression(left_name),ColumnExpression(right_name))";
    String result = lessThanExpression.getString();
    assertEquals(expected_result, result);
  }
}
