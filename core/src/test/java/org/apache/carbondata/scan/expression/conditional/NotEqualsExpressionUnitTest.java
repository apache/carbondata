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

public class NotEqualsExpressionUnitTest {

  static NotEqualsExpression notEqualsExpression;

  @Test public void testEvaluateForNotEqualsExpressionWithBothStringISSame()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("left_name", DataType.STRING);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_name", DataType.STRING);
    right.setColIndex(1);
    notEqualsExpression = new NotEqualsExpression(left, right);
    RowImpl value = new RowImpl();
    String[] row = { "string1" };
    String[] row1 = { "string2" };
    Object objectRow[] = { row, row1 };

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public String getString() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return "string1";

        } else {
          return "string2";
        }
      }
    };

    value.setValues(objectRow);
    ExpressionResult result = notEqualsExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForNotEqualsExpressionWithShortDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {

    ColumnExpression left = new ColumnExpression("left_id", DataType.SHORT);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_id", DataType.SHORT);
    right.setColIndex(1);
    notEqualsExpression = new NotEqualsExpression(left, right);
    RowImpl value = new RowImpl();
    Short[] row = { 15 };
    Short[] row1 = { 16 };
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public Short getShort() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return 15;

        } else {
          return 16;

        }

      }
    };

    ExpressionResult result = notEqualsExpression.evaluate(value);
    assertTrue(result.getBoolean());

  }

  @Test public void testEvaluateForNotEqualsExpressionWithIntDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("right_number", DataType.INT);
    right.setColIndex(1);
    ColumnExpression left = new ColumnExpression("left_number", DataType.INT);
    left.setColIndex(0);
    notEqualsExpression = new NotEqualsExpression(left, right);
    RowImpl value = new RowImpl();
    Integer[] row = { 15 };
    Integer[] row1 = { 16 };
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public Integer getInt() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return 15;

        } else {
          return 16;
        }
      }
    };

    ExpressionResult result = notEqualsExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForNotEqualsExpressionWithDoubleDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("right_contact", DataType.DOUBLE);
    right.setColIndex(1);
    ColumnExpression left = new ColumnExpression("left_contact", DataType.DOUBLE);
    left.setColIndex(0);
    notEqualsExpression = new NotEqualsExpression(left, right);
    RowImpl value = new RowImpl();
    Double[] row = { 445.2D };
    Double[] row1 = { 452.08D };
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public Double getDouble() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return 445.2D;

        } else {
          return 452.08D;
        }
      }
    };

    ExpressionResult result = notEqualsExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForNotEqualsExpressionWithLongDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("right_contact", DataType.LONG);
    right.setColIndex(1);
    ColumnExpression left = new ColumnExpression("left_contact", DataType.LONG);
    left.setColIndex(0);
    notEqualsExpression = new NotEqualsExpression(left, right);
    RowImpl value = new RowImpl();
    Long[] row = { 1234567654321L };
    Long[] row1 = { 12345676541L };
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public Long getLong() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return 1234567654321L;
        } else {
          return 12345676541L;
        }
      }
    };

    ExpressionResult result = notEqualsExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForNotEqualsExpressionWithTimestampDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    try {
      ColumnExpression left = new ColumnExpression("left_timestamp", DataType.TIMESTAMP);
      left.setColIndex(0);
      ColumnExpression right = new ColumnExpression("right_timestamp", DataType.TIMESTAMP);
      right.setColIndex(1);

      notEqualsExpression = new NotEqualsExpression(left, right);

      RowImpl value = new RowImpl();

      DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");

      Date date = dateFormat.parse("23/09/2007");
      long time = date.getTime();
      Timestamp[] row = { new Timestamp(time) };

      Date date1 = dateFormat.parse("24/09/2007");
      long time1 = date1.getTime();
      Timestamp[] row1 = { new Timestamp(time1) };

      Object objectRow[] = { row1, row };
      value.setValues(objectRow);

      new MockUp<ExpressionResult>() {
        Boolean returnMockFlag = true;

        @Mock public Long getTime() {
          if (returnMockFlag) {
            returnMockFlag = false;
            return 1190592000L;
          } else {
            return 1190505600L;
          }
        }
      };

      ExpressionResult result = notEqualsExpression.evaluate(value);
      assertTrue(result.getBoolean());
    } catch (ParseException e) {
      System.out.println("Error while parsing " + e.getMessage());
    }
  }

  @Test(expected = FilterUnsupportedException.class) public void testForNotEqualsExpressionWithDefaultCase()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataType.BOOLEAN);
    right.setColIndex(0);
    notEqualsExpression = new NotEqualsExpression(right, right);
    RowImpl value = new RowImpl();
    Boolean[] row = { true };
    Object objectRow[] = { row };
    value.setValues(objectRow);
    notEqualsExpression.evaluate(value);
  }

  @Test public void testEvaluateWithForNotEqualsExpressionDecimalDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataType.DECIMAL);
    right.setColIndex(1);
    ColumnExpression left = new ColumnExpression("contact", DataType.DECIMAL);
    left.setColIndex(0);
    notEqualsExpression = new NotEqualsExpression(left, right);
    RowImpl value = new RowImpl();
    Decimal[] row = new Decimal[] { Decimal.apply(12345.0) };
    Decimal[] row1 = new Decimal[] { Decimal.apply(1235445.0) };
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public BigDecimal getDecimal() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return new BigDecimal(12345.0);
        } else {
          return new BigDecimal(1235445.0);
        }
      }
    };

    ExpressionResult result = notEqualsExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForNotEqualsExpressionWithIsNullReturnTrue()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("id", DataType.SHORT);
    right.setColIndex(0);
    notEqualsExpression = new NotEqualsExpression(right, right);
    RowImpl value = new RowImpl();
    Short[] row = { 150 };
    Object objectRow[] = { row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public boolean isNull() {
        return true;
      }
    };

    new MockUp<ExpressionResult>() {
      @Mock public Short getShort() {
        return 150;
      }
    };

    ExpressionResult result = notEqualsExpression.evaluate(value);
    assertFalse(result.getBoolean());

  }

  @Test public void testEvaluateForNotEqualsExpressionWithLeftAndRightDifferentDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("name", DataType.STRING);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("number", DataType.INT);
    right.setColIndex(1);
    notEqualsExpression = new NotEqualsExpression(left, right);
    RowImpl value = new RowImpl();
    String[] row1 = { "S" };
    Integer[] row = { 14 };
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public Integer getInt() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return 84;
        } else {
          return 14;
        }
      }
    };

    ExpressionResult result = notEqualsExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testForNotEqualsExpressionWithGetString() throws Exception {
    ColumnExpression right = new ColumnExpression("right_name", DataType.STRING);
    ColumnExpression left = new ColumnExpression("left_name", DataType.STRING);
    notEqualsExpression = new NotEqualsExpression(left, right);
    String expected_result = "NotEquals(ColumnExpression(left_name),ColumnExpression(right_name))";
    String result = notEqualsExpression.getString();
    assertEquals(expected_result, result);
  }

  @Test public void testEvaluateForNotEqualsExpressionWithNullWhileCreatingObject()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("id", DataType.SHORT);
    right.setColIndex(0);
    notEqualsExpression = new NotEqualsExpression(right, right, false);
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

    ExpressionResult result = notEqualsExpression.evaluate(value);
    assertEquals(DataType.BOOLEAN, result.getDataType());

  }

  @Test public void testEvaluateForNotEqualsExpressionWithNullISTureWhileCreatingObject()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("id", DataType.SHORT);
    right.setColIndex(0);
    notEqualsExpression = new NotEqualsExpression(right, right, true);
    RowImpl value = new RowImpl();
    Short[] row = { 15 };
    Object objectRow[] = { row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public boolean isNull() {
        return true;
      }
    };

    ExpressionResult result = notEqualsExpression.evaluate(value);
    assertEquals(DataType.BOOLEAN, result.getDataType());

  }
}
