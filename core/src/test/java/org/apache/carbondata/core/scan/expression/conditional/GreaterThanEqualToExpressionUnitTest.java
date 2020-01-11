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

package org.apache.carbondata.core.scan.expression.conditional;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.intf.RowImpl;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GreaterThanEqualToExpressionUnitTest {

  static GreaterThanEqualToExpression greaterThanEqualToExpression;

  @Test public void testEvaluateForGreaterThanEqualToExpressionWithBothStringISSame()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("left_name", DataTypes.STRING);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_name", DataTypes.STRING);
    right.setColIndex(1);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    String[] row = { "string1" };
    String[] row1 = { "string1" };
    Object objectRow[] = { row, row1 };

    new MockUp<ExpressionResult>() {
      @Mock public String getString() {
        return "string1";
      }
    };
    value.setValues(objectRow);
    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForGreaterThanEqualToExpressionWithShortDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("id", DataTypes.SHORT);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("id", DataTypes.SHORT);
    left.setColIndex(1);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Short[] row = { 15 };
    Short[] row1 = { 16 };
    Object objectRow[] = { row1, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Short getShort() {
        return 16;
      }
    };

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());

  }

  @Test public void testEvaluateForGreaterThanEqualToExpressionWithShortDataType1()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("id", DataTypes.SHORT);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("id", DataTypes.SHORT);
    left.setColIndex(1);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Short[] row = { 15 };
    Short[] row1 = { 16 };
    Object objectRow[] = { row1, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      boolean isFirst = true;
      @Mock public Short getShort() {
        if (isFirst) {
          isFirst = false;
          return 15;
        }
        return 16;
      }
    };

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertFalse(result.getBoolean());

  }

  @Test public void testEvaluateForGreaterThanEqualToExpressionWithIntDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("right_number", DataTypes.INT);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("left_number", DataTypes.INT);
    left.setColIndex(1);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Integer[] row = { 140 };
    Integer[] row1 = { 145 };
    Object objectRow[] = { row1, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Integer getInt() {
        return 145;
      }
    };

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForGreaterThanEqualToExpressionWithIntDataType1()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("right_number", DataTypes.INT);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("left_number", DataTypes.INT);
    left.setColIndex(1);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Integer[] row = { 140 };
    Integer[] row1 = { 145 };
    Object objectRow[] = { row1, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      boolean isFirst = true;
      @Mock public Integer getInt() {
        if (isFirst) {
          isFirst = false;
          return 140;
        }
        return 145;
      }
    };

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertFalse(result.getBoolean());
  }

  @Test public void testEvaluateForGreaterThanEqualToExpressionWithDoubleDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("right_contact", DataTypes.DOUBLE);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("left_contact", DataTypes.DOUBLE);
    left.setColIndex(1);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Double[] row = { 44D };
    Double[] row1 = { 45D };
    Object objectRow[] = { row1, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Double getDouble() {
        return 45D;
      }
    };

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForGreaterThanEqualToExpressionWithDoubleDataType1()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("right_contact", DataTypes.DOUBLE);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("left_contact", DataTypes.DOUBLE);
    left.setColIndex(1);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Double[] row = { 44D };
    Double[] row1 = { 45D };
    Object objectRow[] = { row1, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      boolean isFirst = true;
      @Mock public Double getDouble() {
        if (isFirst) {
          isFirst = false;
          return 44D;
        }
        return 45D;
      }
    };

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertFalse(result.getBoolean());
  }

  @Test public void testEvaluateForGreaterThanEqualToExpressionWithLongDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataTypes.LONG);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("contact", DataTypes.LONG);
    left.setColIndex(1);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Long[] row = { 1234567654321L };
    Long[] row1 = { 1234567654321L };
    Object objectRow[] = { row1, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Long getLong() {
        return 1234567654321L;
      }
    };

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForGreaterThanEqualToExpressionWithLongDataType1()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataTypes.LONG);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("contact", DataTypes.LONG);
    left.setColIndex(1);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Long[] row = { 1234567654321L };
    Long[] row1 = { 1234567654321L };
    Object objectRow[] = { row1, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      boolean isFirst = true;
      @Mock public Long getLong() {
        if (isFirst) {
          isFirst = false;
          return 1234567654322L;
        }
        return 1234567654321L;
      }
    };

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForGreaterThanEqualToExpressionWithTimestampDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    try {
      ColumnExpression left = new ColumnExpression("timestamp", DataTypes.TIMESTAMP);
      left.setColIndex(0);
      ColumnExpression right = new ColumnExpression("timestamp", DataTypes.TIMESTAMP);
      right.setColIndex(1);
      greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
      RowImpl value = new RowImpl();
      DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
      Date date = dateFormat.parse("23/09/2007");
      long time = date.getTime();
      Timestamp[] row = { new Timestamp(time) };
      Timestamp[] row1 = { new Timestamp(time) };
      Object objectRow[] = { row, row1 };
      value.setValues(objectRow);

      new MockUp<ExpressionResult>() {
        @Mock public Long getTimeAsMillisecond() {
          return 18465213000000L;
        }
      };

      ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
      assertTrue(result.getBoolean());
    } catch (ParseException e) {
      System.out.println("Error while parsing " + e.getMessage());
    }
  }

  @Test public void testEvaluateForGreaterThanEqualToExpressionWithTimestampDataType1()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    try {
      ColumnExpression left = new ColumnExpression("timestamp", DataTypes.TIMESTAMP);
      left.setColIndex(0);
      ColumnExpression right = new ColumnExpression("timestamp", DataTypes.TIMESTAMP);
      right.setColIndex(1);
      greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
      RowImpl value = new RowImpl();
      DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
      Date date = dateFormat.parse("23/09/2007");
      long time = date.getTime();
      Timestamp[] row = { new Timestamp(time) };
      Timestamp[] row1 = { new Timestamp(time) };
      Object objectRow[] = { row, row1 };
      value.setValues(objectRow);

      new MockUp<ExpressionResult>() {
        boolean isFirst = true;
        @Mock public Long getTimeAsMillisecond() {
          if (isFirst) {
            isFirst = false;
            return 1190505600L;
          }
          return 18465213000000L;
        }
      };

      ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
      assertFalse(result.getBoolean());
    } catch (ParseException e) {
      System.out.println("Error while parsing " + e.getMessage());
    }
  }

  @Test public void testForGreaterThanEqualToExpressionWithDefaultCase()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataTypes.BOOLEAN);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("contact", DataTypes.BOOLEAN);
    left.setColIndex(1);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Boolean[] row = { true };
    Object objectRow[] = { row, row };
    value.setValues(objectRow);
    greaterThanEqualToExpression.evaluate(value);
  }

  @Test public void testEvaluateForGreaterThanEqualToExpressionWithDecimalDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataTypes.createDefaultDecimalType());
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("contact", DataTypes.createDefaultDecimalType());
    left.setColIndex(1);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    BigDecimal[] row = new BigDecimal[] { new BigDecimal(12345.0) };
    Object objectRow[] = { row, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public BigDecimal getDecimal() {
        return new BigDecimal(12345.0);
      }
    };

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForGreaterThanEqualToExpressionWithIsNullReturnTrue()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("id", DataTypes.SHORT);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("id", DataTypes.SHORT);
    right.setColIndex(1);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Short[] row = { 15 };
    Object objectRow[] = { row, row };
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

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertFalse(result.getBoolean());

  }

  @Test public void testEvaluateForGreaterThanEqualToExpressionWithIsNullReturnTrue1()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("id", DataTypes.SHORT);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("id", DataTypes.SHORT);
    right.setColIndex(1);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Short[] row = { 15 };
    Object objectRow[] = { row, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      boolean isFirst = true;
      @Mock public boolean isNull() {
        if (isFirst) {
          isFirst = false;
          return false;
        }
        return true;
      }
    };

    new MockUp<ExpressionResult>() {
      @Mock public Short getShort() {
        return 15;
      }
    };

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertFalse(result.getBoolean());

  }

  @Test public void testEvaluateForGreaterThanEqualToExpressionWithLeftAndRightDifferentDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("name", DataTypes.STRING);
    left.setColIndex(1);
    ColumnExpression right = new ColumnExpression("number", DataTypes.INT);
    right.setColIndex(0);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    String[] row1 = { "String1" };
    Integer[] row = { 14 };
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Integer getInt() {
        return 14;
      }
    };

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForGreaterThanEqualToExpressionWithLeftAndRightDifferentDataType1()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("name", DataTypes.INT);
    left.setColIndex(1);
    ColumnExpression right = new ColumnExpression("number", DataTypes.STRING);
    right.setColIndex(0);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    String[] row1 = { "String1" };
    Integer[] row = { 14 };
    Object objectRow[] = { row1, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Integer getInt() {
        return 14;
      }
    };

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testForGreaterThanEqualToExpressionWithGetString() throws Exception {
    ColumnExpression left = new ColumnExpression("left_name", DataTypes.STRING);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_name", DataTypes.STRING);
    right.setColIndex(1);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    String expected_result = "GreaterThanEqualTo(ColumnExpression(left_name),ColumnExpression(right_name))";
    String result = greaterThanEqualToExpression.getString();
    assertEquals(expected_result, result);
  }

}

