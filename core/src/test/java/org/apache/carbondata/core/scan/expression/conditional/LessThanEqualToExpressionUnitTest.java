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

public class LessThanEqualToExpressionUnitTest {

  static LessThanEqualToExpression lessThanEqualToExpression;

  @Test public void testEvaluateForLessThanEqualToExpressionWithBothStringISSame()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("left_name", DataTypes.STRING);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_name", DataTypes.STRING);
    left.setColIndex(1);
    lessThanEqualToExpression = new LessThanEqualToExpression(left, left);
    RowImpl value = new RowImpl();
    String[] row = { "String is Value" };
    String[] row1 = { "string1" };
    Object objectRow[] = { row, row1 };

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public String getString() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return "String is Value";

        } else {
          return "string1";

        }

      }
    };

    value.setValues(objectRow);
    ExpressionResult result = lessThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForLessThanEqualToExpressionWithShortDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("id", DataTypes.SHORT);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("id", DataTypes.SHORT);
    left.setColIndex(1);
    lessThanEqualToExpression = new LessThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Short[] row = { 1550 };
    Short[] row1 = { 3365 };
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public Short getShort() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return 1550;

        } else {
          return 3365;

        }

      }
    };

    ExpressionResult result = lessThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());

  }

  @Test public void testEvaluateForLessThanEqualToExpressionWithShortDataType1()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("id", DataTypes.SHORT);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("id", DataTypes.SHORT);
    left.setColIndex(1);
    lessThanEqualToExpression = new LessThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Short[] row = { 1550 };
    Short[] row1 = { 3365 };
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public Short getShort() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return 3365;

        } else {
          return 1550;

        }

      }
    };

    ExpressionResult result = lessThanEqualToExpression.evaluate(value);
    assertFalse(result.getBoolean());

  }

  @Test public void testEvaluateForLessThanEqualToExpressionWithDoubleDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("right_contact", DataTypes.DOUBLE);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("left_contact", DataTypes.DOUBLE);
    left.setColIndex(1);
    lessThanEqualToExpression = new LessThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Double[] row = { 4852.2D };
    Double[] row1 = { 4852.2D };
    Object objectRow[] = { row1, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public Double getDouble() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return 4852.2D;

        } else {
          return 4852.2D;

        }

      }
    };

    ExpressionResult result = lessThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForLessThanEqualToExpressionWithIntDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("right_number", DataTypes.INT);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("left_number", DataTypes.INT);
    left.setColIndex(1);
    lessThanEqualToExpression = new LessThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Integer[] row = { 144580 };
    Integer[] row1 = { 14500 };
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public Integer getInt() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return 14500;

        } else {
          return 144580;

        }

      }
    };

    ExpressionResult result = lessThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForLessThanEqualToExpressionWithTimestampDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    try {
      ColumnExpression left = new ColumnExpression("timestamp", DataTypes.TIMESTAMP);
      left.setColIndex(0);
      ColumnExpression right = new ColumnExpression("timestamp", DataTypes.TIMESTAMP);
      right.setColIndex(1);

      lessThanEqualToExpression = new LessThanEqualToExpression(left, right);

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

        @Mock public Long getTimeAsMillisecond() {
          if (returnMockFlag) {
            returnMockFlag = false;
            return 1190505600L;
          } else {
            return 1190592000L;
          }
        }
      };

      ExpressionResult result = lessThanEqualToExpression.evaluate(value);
      assertTrue(result.getBoolean());
    } catch (ParseException e) {
      System.out.println("Error while parsing " + e.getMessage());
    }
  }

  @Test public void testEvaluateForLessThanEqualToExpressionWithTimestampDataType1()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    try {
      ColumnExpression left = new ColumnExpression("timestamp", DataTypes.TIMESTAMP);
      left.setColIndex(0);
      ColumnExpression right = new ColumnExpression("timestamp", DataTypes.TIMESTAMP);
      right.setColIndex(1);

      lessThanEqualToExpression = new LessThanEqualToExpression(left, right);

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

        @Mock public Long getTimeAsMillisecond() {
          if (returnMockFlag) {
            returnMockFlag = false;
            return 1190592000L;
          } else {
            return 1190505600L;
          }
        }
      };

      ExpressionResult result = lessThanEqualToExpression.evaluate(value);
      assertFalse(result.getBoolean());
    } catch (ParseException e) {
      System.out.println("Error while parsing " + e.getMessage());
    }
  }

  @Test public void testEvaluateForLessThanEqualToExpressionWithLongDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("right_contact", DataTypes.LONG);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("left_contact", DataTypes.LONG);
    left.setColIndex(1);
    lessThanEqualToExpression = new LessThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Long[] row = { 4751256L };
    Long[] row1 = { 48512586L };
    Object objectRow[] = { row1, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public Long getLong() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return 4751256L;
        } else {
          return 48512586L;
        }
      }
    };

    ExpressionResult result = lessThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForLessThanEqualToExpressionWithDecimalDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("right_contact", DataTypes.createDefaultDecimalType());
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("left_contact", DataTypes.createDefaultDecimalType());
    left.setColIndex(1);
    lessThanEqualToExpression = new LessThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    BigDecimal[] row = new BigDecimal[] { new BigDecimal(46851.2) };
    BigDecimal[] row1 = new BigDecimal[] { new BigDecimal(45821.02) };
    Object objectRow[] = { row1, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public BigDecimal getDecimal() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return new BigDecimal(45821.02);
        } else {
          return new BigDecimal(46851.2);
        }
      }
    };

    ExpressionResult result = lessThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForLessThanEqualToExpressionWithDecimalDataType1()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right =
        new ColumnExpression("right_contact", DataTypes.createDefaultDecimalType());
    right.setColIndex(0);
    ColumnExpression left =
        new ColumnExpression("left_contact", DataTypes.createDefaultDecimalType());
    left.setColIndex(1);
    lessThanEqualToExpression = new LessThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    BigDecimal[] row = new BigDecimal[] { new BigDecimal(46851.2) };
    BigDecimal[] row1 = new BigDecimal[] { new BigDecimal(45821.02) };
    Object objectRow[] = { row1, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public BigDecimal getDecimal() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return new BigDecimal(46851.2);
        } else {
          return new BigDecimal(45821.02);
        }
      }
    };

    ExpressionResult result = lessThanEqualToExpression.evaluate(value);
    assertFalse(result.getBoolean());
  }

  @Test public void testForLessThanEqualToExpressionWithDefaultCase()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataTypes.BOOLEAN);
    right.setColIndex(0);
    lessThanEqualToExpression = new LessThanEqualToExpression(right, right);
    RowImpl value = new RowImpl();
    Boolean[] row = { true };
    Object objectRow[] = { row };
    value.setValues(objectRow);
    lessThanEqualToExpression.evaluate(value);
  }

  @Test public void testEvaluateForLessThanEqualToExpressionWithIsNullReturnTrue()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("id", DataTypes.SHORT);
    right.setColIndex(0);
    lessThanEqualToExpression = new LessThanEqualToExpression(right, right);
    RowImpl value = new RowImpl();
    Short[] row = { 15856 };
    Object objectRow[] = { row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public boolean isNull() {
        return true;
      }
    };

    new MockUp<ExpressionResult>() {
      @Mock public Short getShort() {
        return 15856;
      }
    };

    ExpressionResult result = lessThanEqualToExpression.evaluate(value);
    assertFalse(result.getBoolean());

  }

  @Test public void testEvaluateForLessThanEqualToExpressionWithIsNullReturnTrue1()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("id", DataTypes.SHORT);
    right.setColIndex(0);
    lessThanEqualToExpression = new LessThanEqualToExpression(right, right);
    RowImpl value = new RowImpl();
    Short[] row = { 15856 };
    Object objectRow[] = { row };
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
        return 15856;
      }
    };

    ExpressionResult result = lessThanEqualToExpression.evaluate(value);
    assertFalse(result.getBoolean());

  }

  @Test public void testEvaluateForLessThanEqualToExpressionWithLeftAndRightDifferentDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("name", DataTypes.STRING);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("number", DataTypes.INT);
    right.setColIndex(1);
    lessThanEqualToExpression = new LessThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    String[] row = { "S" };
    Integer[] row1 = { 1450 };
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public Integer getInt() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return 84;
        } else {
          return 1450;

        }

      }
    };

    ExpressionResult result = lessThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForLessThanEqualToExpressionWithLeftAndRightDifferentDataType1()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("name", DataTypes.INT);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("number", DataTypes.STRING);
    right.setColIndex(1);
    lessThanEqualToExpression = new LessThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    String[] row = { "S" };
    Integer[] row1 = { 1450 };
    Object objectRow[] = { row1, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      Boolean returnMockFlag = true;

      @Mock public Integer getInt() {
        if (returnMockFlag) {
          returnMockFlag = false;
          return 84;
        } else {
          return 1450;

        }

      }
    };

    ExpressionResult result = lessThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testForLessThanEqualToExpressionWithGetString() throws Exception {
    ColumnExpression right = new ColumnExpression("right_name", DataTypes.STRING);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("left_name", DataTypes.STRING);
    left.setColIndex(0);
    lessThanEqualToExpression = new LessThanEqualToExpression(left, right);
    String expected_result =
        "LessThanEqualTo(ColumnExpression(left_name),ColumnExpression(right_name))";
    String result = lessThanEqualToExpression.getString();
    assertEquals(expected_result, result);
  }
}
