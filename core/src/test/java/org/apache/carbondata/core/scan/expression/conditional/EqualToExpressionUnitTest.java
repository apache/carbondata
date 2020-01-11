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
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class EqualToExpressionUnitTest {

  static EqualToExpression equalToExpression;

  static int i = 0;

  @Test public void testForEqualToExpressionWithGetString() throws Exception {
    ColumnExpression right = new ColumnExpression("name", DataTypes.STRING);
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(right, right);
    String expected_result = "EqualTo(ColumnExpression(name),ColumnExpression(name))";
    String result = equalToExpression.getString();
    assertEquals(expected_result, result);
  }

  @Test public void testEvaluateForEqualToExpressionWithShortDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("id", DataTypes.SHORT);
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(right, right);
    RowImpl value = new RowImpl();
    Short[] row = { 15 };
    Object objectRow[] = { row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Short getShort() {
        return 15;
      }
    };

    ExpressionResult result = equalToExpression.evaluate(value);
    assertTrue(result.getBoolean());

  }

  @Test public void testEvaluateForEqualToExpressionWithStringDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("name", DataTypes.STRING);
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(right, right);
    RowImpl value = new RowImpl();
    String[] row = { "String1" };
    Object objectRow[] = { row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public String getString() {
        return "String1";
      }
    };

    ExpressionResult result = equalToExpression.evaluate(value);
    assertTrue(result.getBoolean());

  }

  @Test public void testEvaluateForEqualToExpressionWithIntDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("number", DataTypes.INT);
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(right, right);
    RowImpl value = new RowImpl();
    Integer[] row = { 14 };
    Object objectRow[] = { row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Integer getInt() {
        return 14;
      }
    };

    ExpressionResult result = equalToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForEqualToExpressionWithDoubleDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataTypes.DOUBLE);
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(right, right);
    RowImpl value = new RowImpl();
    Double[] row = { 44D };
    Object objectRow[] = { row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Double getDouble() {
        return 44D;
      }
    };

    ExpressionResult result = equalToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForEqualToExpressionWithLongDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataTypes.LONG);
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(right, right);
    RowImpl value = new RowImpl();
    Long[] row = { 1234567654321L };
    Object objectRow[] = { row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Long getLong() {
        return 1234567654321L;
      }
    };

    ExpressionResult result = equalToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForEqualToExpressionWithTimestampDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    try {
      ColumnExpression right = new ColumnExpression("timestamp", DataTypes.TIMESTAMP);
      right.setColIndex(0);
      equalToExpression = new EqualToExpression(right, right);
      RowImpl value = new RowImpl();
      DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
      Date date = dateFormat.parse("23/09/2007");
      long time = date.getTime();
      Timestamp[] row = { new Timestamp(time) };
      Object objectRow[] = { row };
      value.setValues(objectRow);

      new MockUp<ExpressionResult>() {
        @Mock public Long getTimeAsMillisecond() {
          return 18465213000000L;
        }
      };

      ExpressionResult result = equalToExpression.evaluate(value);
      assertTrue(result.getBoolean());
    } catch (ParseException e) {
      System.out.println("Error while parsing " + e.getMessage());
    }
  }

  @Test public void testForEqualToExpressionForDefaultCase()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataTypes.BOOLEAN);
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(right, right);
    RowImpl value = new RowImpl();
    Boolean[] row = { true };
    Object objectRow[] = { row };
    value.setValues(objectRow);
    ExpressionResult result = equalToExpression.evaluate(value);
  }

  @Test public void testEvaluateForEqualToExpressionWithBooleanParameter()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("id", DataTypes.SHORT);
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(right, right, true);
    RowImpl value = new RowImpl();
    Short[] row = { 15 };
    Object objectRow[] = { row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Short getShort() {
        return 15;
      }
    };

    ExpressionResult result = equalToExpression.evaluate(value);
    assertTrue(result.getBoolean());

  }

  @Test public void testEvaluateForEqualToExpressionWithLeftAndRightDifferentDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("name", DataTypes.STRING);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("number", DataTypes.INT);
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(left, right);
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

    ExpressionResult result = equalToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForEqualToExpressionWithLeftAndRightDifferentDataType1()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("name", DataTypes.INT);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("number", DataTypes.STRING);
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(left, right);
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

    ExpressionResult result = equalToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForEqualToExpressionWithIsNullReturnFalse()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("id", DataTypes.SHORT);
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(right, right);
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

    ExpressionResult result = equalToExpression.evaluate(value);
    assertFalse(result.getBoolean());

  }

  @Test public void testEvaluateForEqualToExpressionWithNullWhileCreatingObject()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("id", DataTypes.SHORT);
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(right, right, true);
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

    ExpressionResult result = equalToExpression.evaluate(value);
    assertEquals(DataTypes.BOOLEAN, result.getDataType());

  }

  @Test public void testEvaluateForEqualToExpressionWithDecimalDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataTypes.createDefaultDecimalType());
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(right, right);
    RowImpl value = new RowImpl();
    BigDecimal[] row = new BigDecimal[] { new BigDecimal(12345.0) };
    Object objectRow[] = { row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public BigDecimal getDecimal() {
        return new BigDecimal(12345.0);
      }
    };

    ExpressionResult result = equalToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForEqualToExpressionWithDecimalDataType1()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataTypes.createDefaultDecimalType());
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(right, right);
    RowImpl value = new RowImpl();
    BigDecimal[] row = new BigDecimal[] { new BigDecimal(12345.0) };
    BigDecimal[] row1 = new BigDecimal[] { new BigDecimal(12346.0) };
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public BigDecimal getDecimal() {
        if (i == 0) {
          i++;
          return new BigDecimal(12346.0);
        } else {
          return new BigDecimal(12345.0);
        }
      }
    };

    ExpressionResult result = equalToExpression.evaluate(value);
    assertFalse(result.getBoolean());
  }
}
