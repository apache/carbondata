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

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.intf.RowImpl;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class InExpressionUnitTest {

  static InExpression inExpression;

  @Test public void testEvaluateForInExpressionWithString()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("left_name", DataTypes.STRING);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_name", DataTypes.STRING);
    right.setColIndex(1);
    inExpression = new InExpression(left, right);
    RowImpl value = new RowImpl();
    String row = "string1";
    String row1 = "string1";
    Object objectRow[] = { row, row1 };

    new MockUp<ExpressionResult>() {

      @Mock public DataType getDataType() {
        return DataTypes.STRING;
      }

      @Mock public String getString() {
        return "string1";
      }
    };

    value.setValues(objectRow);
    ExpressionResult result = inExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForInExpressionWithShortDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {

    ColumnExpression left = new ColumnExpression("left_id", DataTypes.SHORT);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_id", DataTypes.SHORT);
    right.setColIndex(1);
    inExpression = new InExpression(left, right);
    RowImpl value = new RowImpl();
    Short row = 150;
    Short row1 = 150;
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Short getShort() {
        return 150;
      }
    };

    ExpressionResult result = inExpression.evaluate(value);
    assertEquals(result.getDataType(), DataTypes.BOOLEAN);

  }

  @Test public void testEvaluateForInExpressionWithIntDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {

    ColumnExpression left = new ColumnExpression("left_id", DataTypes.INT);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_id", DataTypes.INT);
    right.setColIndex(1);
    inExpression = new InExpression(left, right);
    RowImpl value = new RowImpl();
    Integer row = 15052;
    Integer row1 = 15052;
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Integer getInt() {
        return 15052;
      }
    };

    ExpressionResult result = inExpression.evaluate(value);
    assertEquals(result.getDataType(), DataTypes.BOOLEAN);

  }

  @Test public void testEvaluateForInExpressionWithDoubleDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("left_contact", DataTypes.DOUBLE);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_contact", DataTypes.DOUBLE);
    right.setColIndex(1);
    inExpression = new InExpression(left, right);
    RowImpl value = new RowImpl();
    Double row = 44521D;
    Double row1 = 44521D;
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Double getDouble() {
        return 44521D;
      }
    };

    ExpressionResult result = inExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForInExpressionWithLongDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("left_contact", DataTypes.LONG);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_contact", DataTypes.LONG);
    right.setColIndex(1);
    inExpression = new InExpression(left, right);
    RowImpl value = new RowImpl();
    Long row = 1234567654321L;
    Long row1 = 1234567654321L;
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Long getLong() {
        return 1234567654321L;
      }
    };

    ExpressionResult result = inExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForInExpressionWithTimestampDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    try {
      ColumnExpression left = new ColumnExpression("left_timestamp", DataTypes.TIMESTAMP);
      left.setColIndex(0);
      ColumnExpression right = new ColumnExpression("right_timestamp", DataTypes.TIMESTAMP);
      right.setColIndex(1);
      inExpression = new InExpression(left, right);

      RowImpl value = new RowImpl();
      DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
      Date date = dateFormat.parse("23/09/2007");

      long time = date.getTime();

      Timestamp row = new Timestamp(time);
      Timestamp row1 = new Timestamp(time);
      Object objectRow[] = { row, row1 };
      value.setValues(objectRow);

     new MockUp<ExpressionResult>() {
        @Mock public Long getTime() {
          return 18465213000000L;
        }
      };

      ExpressionResult result = inExpression.evaluate(value);
      assertFalse(result.getBoolean());
    } catch (ParseException e) {
      System.out.println("Error while parsing " + e.getMessage());
    }
  }

  @Test public void testEvaluateForInExpressionWithDecimalDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("left_contact", DataTypes.createDefaultDecimalType());
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_contact", DataTypes.createDefaultDecimalType());
    right.setColIndex(1);
    inExpression = new InExpression(left, right);
    RowImpl value = new RowImpl();
    BigDecimal row = new BigDecimal(123452154.0);
    BigDecimal row1 = new BigDecimal(123452154.0);
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public BigDecimal getDecimal() {
        return new BigDecimal(123452154.0);
      }
    };

    ExpressionResult result = inExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testForInExpressionWithDefaultCase()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("contact", DataTypes.BOOLEAN);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("contact", DataTypes.BOOLEAN);
    right.setColIndex(1);
    inExpression = new InExpression(left, right);
    RowImpl value = new RowImpl();
    Boolean row = true;
    Boolean row1 = true;
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);
    inExpression.evaluate(value);
  }

  @Test public void testForInExpressionWithGetString() throws Exception {
    ColumnExpression left = new ColumnExpression("left_name", DataTypes.STRING);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_name", DataTypes.STRING);
    right.setColIndex(1);
    inExpression = new InExpression(left, right);
    String expected_result = "IN(ColumnExpression(left_name),ColumnExpression(right_name))";
    String result = inExpression.getString();
    assertEquals(expected_result, result);
  }

  @Test public void testEvaluateForInExpressionWithLeftAndRightDifferentDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("name", DataTypes.STRING);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("number", DataTypes.INT);
    left.setColIndex(1);
    inExpression = new InExpression(left, right);
    RowImpl value = new RowImpl();
    String row1 =  "String1";
    Integer row =  14523 ;
    Object objectRow[] = { row1, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Integer getInt() {
        return 14523;
      }
    };

    ExpressionResult result = inExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }
}
