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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NotInExpressionUnitTest {

  static NotInExpression notInExpression;

  @Test public void testEvaluateForNotInExpressionWithString()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("left_name", DataType.STRING);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_name", DataType.STRING);
    right.setColIndex(1);
    notInExpression = new NotInExpression(left, right);
    RowImpl value = new RowImpl();
    String row = "Row is for left";
    String row1 = "I am row 1";
    Object objectRow[] = { row, row1 };

    new MockUp<ExpressionResult>() {

      @Mock public DataType getDataType() {
        return DataType.STRING;
      }

      @Mock public String getString() {
        return "string1";
      }
    };

    value.setValues(objectRow);
    ExpressionResult result = notInExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForNotInExpressionWithShortDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {

    ColumnExpression left = new ColumnExpression("left_id", DataType.SHORT);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_id", DataType.SHORT);
    right.setColIndex(1);
    notInExpression = new NotInExpression(left, right);
    RowImpl value = new RowImpl();
    Short row = 15653;
    Short row1 = 15582;
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Short getShort() {
        return 15582;
      }
    };

    ExpressionResult result = notInExpression.evaluate(value);
    assertEquals(result.getDataType(), DataType.BOOLEAN);

  }

  @Test public void testEvaluateForNotInExpressionWithIntDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {

    ColumnExpression left = new ColumnExpression("left_id", DataType.INT);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_id", DataType.INT);
    right.setColIndex(1);
    notInExpression = new NotInExpression(left, right);
    RowImpl value = new RowImpl();
    Integer row = 150569;
    Integer row1 = 15052;
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Integer getInt() {
        return 15052;
      }
    };

    ExpressionResult result = notInExpression.evaluate(value);
    assertEquals(result.getDataType(), DataType.BOOLEAN);

  }

  @Test public void testEvaluateForNotInExpressionWithDoubleDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("left_contact", DataType.DOUBLE);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_contact", DataType.DOUBLE);
    right.setColIndex(1);
    notInExpression = new NotInExpression(left, right);
    RowImpl value = new RowImpl();
    Double row = 44521D;
    Double row1 = 44521.023D;
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Double getDouble() {
        return 44521.023D;
      }
    };

    ExpressionResult result = notInExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForNotInExpressionWithLongDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("left_contact", DataType.LONG);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_contact", DataType.LONG);
    right.setColIndex(1);
    notInExpression = new NotInExpression(left, right);
    RowImpl value = new RowImpl();
    Long row = 123456256325632L;
    Long row1 = 156212456245556L;
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Long getLong() {
        return 156212456245556L;
      }
    };

    ExpressionResult result = notInExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateForNotInExpressionWithTimestampDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    try {
      ColumnExpression left = new ColumnExpression("left_timestamp", DataType.TIMESTAMP);
      left.setColIndex(0);
      ColumnExpression right = new ColumnExpression("right_timestamp", DataType.TIMESTAMP);
      right.setColIndex(1);
      notInExpression = new NotInExpression(right, right);

      RowImpl value = new RowImpl();
      DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
      Date date = dateFormat.parse("2007/03/03");

      long time = date.getTime();

      Timestamp row = new Timestamp(time);
      Timestamp row1 = new Timestamp(time);
      Object objectRow[] = { row, row1 };
      value.setValues(objectRow);

      new MockUp<ExpressionResult>() {
        @Mock public Long getTime() {
          return 1172860200000L;
        }
      };

      ExpressionResult result = notInExpression.evaluate(value);
      assertTrue(result.getBoolean());
    } catch (ParseException e) {
      System.out.println("Error while parsing " + e.getMessage());
    }
  }

  @Test public void testEvaluateForNotInExpressionWithDecimalDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("left_contact", DataType.DECIMAL);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_contact", DataType.DECIMAL);
    right.setColIndex(1);
    notInExpression = new NotInExpression(left, right);
    RowImpl value = new RowImpl();
    Decimal row = Decimal.apply(123452154.0);
    Decimal row1 = Decimal.apply(1234521215454.0);
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public BigDecimal getDecimal() {
        return new BigDecimal(1234521215454.0);
      }
    };

    ExpressionResult result = notInExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test(expected = FilterUnsupportedException.class) public void testDefaultCaseForNotInExpression()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("contact", DataType.BOOLEAN);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("contact", DataType.BOOLEAN);
    right.setColIndex(1);
    notInExpression = new NotInExpression(left, right);
    RowImpl value = new RowImpl();
    Boolean row = true;
    Boolean row1 = true;
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);
    notInExpression.evaluate(value);
  }

  @Test public void testForNotInExpressionWithGetString() throws Exception {
    ColumnExpression left = new ColumnExpression("left_name", DataType.STRING);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_name", DataType.STRING);
    right.setColIndex(1);
    notInExpression = new NotInExpression(left, right);
    String expected_result = "NOT IN(ColumnExpression(left_name),ColumnExpression(right_name))";
    String result = notInExpression.getString();
    assertEquals(expected_result, result);
  }
  @Test public void testEvaluateForNotInExpressionWithLeftAndRightDifferentDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("name", DataType.STRING);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("number", DataType.INT);
    left.setColIndex(1);
    notInExpression = new NotInExpression(left, right);
    RowImpl value = new RowImpl();
    String row1 = "String1";
    Integer row = 14523213;
    Object objectRow[] = { row1, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Integer getInt() {
        return 145232130;
      }
    };

    ExpressionResult result = notInExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

}
