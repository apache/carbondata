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

package org.apache.carbondata.core.scan.expression.logical;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.intf.RowImpl;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class OrExpressionTest {
  private OrExpression orExpression;

  @Before public void setUp() {
    ColumnExpression leftExpression = new ColumnExpression("IMEI", DataTypes.BOOLEAN);
    ColumnExpression rightExpression = new ColumnExpression("IMEI", DataTypes.BOOLEAN);
    orExpression = new OrExpression(leftExpression, rightExpression);
  }

  @Test public void testGetString() {
    String actualValue = orExpression.getString();
    String expectedValue = "Or(ColumnExpression(IMEI),ColumnExpression(IMEI))";
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testEvaluate() throws FilterIllegalMemberException, FilterUnsupportedException {
    RowImpl rowImpl = new RowImpl();
    rowImpl.setValues(new Boolean[] { false });
    final ExpressionResult expressionResult = new ExpressionResult(DataTypes.BOOLEAN, "test");
    new MockUp<ColumnExpression>() {
      @Mock public ExpressionResult evaluate(RowIntf value) {
        return expressionResult;
      }
    };

    assertTrue(orExpression.evaluate(rowImpl) instanceof ExpressionResult);
  }

  @Test public void testEvaluate1() throws FilterIllegalMemberException,
  FilterUnsupportedException {
    RowImpl rowImpl = new RowImpl();
    rowImpl.setValues(new Boolean[] { false });
    final ExpressionResult expressionResult = new ExpressionResult(DataTypes.BOOLEAN, "true");
    new MockUp<ColumnExpression>() {
      @Mock public ExpressionResult evaluate(RowIntf value) {
        return expressionResult;
      }
    };

    assertTrue(orExpression.evaluate(rowImpl) instanceof ExpressionResult);
  }

  @Test public void testEvaluate2() throws FilterIllegalMemberException,
      FilterUnsupportedException {
    RowImpl rowImpl = new RowImpl();
    rowImpl.setValues(new Boolean[] { false });
    final ExpressionResult expressionResultRight = new ExpressionResult(DataTypes.BOOLEAN, "false");
    final ExpressionResult expressionResultLeft = new ExpressionResult(DataTypes.BOOLEAN, "true");
    new MockUp<ColumnExpression>() {
      boolean isLeft = true;
      @Mock public ExpressionResult evaluate(RowIntf value) {
        if (isLeft) {
          isLeft = false;
          return expressionResultLeft;
        }
        return expressionResultRight;
      }
    };

    assertTrue(orExpression.evaluate(rowImpl) instanceof ExpressionResult);
  }

  @Test public void testEvaluate3() throws FilterIllegalMemberException,
      FilterUnsupportedException {
    RowImpl rowImpl = new RowImpl();
    rowImpl.setValues(new Boolean[] { false });
    final ExpressionResult expressionResultRight = new ExpressionResult(DataTypes.BOOLEAN, "false");
    final ExpressionResult expressionResultLeft = new ExpressionResult(DataTypes.BOOLEAN, "false");
    new MockUp<ColumnExpression>() {
      boolean isLeft = true;
      @Mock public ExpressionResult evaluate(RowIntf value) {
        if (isLeft) {
          isLeft = false;
          return expressionResultLeft;
        }
        return expressionResultRight;
      }
    };

    assertTrue(orExpression.evaluate(rowImpl) instanceof ExpressionResult);
  }

  @Test public void testEvaluate4()
      throws FilterIllegalMemberException, FilterUnsupportedException {
    RowImpl rowImpl = new RowImpl();
    rowImpl.setValues(new Boolean[] { false });
    final ExpressionResult expressionResult = new ExpressionResult(DataTypes.BOOLEAN, "false");
    new MockUp<ColumnExpression>() {
      @Mock public ExpressionResult evaluate(RowIntf value) {
        return expressionResult;
      }
    };

    assertTrue(orExpression.evaluate(rowImpl) instanceof ExpressionResult);
  }

  @Test(expected = Exception.class) public void testEvaluateForDefault()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    RowImpl rowImpl = new RowImpl();
    rowImpl.setValues(new Boolean[] { true });
    final ExpressionResult expressionResult = new ExpressionResult(DataTypes.STRING, "test");
    new MockUp<ColumnExpression>() {
      @Mock public ExpressionResult evaluate(RowIntf value)
          throws FilterUnsupportedException, FilterIllegalMemberException {
        return expressionResult;
      }
    };
    orExpression.evaluate(rowImpl);
  }
}
