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

package org.apache.carbondata.scan.expression.logical;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.scan.expression.ColumnExpression;
import org.apache.carbondata.scan.expression.ExpressionResult;
import org.apache.carbondata.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.scan.filter.intf.RowImpl;
import org.apache.carbondata.scan.filter.intf.RowIntf;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class OrExpressionTest {
  private OrExpression orExpression;

  @Before public void setUp() {
    ColumnExpression leftExpression = new ColumnExpression("IMEI", DataType.BOOLEAN);
    ColumnExpression rightExpression = new ColumnExpression("IMEI", DataType.BOOLEAN);
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
    final ExpressionResult expressionResult = new ExpressionResult(DataType.BOOLEAN, "test");
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
    final ExpressionResult expressionResult = new ExpressionResult(DataType.STRING, "test");
    new MockUp<ColumnExpression>() {
      @Mock public ExpressionResult evaluate(RowIntf value)
          throws FilterUnsupportedException, FilterIllegalMemberException {
        return expressionResult;
      }
    };
    orExpression.evaluate(rowImpl);
  }
}
