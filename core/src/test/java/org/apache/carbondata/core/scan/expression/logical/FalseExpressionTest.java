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
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.RowImpl;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FalseExpressionTest {
  private FalseExpression falseExpression;

  @Before public void setUp() {
    ColumnExpression columnExpression = new ColumnExpression("IMEI", DataTypes.BOOLEAN);
    falseExpression = new FalseExpression(columnExpression);
  }

  @Test public void testEvaluate() throws FilterUnsupportedException, FilterIllegalMemberException {
    RowImpl rowImpl = new RowImpl();
    rowImpl.setValues(new Boolean[] { true });
    ExpressionResult actualValue = falseExpression.evaluate(rowImpl);
    assertEquals(new ExpressionResult(DataTypes.BOOLEAN, false), actualValue);
  }

  @Test public void testGetString() {
    String actualValue = falseExpression.getString();
    String expectedValue = "False(ColumnExpression(IMEI)";
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testFilterExpressionType() {
    ExpressionType actualValue = falseExpression.getFilterExpressionType();
    assertEquals(ExpressionType.FALSE, actualValue);
  }
}
