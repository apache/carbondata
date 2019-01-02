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

package org.apache.carbondata.core.scan.expression;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.filter.intf.RowImpl;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LiteralExpressionTest {

  private LiteralExpression literalExpression;

  @Before public void setUp() {
    String value = "testing";
    literalExpression = new LiteralExpression(value, DataTypes.STRING);
  }

  @Test public void testEvaluate() {
    RowImpl rowImpl = new RowImpl();
    rowImpl.setValues(new String[] { "testing" });
    ExpressionResult expectedResult = new ExpressionResult(DataTypes.STRING, "testing");
    assertEquals(expectedResult, literalExpression.evaluate(rowImpl));
  }

  @Test public void testGetExpressionResult() {
    RowImpl rowImpl = new RowImpl();
    rowImpl.setValues(new String[] { "testing" });
    literalExpression.evaluate(rowImpl);
    ExpressionResult expectedResult = new ExpressionResult(DataTypes.STRING, "testing");
    assertEquals(expectedResult, literalExpression.evaluate(rowImpl));
  }

  @Test public void testGetString() {
    String actualValue = literalExpression.getString();
    String expectedValue = "LiteralExpression(testing)";
    assertEquals(expectedValue, actualValue);
  }
}
