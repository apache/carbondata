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

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.filter.intf.RowImpl;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ColumnExpressionTest {

  private ColumnExpression columnExpression;

  @Before public void setUp() {
    String columnName = "IMEI";
    DataType dataType = DataTypes.STRING;
    columnExpression = new ColumnExpression(columnName, dataType);
  }

  @Test public void testEvaluate() {
    RowImpl rowImpl = new RowImpl();
    rowImpl.setValues(new Integer[] { 1 });
    new MockUp<RowImpl>() {
      @Mock public Object getVal(int index) {
        return 1;
      }
    };
    ExpressionResult expectedValue = new ExpressionResult(DataTypes.INT, 1);
    assertEquals(expectedValue, columnExpression.evaluate(rowImpl));
    assertEquals(1, rowImpl.size());
  }

  @Test public void testEvaluateForNullValue() {
    RowImpl rowImpl = new RowImpl();
    rowImpl.setValues(null);
    new MockUp<RowImpl>() {
      @Mock public Object getVal(int index) {
        return null;
      }
    };
    ExpressionResult expectedValue = new ExpressionResult(null);
    assertEquals(expectedValue, columnExpression.evaluate(rowImpl));
  }

  @Test public void testGetString() {
    String actualValue = columnExpression.getString();
    String expectedValue = "ColumnExpression(IMEI)";
    assertEquals(expectedValue, actualValue);
  }
}
