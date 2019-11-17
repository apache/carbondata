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

import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.intf.RowImpl;

import org.junit.Test;

import java.util.ArrayList;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ListExpressionUnitTest {

  static ListExpression listExpression;

  @Test public void test() throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("left_name", DataTypes.STRING);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_name", DataTypes.STRING);
    right.setColIndex(1);

    List<Expression> children = new ArrayList<>();
    children.add(left);
    children.add(right);

    listExpression = new ListExpression(children);
    RowImpl value = new RowImpl();
    String row = "Row is for left";
    String row1 = "I am row 1";
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);
    String expected_value = "Row is for left";
    ExpressionResult result = listExpression.evaluate(value);
    assertThat(expected_value, is(equalTo(result.getList().get(0).getString())));
  }

  @Test public void testGetString() throws FilterUnsupportedException, FilterIllegalMemberException  {
    ColumnExpression left = new ColumnExpression("left_name", DataTypes.STRING);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_name", DataTypes.STRING);
    right.setColIndex(1);

    List<Expression> children = new ArrayList<>();
    children.add(left);
    children.add(right);

    listExpression = new ListExpression(children);
    RowImpl value = new RowImpl();
    String row = "Row is for left";
    String row1 = "I am row 1";
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);
    String expected_value = "ListExpression(ColumnExpression(left_name);ColumnExpression(right_name);)";
    String exresult = listExpression.getString();
    assertTrue(exresult.equals(expected_value));
  }
}
