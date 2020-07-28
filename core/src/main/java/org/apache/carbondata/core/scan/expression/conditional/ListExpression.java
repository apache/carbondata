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

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;

public class ListExpression extends Expression {
  private static final long serialVersionUID = 1L;

  public ListExpression(List<Expression> children) {
    this.children = children;
  }

  @Override
  public ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException {
    List<ExpressionResult> listOfExprRes = new ArrayList<ExpressionResult>(10);

    for (Expression expr : children) {
      try {
        listOfExprRes.add(expr.evaluate(value));
      } catch (FilterIllegalMemberException e) {
        continue;
      }
    }
    return new ExpressionResult(listOfExprRes);
  }

  @Override
  public ExpressionType getFilterExpressionType() {
    return ExpressionType.LIST;
  }

  @Override
  public String getString() {
    StringBuilder value = new StringBuilder();
    value.append("ListExpression(");
    for (Expression expr : children) {
      value.append(expr.getString()).append(";");
    }
    value.append(')');

    return  value.toString();
  }

  @Override
  public String getStatement() {
    StringBuilder value = new StringBuilder();
    value.append("(");
    for (Expression expr : children) {
      value.append(expr.getString()).append(";");
    }
    value.append(')');

    return value.toString();
  }

  @Override
  public void findAndSetChild(Expression oldExpr, Expression newExpr) {
  }
}
