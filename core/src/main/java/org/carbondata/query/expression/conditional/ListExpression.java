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

package org.carbondata.query.expression.conditional;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.query.carbonfilterinterface.ExpressionType;
import org.carbondata.query.carbonfilterinterface.RowIntf;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.ExpressionResult;
import org.carbondata.query.expression.exception.FilterIllegalMemberException;
import org.carbondata.query.expression.exception.FilterUnsupportedException;

public class ListExpression extends Expression {
  private static final long serialVersionUID = 1L;

  public ListExpression(List<Expression> children) {
    this.children = children;
  }

  @Override public ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException {
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

  @Override public ExpressionType getFilterExpressionType() {
    // TODO Auto-generated method stub
    return ExpressionType.LIST;
  }

  @Override public String getString() {
    // TODO Auto-generated method stub
    return null;
  }

}
