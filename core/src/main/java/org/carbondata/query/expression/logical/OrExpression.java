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

package org.carbondata.query.expression.logical;

import org.carbondata.query.carbonfilterinterface.ExpressionType;
import org.carbondata.query.carbonfilterinterface.RowIntf;
import org.carbondata.query.expression.DataType;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.ExpressionResult;
import org.carbondata.query.expression.exception.FilterIllegalMemberException;
import org.carbondata.query.expression.exception.FilterUnsupportedException;

public class OrExpression extends BinaryLogicalExpression {

  private static final long serialVersionUID = 4220598043176438380L;

  public OrExpression(Expression left, Expression right) {
    super(left, right);
  }

  @Override public ExpressionResult evaluate(RowIntf value)
      throws FilterIllegalMemberException, FilterUnsupportedException {
    ExpressionResult resultLeft = left.evaluate(value);
    ExpressionResult resultRight = right.evaluate(value);
    switch (resultLeft.getDataType()) {
      case BooleanType:
        resultLeft.set(DataType.BooleanType, (resultLeft.getBoolean() || resultRight.getBoolean()));
        break;
      default:
        throw new FilterUnsupportedException(
            "Incompatible datatype for applying OR Expression Filter");
    }

    return resultLeft;
  }

  @Override public ExpressionType getFilterExpressionType() {
    return ExpressionType.OR;
  }

  @Override public String getString() {
    return "Or(" + left.getString() + ',' + right.getString() + ')';
  }

}
