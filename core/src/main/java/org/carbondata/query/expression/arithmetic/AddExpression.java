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

package org.carbondata.query.expression.arithmetic;

import org.carbondata.query.carbonfilterinterface.ExpressionType;
import org.carbondata.query.carbonfilterinterface.RowIntf;
import org.carbondata.query.expression.DataType;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.ExpressionResult;
import org.carbondata.query.expression.exception.FilterIllegalMemberException;
import org.carbondata.query.expression.exception.FilterUnsupportedException;

public class AddExpression extends BinaryArithmeticExpression {
  private static final long serialVersionUID = 7999436055420911612L;

  public AddExpression(Expression left, Expression right) {
    super(left, right);
  }

  @Override public ExpressionResult evaluate(RowIntf value)
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ExpressionResult addExprLeftRes = left.evaluate(value);
    ExpressionResult addExprRightRes = right.evaluate(value);
    ExpressionResult val1 = addExprLeftRes;
    ExpressionResult val2 = addExprRightRes;
    if (addExprLeftRes.isNull() || addExprRightRes.isNull()) {
      addExprLeftRes.set(addExprLeftRes.getDataType(), null);
      return addExprLeftRes;
    }

    if (addExprLeftRes.getDataType() != addExprRightRes.getDataType()) {
      if (addExprLeftRes.getDataType().getPresedenceOrder() < addExprRightRes.getDataType()
          .getPresedenceOrder()) {
        val2 = addExprLeftRes;
        val1 = addExprRightRes;
      }
    }
    switch (val1.getDataType()) {
      case StringType:
      case DoubleType:
        addExprRightRes.set(DataType.DoubleType, val1.getDouble() + val2.getDouble());
        break;
      case IntegerType:
        addExprRightRes.set(DataType.IntegerType, val1.getInt() + val2.getInt());
        break;
      case LongType:
        addExprRightRes.set(DataType.LongType, val1.getLong() + val2.getLong());
        break;
      case DecimalType:
        addExprRightRes.set(DataType.DecimalType, val1.getDecimal().add(val2.getDecimal()));
        break;
      default:
        throw new FilterUnsupportedException(
            "Incompatible datatype for applying Add Expression Filter " + val1.getDataType());
    }
    return addExprRightRes;
  }

  @Override public ExpressionType getFilterExpressionType() {
    return ExpressionType.ADD;
  }

  @Override public String getString() {
    return "Add(" + left.getString() + ',' + right.getString() + ',';
  }
}
