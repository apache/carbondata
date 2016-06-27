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

public class SubstractExpression extends BinaryArithmeticExpression {

  private static final long serialVersionUID = -8304726440185363102L;

  public SubstractExpression(Expression left, Expression right) {
    super(left, right);
  }

  @Override public ExpressionResult evaluate(RowIntf value)
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ExpressionResult subtractExprLeftRes = left.evaluate(value);
    ExpressionResult subtractExprRightRes = right.evaluate(value);
    ExpressionResult val1 = subtractExprLeftRes;
    ExpressionResult val2 = subtractExprRightRes;
    if (subtractExprLeftRes.isNull() || subtractExprRightRes.isNull()) {
      subtractExprLeftRes.set(subtractExprLeftRes.getDataType(), null);
      return subtractExprLeftRes;
    }
    if (subtractExprLeftRes.getDataType() != subtractExprRightRes.getDataType()) {
      if (subtractExprLeftRes.getDataType().getPresedenceOrder() < subtractExprRightRes
          .getDataType().getPresedenceOrder()) {
        val2 = subtractExprLeftRes;
        val1 = subtractExprRightRes;
      }
    }
    switch (val1.getDataType()) {
      case StringType:
      case DoubleType:
        subtractExprRightRes.set(DataType.DoubleType, val1.getDouble() - val2.getDouble());
        break;
      case IntegerType:
        subtractExprRightRes.set(DataType.IntegerType, val1.getInt() - val2.getInt());
        break;
      case LongType:
        subtractExprRightRes.set(DataType.LongType, val1.getLong() - val2.getLong());
        break;
      case DecimalType:
        subtractExprRightRes
            .set(DataType.DecimalType, val1.getDecimal().subtract(val2.getDecimal()));
        break;
      default:
        throw new FilterUnsupportedException(
            "Incompatible datatype for applying Add Expression Filter " + subtractExprLeftRes
                .getDataType());
    }
    return subtractExprRightRes;
  }

  @Override public ExpressionType getFilterExpressionType() {
    return ExpressionType.SUBSTRACT;
  }

  @Override public String getString() {
    return "Substract(" + left.getString() + ',' + right.getString() + ')';
  }
}
