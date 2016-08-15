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

package org.apache.carbondata.scan.expression.arithmetic;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.scan.expression.Expression;
import org.apache.carbondata.scan.expression.ExpressionResult;
import org.apache.carbondata.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.scan.filter.intf.ExpressionType;
import org.apache.carbondata.scan.filter.intf.RowIntf;

public class DivideExpression extends BinaryArithmeticExpression {
  private static final long serialVersionUID = -7269266926782365612L;

  public DivideExpression(Expression left, Expression right) {
    super(left, right);
  }

  @Override public ExpressionResult evaluate(RowIntf value)
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ExpressionResult divideExprLeftRes = left.evaluate(value);
    ExpressionResult divideExprRightRes = right.evaluate(value);
    ExpressionResult val1 = divideExprLeftRes;
    ExpressionResult val2 = divideExprRightRes;
    if (divideExprLeftRes.isNull() || divideExprRightRes.isNull()) {
      divideExprLeftRes.set(divideExprLeftRes.getDataType(), null);
      return divideExprLeftRes;
    }
    if (divideExprLeftRes.getDataType() != divideExprRightRes.getDataType()) {
      if (divideExprLeftRes.getDataType().getPresedenceOrder() < divideExprRightRes.getDataType()
          .getPresedenceOrder()) {
        val2 = divideExprLeftRes;
        val1 = divideExprRightRes;
      }
    }
    switch (val1.getDataType()) {
      case STRING:
      case DOUBLE:
        divideExprRightRes.set(DataType.DOUBLE, val1.getDouble() / val2.getDouble());
        break;
      case SHORT:
        divideExprRightRes.set(DataType.SHORT, val1.getShort() / val2.getShort());
        break;
      case INT:
        divideExprRightRes.set(DataType.INT, val1.getInt() / val2.getInt());
        break;
      case LONG:
        divideExprRightRes.set(DataType.LONG, val1.getLong() / val2.getLong());
        break;
      case DECIMAL:
        divideExprRightRes.set(DataType.DECIMAL, val1.getDecimal().divide(val2.getDecimal()));
        break;
      default:
        throw new FilterUnsupportedException(
            "Incompatible datatype for applying Add Expression Filter " + divideExprLeftRes
                .getDataType());
    }
    return divideExprRightRes;
  }

  @Override public ExpressionType getFilterExpressionType() {
    return ExpressionType.DIVIDE;
  }

  @Override public String getString() {
    return "Divide(" + left.getString() + ',' + right.getString() + ')';
  }
}
