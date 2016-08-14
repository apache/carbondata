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
      case STRING:
      case DOUBLE:
        subtractExprRightRes.set(DataType.DOUBLE, val1.getDouble() - val2.getDouble());
        break;
      case SHORT:
        subtractExprRightRes.set(DataType.SHORT, val1.getShort() - val2.getShort());
        break;
      case INT:
        subtractExprRightRes.set(DataType.INT, val1.getInt() - val2.getInt());
        break;
      case LONG:
        subtractExprRightRes.set(DataType.LONG, val1.getLong() - val2.getLong());
        break;
      case DECIMAL:
        subtractExprRightRes
            .set(DataType.DECIMAL, val1.getDecimal().subtract(val2.getDecimal()));
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
