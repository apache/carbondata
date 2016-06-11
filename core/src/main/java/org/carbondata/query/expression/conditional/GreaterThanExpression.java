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

import org.carbondata.query.carbonfilterinterface.ExpressionType;
import org.carbondata.query.carbonfilterinterface.RowIntf;
import org.carbondata.query.expression.DataType;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.ExpressionResult;
import org.carbondata.query.expression.exception.FilterUnsupportedException;

public class GreaterThanExpression extends BinaryConditionalExpression {
  private static final long serialVersionUID = -5319109756575539219L;

  public GreaterThanExpression(Expression left, Expression right) {
    super(left, right);
    // TODO Auto-generated constructor stub
  }

  @Override public ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException {
    ExpressionResult exprLeftRes = left.evaluate(value);
    ExpressionResult exprRightRes = right.evaluate(value);
    ExpressionResult val1 = exprLeftRes;
    if (exprLeftRes.isNull() || exprRightRes.isNull()) {
      exprLeftRes.set(DataType.BooleanType, false);
      return exprLeftRes;
    }
    if (exprLeftRes.getDataType() != exprRightRes.getDataType()) {
      if (exprLeftRes.getDataType().getPresedenceOrder() < exprRightRes.getDataType()
          .getPresedenceOrder()) {
        val1 = exprRightRes;
      }

    }
    boolean result = false;
    switch (val1.getDataType()) {
      case StringType:
        result = exprLeftRes.getString().compareTo(exprRightRes.getString()) > 0;
        break;
      case DoubleType:
        result = exprLeftRes.getDouble() > (exprRightRes.getDouble());
        break;
      case IntegerType:
        result = exprLeftRes.getInt() > (exprRightRes.getInt());
        break;
      case TimestampType:
        result = exprLeftRes.getTime() > (exprRightRes.getTime());
        break;
      case LongType:
        result = exprLeftRes.getLong() > (exprRightRes.getLong());
        break;
      case DecimalType:
        result = exprLeftRes.getDecimal().compareTo(exprRightRes.getDecimal()) > 0;
        break;
      default:
        throw new FilterUnsupportedException(
            "DataType: " + val1.getDataType() + " not supported for the filter expression");
    }
    val1.set(DataType.BooleanType, result);
    return val1;
  }

  @Override public ExpressionType getFilterExpressionType() {
    return ExpressionType.GREATERTHAN;
  }

  @Override public String getString() {
    return "GreaterThan(" + left.getString() + ',' + right.getString() + ')';
  }

}
