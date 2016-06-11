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

public class LessThanEqualToExpression extends BinaryConditionalExpression {
  private static final long serialVersionUID = 1L;

  public LessThanEqualToExpression(Expression left, Expression right) {
    super(left, right);
    // TODO Auto-generated constructor stub
  }

  public ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException {
    ExpressionResult elRes = left.evaluate(value);
    ExpressionResult erRes = right.evaluate(value);
    ExpressionResult exprResValue1 = elRes;
    if (elRes.isNull() || erRes.isNull()) {
      elRes.set(DataType.BooleanType, false);
      return elRes;
    }
    if (elRes.getDataType() != erRes.getDataType()) {
      if (elRes.getDataType().getPresedenceOrder() < erRes.getDataType().getPresedenceOrder()) {
        exprResValue1 = erRes;
      }

    }
    boolean result = false;
    switch (exprResValue1.getDataType()) {
      case StringType:
        result = elRes.getString().compareTo(erRes.getString()) <= 0;
        break;
      case IntegerType:
        result = elRes.getInt() <= (erRes.getInt());
        break;
      case DoubleType:
        result = elRes.getDouble() <= (erRes.getDouble());
        break;
      case TimestampType:
        result = elRes.getTime() <= (erRes.getTime());
        break;
      case LongType:
        result = elRes.getLong() <= (erRes.getLong());
        break;
      case DecimalType:
        result = elRes.getDecimal().compareTo(erRes.getDecimal()) <= 0;
        break;
      default:
        throw new FilterUnsupportedException("DataType: " + exprResValue1.getDataType()
            + " not supported for the filter expression");
    }
    exprResValue1.set(DataType.BooleanType, result);
    return exprResValue1;
  }

  @Override public ExpressionType getFilterExpressionType() {
    // TODO Auto-generated method stub
    return ExpressionType.LESSTHAN_EQUALTO;
  }

  @Override public String getString() {
    return "LessThanEqualTo(" + left.getString() + ',' + right.getString() + ')';
  }

}
