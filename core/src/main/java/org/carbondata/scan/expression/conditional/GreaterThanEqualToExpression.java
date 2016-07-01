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

package org.carbondata.scan.expression.conditional;

import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.scan.expression.Expression;
import org.carbondata.scan.expression.ExpressionResult;
import org.carbondata.scan.expression.exception.FilterIllegalMemberException;
import org.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.carbondata.scan.filter.intf.ExpressionType;
import org.carbondata.scan.filter.intf.RowIntf;

public class GreaterThanEqualToExpression extends BinaryConditionalExpression {
  private static final long serialVersionUID = 4185317066280688984L;

  public GreaterThanEqualToExpression(Expression left, Expression right) {
    super(left, right);
  }

  public ExpressionResult evaluate(RowIntf value)
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ExpressionResult elRes = left.evaluate(value);
    ExpressionResult erRes = right.evaluate(value);
    ExpressionResult exprResVal1 = elRes;
    if (elRes.isNull() || erRes.isNull()) {
      elRes.set(DataType.BOOLEAN, false);
      return elRes;
    }
    if (elRes.getDataType() != erRes.getDataType()) {
      if (elRes.getDataType().getPresedenceOrder() < erRes.getDataType().getPresedenceOrder()) {
        exprResVal1 = erRes;
      }

    }
    boolean result = false;
    switch (exprResVal1.getDataType()) {
      case STRING:
        result = elRes.getString().compareTo(erRes.getString()) >= 0;
        break;
      case SHORT:
        result = elRes.getShort() >= (erRes.getShort());
        break;
      case INT:
        result = elRes.getInt() >= (erRes.getInt());
        break;
      case DOUBLE:
        result = elRes.getDouble() >= (erRes.getDouble());
        break;
      case TIMESTAMP:
        result = elRes.getTime() >= (erRes.getTime());
        break;
      case LONG:
        result = elRes.getLong() >= (erRes.getLong());
        break;
      case DECIMAL:
        result = elRes.getDecimal().compareTo(erRes.getDecimal()) >= 0;
        break;
      default:
        throw new FilterUnsupportedException(
            "DataType: " + exprResVal1.getDataType() + " not supported for the filter expression");
    }
    exprResVal1.set(DataType.BOOLEAN, result);
    return exprResVal1;
  }

  @Override public ExpressionType getFilterExpressionType() {
    return ExpressionType.GREATERTHAN_EQUALTO;
  }

  @Override public String getString() {
    return "GreaterThanEqualTo(" + left.getString() + ',' + right.getString() + ')';
  }
}
