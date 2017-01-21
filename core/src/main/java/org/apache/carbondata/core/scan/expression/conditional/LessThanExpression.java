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

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;

public class LessThanExpression extends BinaryConditionalExpression {

  private static final long serialVersionUID = 6343040416663699924L;

  public LessThanExpression(Expression left, Expression right) {
    super(left, right);
  }

  public ExpressionResult evaluate(RowIntf value)
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ExpressionResult erRes = right.evaluate(value);
    ExpressionResult elRes = left.evaluate(value);

    ExpressionResult val1 = elRes;

    boolean result = false;

    if (elRes.isNull() || erRes.isNull()) {
      elRes.set(DataType.BOOLEAN, false);
      return elRes;
    }
    if (elRes.getDataType() != erRes.getDataType()) {
      if (elRes.getDataType().getPrecedenceOrder() < erRes.getDataType().getPrecedenceOrder()) {
        val1 = erRes;
      }

    }
    switch (val1.getDataType()) {
      case STRING:
        result = elRes.getString().compareTo(erRes.getString()) < 0;
        break;
      case SHORT:
        result = elRes.getShort() < (erRes.getShort());
        break;
      case INT:
        result = elRes.getInt() < (erRes.getInt());
        break;
      case DOUBLE:
        result = elRes.getDouble() < (erRes.getDouble());
        break;
      case DATE:
      case TIMESTAMP:
        result = elRes.getTime() < (erRes.getTime());
        break;
      case LONG:
        result = elRes.getLong() < (erRes.getLong());
        break;
      case DECIMAL:
        result = elRes.getDecimal().compareTo(erRes.getDecimal()) < 0;
        break;
      default:
        throw new FilterUnsupportedException(
            "DataType: " + val1.getDataType() + " not supported for the filter expression");
    }
    val1.set(DataType.BOOLEAN, result);
    return val1;
  }

  @Override public ExpressionType getFilterExpressionType() {
    return ExpressionType.LESSTHAN;
  }

  @Override public String getString() {
    return "LessThan(" + left.getString() + ',' + right.getString() + ')';
  }

}