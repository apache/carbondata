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

package org.apache.carbondata.scan.expression.conditional;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.scan.expression.Expression;
import org.apache.carbondata.scan.expression.ExpressionResult;
import org.apache.carbondata.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.scan.filter.intf.ExpressionType;
import org.apache.carbondata.scan.filter.intf.RowIntf;

public class ContainsExpression extends BinaryConditionalExpression {

  private static final long serialVersionUID = 1L;

  public ContainsExpression(Expression left, Expression right) {
    super(left, right);
  }

  public ContainsExpression(Expression left, Expression right, boolean isNull) {
    super(left, right);
    this.isNull = isNull;
  }

  @Override public ExpressionResult evaluate(RowIntf value)
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ExpressionResult elRes = left.evaluate(value);
    ExpressionResult erRes = right.evaluate(value);

    boolean result = false;

    ExpressionResult val1 = elRes;
    ExpressionResult val2 = erRes;

    if (elRes.isNull() || erRes.isNull()) {
      if (isNull) {
        elRes.set(DataType.BOOLEAN, elRes.isNull() == erRes.isNull());
      } else {
        elRes.set(DataType.BOOLEAN, false);
      }
      return elRes;
    }
    //default implementation if the data types are different for the resultsets
    if (elRes.getDataType() != erRes.getDataType()) {
      if (elRes.getDataType().getPrecedenceOrder() < erRes.getDataType().getPrecedenceOrder()) {
        val2 = elRes;
        val1 = erRes;
      }
    }

    switch (val1.getDataType()) {
      case STRING:
        result = val1.getString().contains(val2.getString());
        break;
      case SHORT:
        result = val1.getShort().toString().contains(val2.getShort().toString());
        break;
      case INT:
        result = val1.getInt().toString().contains(val2.getInt().toString());
        break;
      case DOUBLE:
        result = val1.getDouble().toString().contains(val2.getDouble().toString());
        break;
      case TIMESTAMP:
        result = val1.getTime().toString().contains(val2.getTime().toString());
        break;
      case LONG:
        result = val1.getLong().toString().contains(val2.getLong().toString());
        break;
      case DECIMAL:
        result = val1.getDecimal().toEngineeringString()
         .contains(val2.getDecimal().toEngineeringString());
        break;
      default:
        throw new FilterUnsupportedException(
            "DataType: " + val1.getDataType() + " not supported for the filter expression");
    }
    val1.set(DataType.BOOLEAN, result);
    return val1;
  }

  @Override public ExpressionType getFilterExpressionType() {
    return ExpressionType.CONTAINS;
  }

  @Override public String getString() {
    return "Contains(" + left.getString() + ',' + right.getString() + ')';
  }

}
