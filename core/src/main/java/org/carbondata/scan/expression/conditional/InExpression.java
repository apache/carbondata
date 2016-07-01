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

import java.util.HashSet;
import java.util.Set;

import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.scan.expression.Expression;
import org.carbondata.scan.expression.ExpressionResult;
import org.carbondata.scan.expression.exception.FilterIllegalMemberException;
import org.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.carbondata.scan.filter.intf.ExpressionType;
import org.carbondata.scan.filter.intf.RowIntf;

public class InExpression extends BinaryConditionalExpression {
  private static final long serialVersionUID = -3149927446694175489L;

  protected transient Set<ExpressionResult> setOfExprResult;

  public InExpression(Expression left, Expression right) {
    super(left, right);
  }

  @Override public ExpressionResult evaluate(RowIntf value)
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ExpressionResult leftRsult = left.evaluate(value);

    if (setOfExprResult == null) {
      ExpressionResult rightRsult = right.evaluate(value);
      ExpressionResult val = null;
      setOfExprResult = new HashSet<ExpressionResult>(10);
      for (ExpressionResult expressionResVal : rightRsult.getList()) {

        if (leftRsult.getDataType().name().equals(expressionResVal.getDataType().name())) {
          if (expressionResVal.getDataType().getPresedenceOrder() < leftRsult.getDataType()
              .getPresedenceOrder()) {
            val = leftRsult;
          } else {
            val = expressionResVal;
          }
          switch (val.getDataType()) {
            case STRING:
              val = new ExpressionResult(val.getDataType(), expressionResVal.getString());
              break;
            case SHORT:
              val = new ExpressionResult(val.getDataType(), expressionResVal.getShort());
              break;
            case INT:
              val = new ExpressionResult(val.getDataType(), expressionResVal.getInt());
              break;
            case DOUBLE:
              val = new ExpressionResult(val.getDataType(), expressionResVal.getDouble());
              break;
            case TIMESTAMP:
              val = new ExpressionResult(val.getDataType(), expressionResVal.getTime());
              break;
            case LONG:
              val = new ExpressionResult(val.getDataType(), expressionResVal.getLong());
              break;
            case DECIMAL:
              val = new ExpressionResult(val.getDataType(), expressionResVal.getDecimal());
              break;
            default:
              throw new FilterUnsupportedException(
                  "DataType: " + val.getDataType() + " not supported for the filter expression");
          }
        }
        setOfExprResult.add(val);

      }
    }
    leftRsult.set(DataType.BOOLEAN, setOfExprResult.contains(leftRsult));
    return leftRsult;
  }

  @Override public ExpressionType getFilterExpressionType() {
    return ExpressionType.IN;
  }

  @Override public String getString() {
    return "IN(" + left.getString() + ',' + right.getString() + ')';
  }

}
