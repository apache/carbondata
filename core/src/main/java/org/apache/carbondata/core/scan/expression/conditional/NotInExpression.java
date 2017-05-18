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

import java.util.HashSet;
import java.util.Set;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;

public class NotInExpression extends BinaryConditionalExpression {
  private static final long serialVersionUID = -6835841923752118034L;
  protected transient Set<ExpressionResult> setOfExprResult;

  public NotInExpression(Expression left, Expression right) {
    super(left, right);
  }

  @Override public ExpressionResult evaluate(RowIntf value)
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ExpressionResult leftRsult = left.evaluate(value);
    if (setOfExprResult == null) {
      ExpressionResult val = null;
      ExpressionResult rightRsult = right.evaluate(value);
      // Both left and right result need to be checked for null because NotInExpression is basically
      // an And Operation on the list of predicates that are provided.
      // Example: x in (1,2,null) would be converted to x=1 AND x=2 AND x=null.
      // If any of the predicates is null then the result is unknown for all the predicates thus
      // we will return false for each of them.
      for (ExpressionResult expressionResult: rightRsult.getList()) {
        if (expressionResult.isNull() || leftRsult.isNull()) {
          leftRsult.set(DataType.BOOLEAN, false);
          return leftRsult;
        }
      }
      setOfExprResult = new HashSet<ExpressionResult>(10);
      for (ExpressionResult exprResVal : rightRsult.getList()) {
        if (exprResVal.getDataType().getPrecedenceOrder() < leftRsult.getDataType()
            .getPrecedenceOrder()) {
          val = leftRsult;
        } else {
          val = exprResVal;
        }
        switch (val.getDataType()) {
          case STRING:
            val = new ExpressionResult(val.getDataType(), exprResVal.getString());
            break;
          case SHORT:
            val = new ExpressionResult(val.getDataType(), exprResVal.getShort());
            break;
          case INT:
            val = new ExpressionResult(val.getDataType(), exprResVal.getInt());
            break;
          case DOUBLE:
            val = new ExpressionResult(val.getDataType(), exprResVal.getDouble());
            break;
          case DATE:
          case TIMESTAMP:
            val = new ExpressionResult(val.getDataType(), exprResVal.getTime());
            break;
          case LONG:
            val = new ExpressionResult(val.getDataType(), exprResVal.getLong());
            break;
          case DECIMAL:
            val = new ExpressionResult(val.getDataType(), exprResVal.getDecimal());
            break;
          default:
            throw new FilterUnsupportedException(
                "DataType: " + val.getDataType() + " not supported for the filter expression");
        }
        setOfExprResult.add(val);
      }
    }
    leftRsult.set(DataType.BOOLEAN, !setOfExprResult.contains(leftRsult));
    return leftRsult;
  }

  @Override public ExpressionType getFilterExpressionType() {
    return ExpressionType.NOT_IN;
  }

  @Override public String getString() {
    return "NOT IN(" + left.getString() + ',' + right.getString() + ')';
  }

}
