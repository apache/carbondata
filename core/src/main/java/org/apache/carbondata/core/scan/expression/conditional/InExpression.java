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
        if (expressionResVal.getDataType().getPrecedenceOrder() < leftRsult.getDataType()
            .getPrecedenceOrder()) {
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
          case LONG:
            val = new ExpressionResult(val.getDataType(), expressionResVal.getLong());
            break;
          case DATE:
          case TIMESTAMP:
            val = new ExpressionResult(val.getDataType(), expressionResVal.getTime());
            break;
          case DECIMAL:
            val = new ExpressionResult(val.getDataType(), expressionResVal.getDecimal());
            break;
          default:
            throw new FilterUnsupportedException(
                "DataType: " + val.getDataType() + " not supported for the filter expression");
        }
        setOfExprResult.add(val);
      }
    }
    // Only left result needs to be checked for null because InExpression is basically
    // an OR Operation on the list of predicates that are provided.
    // Example: x in (1,2,null) would be converted to x=1 OR x=2 OR x=null.
    // If any of the predicates is null then the result is unknown thus we will return false
    // for x=null.
    // Left check will cover both the cases when left and right is null therefore no need
    // for a check on the right result.
    // Example: (null==null) -> Left null return false, (1==null) would automatically be false.
    if (leftRsult.isNull()) {
      leftRsult.set(DataType.BOOLEAN, false);
    } else {
      leftRsult.set(DataType.BOOLEAN, setOfExprResult.contains(leftRsult));
    }
    return leftRsult;
  }

  @Override public ExpressionType getFilterExpressionType() {
    return ExpressionType.IN;
  }

  @Override public String getString() {
    return "IN(" + left.getString() + ',' + right.getString() + ')';
  }

}
