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
import org.apache.carbondata.core.metadata.datatype.DataTypes;
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

  @Override
  public ExpressionResult evaluate(RowIntf value)
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
        DataType dataType = val.getDataType();
        if (dataType == DataTypes.BOOLEAN) {
          val = new ExpressionResult(val.getDataType(), expressionResVal.getBoolean());
        } else if (dataType == DataTypes.STRING) {
          val = new ExpressionResult(val.getDataType(), expressionResVal.getString());
        } else if (dataType == DataTypes.SHORT) {
          val = new ExpressionResult(val.getDataType(), expressionResVal.getShort());
        } else if (dataType == DataTypes.INT) {
          val = new ExpressionResult(val.getDataType(), expressionResVal.getInt());
        } else if (dataType == DataTypes.DOUBLE) {
          val = new ExpressionResult(val.getDataType(), expressionResVal.getDouble());
        } else if (dataType == DataTypes.LONG) {
          val = new ExpressionResult(val.getDataType(), expressionResVal.getLong());
        } else if (dataType == DataTypes.DATE) {
          val = new ExpressionResult(val.getDataType(), expressionResVal.getTime());
        } else if (dataType == DataTypes.TIMESTAMP) {
          val = new ExpressionResult(val.getDataType(), expressionResVal.getTimeAsMillisecond());
        } else if (DataTypes.isDecimal(dataType)) {
          val = new ExpressionResult(val.getDataType(), expressionResVal.getDecimal());
        } else {
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
      leftRsult.set(DataTypes.BOOLEAN, false);
    } else {
      leftRsult.set(DataTypes.BOOLEAN, setOfExprResult.contains(leftRsult));
    }
    return leftRsult;
  }

  @Override
  public ExpressionType getFilterExpressionType() {
    return ExpressionType.IN;
  }

  @Override
  public String getString() {
    return "IN(" + left.getString() + ',' + right.getString() + ')';
  }

  @Override
  public String getStatement() {
    return left.getStatement() + " in " + right.getStatement();
  }
}
