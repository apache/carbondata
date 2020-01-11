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

public class NotInExpression extends BinaryConditionalExpression {
  private static final long serialVersionUID = -6835841923752118034L;
  protected transient Set<ExpressionResult> setOfExprResult;
  protected transient ExpressionResult nullValuePresent = null;

  public NotInExpression(Expression left, Expression right) {
    super(left, right);
  }

  @Override
  public ExpressionResult evaluate(RowIntf value)
      throws FilterUnsupportedException, FilterIllegalMemberException {

    // Both left and right result need to be checked for null because NotInExpression is basically
    // an And Operation on the list of predicates that are provided.
    // Example: x in (1,2,null) would be converted to x=1 AND x=2 AND x=null.
    // If any of the predicates is null then the result is unknown for all the predicates thus
    // we will return false for each of them.
    if (nullValuePresent != null) {
      return nullValuePresent;
    }

    ExpressionResult leftRsult = left.evaluate(value);
    if (leftRsult.isNull()) {
      leftRsult.set(DataTypes.BOOLEAN, false);
      return leftRsult;
    }

    if (setOfExprResult == null) {
      ExpressionResult val = null;
      ExpressionResult rightRsult = right.evaluate(value);
      setOfExprResult = new HashSet<ExpressionResult>(10);
      for (ExpressionResult exprResVal : rightRsult.getList()) {

        if (exprResVal.isNull()) {
          nullValuePresent = new ExpressionResult(DataTypes.BOOLEAN, false);
          leftRsult.set(DataTypes.BOOLEAN, false);
          return leftRsult;
        }

        if (exprResVal.getDataType().getPrecedenceOrder() < leftRsult.getDataType()
            .getPrecedenceOrder()) {
          val = leftRsult;
        } else {
          val = exprResVal;
        }
        DataType dataType = val.getDataType();
        if (dataType == DataTypes.BOOLEAN) {
          val = new ExpressionResult(val.getDataType(), exprResVal.getBoolean());
        } else if (dataType == DataTypes.STRING) {
          val = new ExpressionResult(val.getDataType(), exprResVal.getString());
        } else if (dataType == DataTypes.SHORT) {
          val = new ExpressionResult(val.getDataType(), exprResVal.getShort());
        } else if (dataType == DataTypes.INT) {
          val = new ExpressionResult(val.getDataType(), exprResVal.getInt());
        } else if (dataType == DataTypes.DOUBLE) {
          val = new ExpressionResult(val.getDataType(), exprResVal.getDouble());
        } else if (dataType == DataTypes.DATE) {
          val = new ExpressionResult(val.getDataType(), exprResVal.getTime());
        } else if (dataType == DataTypes.TIMESTAMP) {
          val = new ExpressionResult(val.getDataType(), exprResVal.getTimeAsMillisecond());
        } else if (dataType == DataTypes.LONG) {
          val = new ExpressionResult(val.getDataType(), exprResVal.getLong());
        } else if (DataTypes.isDecimal(dataType)) {
          val = new ExpressionResult(val.getDataType(), exprResVal.getDecimal());
        } else {
          throw new FilterUnsupportedException(
              "DataType: " + val.getDataType() + " not supported for the filter expression");
        }

        setOfExprResult.add(val);
      }
    }

    leftRsult.set(DataTypes.BOOLEAN, !setOfExprResult.contains(leftRsult));
    return leftRsult;
  }

  @Override
  public ExpressionType getFilterExpressionType() {
    return ExpressionType.NOT_IN;
  }

  @Override
  public String getString() {
    return "NOT IN(" + left.getString() + ',' + right.getString() + ')';
  }

  @Override
  public String getStatement() {
    return left.getStatement() + " not in " + right.getStatement();
  }
}
