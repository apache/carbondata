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
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;

public class GreaterThanExpression extends BinaryConditionalExpression {
  private static final long serialVersionUID = -5319109756575539219L;

  public GreaterThanExpression(Expression left, Expression right) {
    super(left, right);
  }

  @Override
  public ExpressionResult evaluate(RowIntf value)
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ExpressionResult exprLeftRes = left.evaluate(value);
    ExpressionResult exprRightRes = right.evaluate(value);
    ExpressionResult val1 = exprLeftRes;
    if (exprLeftRes.isNull() || exprRightRes.isNull()) {
      exprLeftRes.set(DataTypes.BOOLEAN, false);
      return exprLeftRes;
    }
    if (exprLeftRes.getDataType() != exprRightRes.getDataType()) {
      if (exprLeftRes.getDataType().getPrecedenceOrder() < exprRightRes.getDataType()
          .getPrecedenceOrder()) {
        val1 = exprRightRes;
      }

    }
    boolean result = false;
    DataType dataType = val1.getDataType();
    if (dataType == DataTypes.BOOLEAN) {
      result = exprLeftRes.getBoolean().compareTo(exprRightRes.getBoolean()) > 0;
    } else if (dataType == DataTypes.STRING || dataType == DataTypes.VARCHAR) {
      result = exprLeftRes.getString().compareTo(exprRightRes.getString()) > 0;
    } else if (dataType == DataTypes.DOUBLE) {
      result = exprLeftRes.getDouble() > (exprRightRes.getDouble());
    } else if (dataType == DataTypes.SHORT) {
      result = exprLeftRes.getShort() > (exprRightRes.getShort());
    } else if (dataType == DataTypes.INT) {
      result = exprLeftRes.getInt() > (exprRightRes.getInt());
    } else if (dataType == DataTypes.DATE) {
      result = exprLeftRes.getTime() > (exprRightRes.getTime());
    } else if (dataType == DataTypes.TIMESTAMP) {
      result = exprLeftRes.getTimeAsMillisecond() > (exprRightRes.getTimeAsMillisecond());
    } else if (dataType == DataTypes.LONG) {
      result = exprLeftRes.getLong() > (exprRightRes.getLong());
    } else if (DataTypes.isDecimal(dataType)) {
      result = exprLeftRes.getDecimal().compareTo(exprRightRes.getDecimal()) > 0;
    } else {
      throw new FilterUnsupportedException(
          "DataType: " + val1.getDataType() + " not supported for the filter expression");
    }
    val1.set(DataTypes.BOOLEAN, result);
    return val1;
  }

  @Override
  public ExpressionType getFilterExpressionType() {
    return ExpressionType.GREATERTHAN;
  }

  @Override
  public String getString() {
    return "GreaterThan(" + left.getString() + ',' + right.getString() + ')';
  }

  @Override
  public String getStatement() {
    return left.getStatement() + " > " + right.getStatement();
  }
}
