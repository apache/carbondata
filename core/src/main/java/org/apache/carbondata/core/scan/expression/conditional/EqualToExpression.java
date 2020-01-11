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
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;

public class EqualToExpression extends BinaryConditionalExpression {

  private static final long serialVersionUID = 1L;

  public EqualToExpression(Expression left, Expression right) {
    super(left, right);
  }

  public EqualToExpression(Expression left, Expression right, boolean isNull) {
    super(left, right);
    this.isNull = isNull;
  }

  @Override
  public ExpressionResult evaluate(RowIntf value)
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ExpressionResult elRes = left.evaluate(value);
    ExpressionResult erRes = right.evaluate(value);

    boolean result = false;

    ExpressionResult val1 = elRes;
    ExpressionResult val2 = erRes;

    if (elRes.isNull() || erRes.isNull()) {
      if (isNull) {
        elRes.set(DataTypes.BOOLEAN, elRes.isNull() == erRes.isNull());
      } else {
        elRes.set(DataTypes.BOOLEAN, false);
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

    DataType dataType = val1.getDataType();
    if (dataType == DataTypes.BOOLEAN) {
      result = val1.getBoolean().equals(val2.getBoolean());
    } else if (dataType == DataTypes.STRING) {
      result = val1.getString().equals(val2.getString());
    } else if (dataType == DataTypes.SHORT) {
      result = val1.getShort().equals(val2.getShort());
    } else if (dataType == DataTypes.INT) {
      result = val1.getInt().equals(val2.getInt());
    } else if (dataType == DataTypes.DOUBLE) {
      result = FilterUtil.nanSafeEqualsDoubles(val1.getDouble(), val2.getDouble());
    } else if (dataType == DataTypes.DATE) {
      result = val1.getTime().equals(val2.getTime());
    } else if (dataType == DataTypes.TIMESTAMP) {
      result = val1.getTimeAsMillisecond().equals(val2.getTimeAsMillisecond());
    } else if (dataType == DataTypes.LONG) {
      result = val1.getLong().equals(val2.getLong());
    } else if (DataTypes.isDecimal(dataType)) {
      result = val1.getDecimal().compareTo(val2.getDecimal()) == 0;
    } else {
      throw new FilterUnsupportedException(
          "DataType: " + val1.getDataType() + " not supported for the filter expression");
    }
    val1.set(DataTypes.BOOLEAN, result);
    return val1;
  }

  @Override
  public ExpressionType getFilterExpressionType() {
    return ExpressionType.EQUALS;
  }

  @Override
  public String getString() {
    return "EqualTo(" + left.getString() + ',' + right.getString() + ')';
  }

  @Override
  public String getStatement() {
    if (isNull) {
      return  left.getStatement() + " is " + right.getStatement();
    } else {
      return  left.getStatement() + " = " + right.getStatement();
    }
  }
}
