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

public class NotEqualsExpression extends BinaryConditionalExpression {

  private static final long serialVersionUID = 8684006025540863973L;
  private boolean isNotNull = false;
  public NotEqualsExpression(Expression left, Expression right, boolean isNotNull) {
    super(left, right);
    this.isNotNull = isNotNull;
  }

  public NotEqualsExpression(Expression left, Expression right) {
    super(left, right);
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
      if (isNotNull) {
        elRes.set(DataTypes.BOOLEAN, elRes.isNull() != erRes.isNull());
      } else {
        elRes.set(DataTypes.BOOLEAN, false);
      }
      return elRes;
    }
    //default implementation if the data types are different for the resultsets
    if (elRes.getDataType() != erRes.getDataType()) {
      if (elRes.getDataType().getPrecedenceOrder() < erRes.getDataType().getPrecedenceOrder()) {
        val1 = erRes;
        val2 = elRes;
      }
    }
    DataType dataType = val1.getDataType();
    if (dataType == DataTypes.BOOLEAN) {
      result = !val1.getBoolean().equals(val2.getBoolean());
    } else if (dataType == DataTypes.STRING) {
      result = !val1.getString().equals(val2.getString());
    } else if (dataType == DataTypes.SHORT) {
      result = val1.getShort().shortValue() != val2.getShort().shortValue();
    } else if (dataType == DataTypes.INT) {
      result = val1.getInt().intValue() != val2.getInt().intValue();
    } else if (dataType == DataTypes.DOUBLE) {
      result = val1.getDouble().doubleValue() != val2.getDouble().doubleValue();
    } else if (dataType == DataTypes.DATE) {
      result = val1.getTime().longValue() != val2.getTime().longValue();
    } else if (dataType == DataTypes.TIMESTAMP) {
      result = val1.getTimeAsMillisecond().longValue() != val2.getTimeAsMillisecond().longValue();
    } else if (dataType == DataTypes.LONG) {
      result = elRes.getLong().longValue() != (erRes.getLong()).longValue();
    } else if (DataTypes.isDecimal(dataType)) {
      result = elRes.getDecimal().compareTo(erRes.getDecimal()) != 0;
    } else {
      throw new FilterUnsupportedException(
          "DataType: " + val1.getDataType() + " not supported for the filter expression");
    }
    val1.set(DataTypes.BOOLEAN, result);
    return val1;
  }

  @Override
  public ExpressionType getFilterExpressionType() {
    return ExpressionType.NOT_EQUALS;
  }

  @Override
  public String getString() {
    return "NotEquals(" + left.getString() + ',' + right.getString() + ')';
  }

  @Override
  public String getStatement() {
    return left.getStatement() + " <> " + right.getStatement();
  }
}
