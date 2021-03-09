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

package org.apache.carbondata.core.scan.expression;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.util.CarbonUtil;

public class LiteralExpression extends LeafExpression {

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private Object value;
  private DataType dataType;

  public LiteralExpression(Object value, DataType dataType) {
    this.value = value;
    this.dataType = dataType;
  }

  @Override
  public ExpressionResult evaluate(RowIntf value) {
    return new ExpressionResult(dataType, this.value, true);
  }

  public ExpressionResult getExpressionResult() {
    return new ExpressionResult(dataType, this.value, true);
  }

  @Override
  public ExpressionType getFilterExpressionType() {
    // TODO Auto-generated method stub
    return ExpressionType.LITERAL;
  }

  @Override
  public String getString() {
    // TODO Auto-generated method stub
    return "LiteralExpression(" + value + ')';
  }

  @Override
  public String getStatement() {
    boolean quoteString = false;
    Object val = value;
    if (val != null) {
      if (dataType == DataTypes.STRING || val instanceof String) {
        quoteString = true;
      } else if (dataType == DataTypes.TIMESTAMP || dataType == DataTypes.DATE) {
        val = CarbonUtil.getFormattedDateOrTimestamp(dataType == DataTypes.TIMESTAMP ?
            CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT :
            CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT, dataType, value, true);
        quoteString = true;
      }
    }
    return val == null ? null : quoteString ? "'" + val.toString() + "'" : val.toString();
  }

  /**
   * getLiteralExpDataType.
   *
   * @return
   */
  public DataType getLiteralExpDataType() {
    return dataType;
  }

  public Object getLiteralExpValue() {
    return value;
  }

  @Override
  public void findAndSetChild(Expression oldExpr, Expression newExpr) {
  }

}
