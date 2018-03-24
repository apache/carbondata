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

package org.apache.carbondata.core.scan.expression.logical;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;

public class OrExpression extends BinaryLogicalExpression {

  private static final long serialVersionUID = 4220598043176438380L;

  public OrExpression(Expression left, Expression right) {
    super(left, right);
  }

  @Override
  public ExpressionResult evaluate(RowIntf value)
      throws FilterIllegalMemberException, FilterUnsupportedException {
    ExpressionResult resultLeft = left.evaluate(value);
    ExpressionResult resultRight = right.evaluate(value);
    if (resultLeft.getDataType() == DataTypes.BOOLEAN) {
      resultLeft.set(DataTypes.BOOLEAN, (resultLeft.getBoolean() || resultRight.getBoolean()));
    } else {
      throw new FilterUnsupportedException(
          "Incompatible datatype for applying OR Expression Filter");
    }
    return resultLeft;
  }

  @Override
  public ExpressionType getFilterExpressionType() {
    return ExpressionType.OR;
  }

  @Override
  public String getString() {
    return "Or(" + left.getString() + ',' + right.getString() + ')';
  }

  @Override
  public String getStatement() {
    return "(" + left.getString() + " or " + right.getString() + ")";
  }
}
