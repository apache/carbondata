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

package org.carbondata.query.expression.logical;

import org.carbondata.query.carbonfilterinterface.ExpressionType;
import org.carbondata.query.carbonfilterinterface.RowIntf;
import org.carbondata.query.expression.DataType;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.ExpressionResult;
import org.carbondata.query.expression.UnaryExpression;
import org.carbondata.query.expression.exception.FilterIllegalMemberException;
import org.carbondata.query.expression.exception.FilterUnsupportedException;

public class NotExpression extends UnaryExpression {
  private static final long serialVersionUID = 1L;

  public NotExpression(Expression child) {
    super(child);
  }

  @Override public ExpressionResult evaluate(RowIntf value)
      throws FilterIllegalMemberException, FilterUnsupportedException {
    ExpressionResult expResult = child.evaluate(value);
    expResult.set(DataType.BooleanType, !(expResult.getBoolean()));
    switch (expResult.getDataType()) {
      case BooleanType:
        expResult.set(DataType.BooleanType, !(expResult.getBoolean()));
        break;
      default:
        throw new FilterUnsupportedException(
            "Incompatible datatype for applying NOT Expression Filter");
    }
    return expResult;
  }

  @Override public ExpressionType getFilterExpressionType() {
    return ExpressionType.NOT;
  }

  @Override public String getString() {
    return "Not(" + child.getString() + ')';
  }
}
