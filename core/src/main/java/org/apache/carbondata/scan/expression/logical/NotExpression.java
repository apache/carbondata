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

package org.apache.carbondata.scan.expression.logical;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.scan.expression.Expression;
import org.apache.carbondata.scan.expression.ExpressionResult;
import org.apache.carbondata.scan.expression.UnaryExpression;
import org.apache.carbondata.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.scan.filter.intf.ExpressionType;
import org.apache.carbondata.scan.filter.intf.RowIntf;

public class NotExpression extends UnaryExpression {
  private static final long serialVersionUID = 1L;

  public NotExpression(Expression child) {
    super(child);
  }

  @Override public ExpressionResult evaluate(RowIntf value)
      throws FilterIllegalMemberException, FilterUnsupportedException {
    ExpressionResult expResult = child.evaluate(value);
    expResult.set(DataType.BOOLEAN, !(expResult.getBoolean()));
    switch (expResult.getDataType()) {
      case BOOLEAN:
        expResult.set(DataType.BOOLEAN, !(expResult.getBoolean()));
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
