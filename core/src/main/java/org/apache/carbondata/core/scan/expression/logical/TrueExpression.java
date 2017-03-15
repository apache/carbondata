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

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.BinaryConditionalExpression;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;

/**
 * This class will form an expression whose evaluation will be always true.
 */
public class TrueExpression extends BinaryConditionalExpression {


  private static final long serialVersionUID = -8390184061336799370L;

  public TrueExpression(Expression child1) {
    super(child1, new LiteralExpression(null,null));
  }

  /**
   * This method will always return false, mainly used in the filter expressions
   * which are illogical.
   * eg: columnName NOT IN('Java',NULL)
   * @param value
   * @return
   * @throws FilterUnsupportedException
   * @throws FilterIllegalMemberException
   */
  @Override public ExpressionResult evaluate(RowIntf value)
      throws FilterUnsupportedException, FilterIllegalMemberException {
    return new ExpressionResult(DataType.BOOLEAN,true);
  }

  /**
   * This method will return the expression types
   * @return
   */
  @Override public ExpressionType getFilterExpressionType() {
    return ExpressionType.TRUE;
  }
  @Override public String getString() {
    return null;
  }
}
