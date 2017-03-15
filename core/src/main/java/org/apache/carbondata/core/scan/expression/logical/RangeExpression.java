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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.BinaryConditionalExpression;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;

public class RangeExpression extends BinaryConditionalExpression {

  private static final long serialVersionUID = 1L;

  public RangeExpression(Expression left, Expression right) {
    super(left, right);
  }

  @Override public ExpressionResult evaluate(RowIntf value)
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ExpressionResult resultLeft = left.evaluate(value);
    ExpressionResult resultRight = right.evaluate(value);
    switch (resultLeft.getDataType()) {
      case BOOLEAN:
        resultLeft.set(DataType.BOOLEAN, (resultLeft.getBoolean() && resultRight.getBoolean()));
        break;
      default:
        throw new FilterUnsupportedException(
            "Incompatible datatype for applying RANGE Expression Filter");
    }
    return resultLeft;
  }

  @Override public ExpressionType getFilterExpressionType() {
    return ExpressionType.RANGE;
  }

  @Override public String getString() {
    return null;
  }

  @Override public List<ExpressionResult> getLiterals() {
    List<ExpressionResult> listOfExp =
        new ArrayList<ExpressionResult>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    getLiteralsResult(this, listOfExp);
    Collections.sort(listOfExp);
    return listOfExp;
  }

  private void getLiteralsResult(Expression expression, List<ExpressionResult> listOfExp) {
    for (Expression child : expression.getChildren()) {
      if (child instanceof LiteralExpression) {
        ExpressionResult colExp = ((LiteralExpression) child).getExpressionResult();
        listOfExp.add(colExp);
      } else {
        getLiteralsResult(child, listOfExp);
      }
    }
  }
}
