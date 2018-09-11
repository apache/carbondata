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
import org.apache.carbondata.core.scan.expression.BinaryExpression;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.LiteralExpression;

public abstract class BinaryLogicalExpression extends BinaryExpression {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  public BinaryLogicalExpression(Expression left, Expression right) {
    super(left, right);
    // TODO Auto-generated constructor stub
  }

  public List<ExpressionResult> getLiterals() {
    List<ExpressionResult> listOfExp =
        new ArrayList<ExpressionResult>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    getExpressionResultList(this, listOfExp);
    Collections.sort(listOfExp);
    return listOfExp;
  }

  // Will get the column informations involved in the expressions by
  // traversing the tree
  public List<ColumnExpression> getColumnList() {
    // TODO
    List<ColumnExpression> listOfExp =
        new ArrayList<ColumnExpression>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    getColumnList(this, listOfExp);
    return listOfExp;
  }

  private void getColumnList(Expression expression, List<ColumnExpression> lst) {
    if (expression instanceof ColumnExpression) {
      ColumnExpression colExp = (ColumnExpression) expression;
      boolean found = false;

      for (ColumnExpression currentColExp : lst) {
        if (currentColExp.getColumnName().equalsIgnoreCase(colExp.getColumnName())) {
          found = true;
          colExp.setColIndex(currentColExp.getColIndex());
          break;
        }
      }
      if (!found) {
        colExp.setColIndex(lst.size());
        lst.add(colExp);
      }
    }
    for (Expression child : expression.getChildren()) {
      getColumnList(child, lst);
    }
  }

  public boolean isSingleColumn() {
    List<ColumnExpression> listOfExp =
        new ArrayList<ColumnExpression>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    getColumnList(this, listOfExp);
    if (listOfExp.size() == 1 && (listOfExp.get(0).isDimension() || listOfExp.get(0).isMeasure())) {
      return true;
    }
    return false;

  }

  private void getExpressionResultList(Expression binaryConditionalExpression,
      List<ExpressionResult> listOfExp) {
    if (binaryConditionalExpression instanceof LiteralExpression) {
      ExpressionResult colExp =
          ((LiteralExpression) binaryConditionalExpression).getExpressionResult();
      listOfExp.add(colExp);
    }
    for (Expression child : binaryConditionalExpression.getChildren()) {
      getExpressionResultList(child, listOfExp);
    }

  }

}
