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

package org.carbondata.query.expression.conditional;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.logical.BinaryLogicalExpression;

public abstract class BinaryConditionalExpression extends BinaryLogicalExpression
    implements ConditionalExpression {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  public BinaryConditionalExpression(Expression left, Expression right) {
    super(left, right);
    // TODO Auto-generated constructor stub
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
        if (currentColExp.getColumnName().equals(colExp.getColumnName())) {
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

  public boolean isSingleDimension() {
    List<ColumnExpression> listOfExp =
        new ArrayList<ColumnExpression>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    getColumnList(this, listOfExp);
    if (listOfExp.size() == 1 && listOfExp.get(0).isDimension()) {
      return true;
    }
    return false;

  }

  /**
   * the method will return flag (true or false) depending on the existence of the
   * direct dictionary columns in conditional expression
   *
   * @return the method will return flag (true or false)
   */
  @Override public boolean isDirectDictionaryColumns() {
    List<ColumnExpression> listOfExp =
        new ArrayList<ColumnExpression>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    getColumnList(this, listOfExp);
    for (ColumnExpression ce : listOfExp) {
      if (!ce.getCarbonColumn().hasEncoding(Encoding.DICTIONARY)) {
        return true;
      }
    }
    return false;
  }

}
