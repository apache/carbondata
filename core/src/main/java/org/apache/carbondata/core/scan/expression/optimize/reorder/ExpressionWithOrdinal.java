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

package org.apache.carbondata.core.scan.expression.optimize.reorder;

import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.UnknownExpression;
import org.apache.carbondata.core.scan.expression.conditional.ConditionalExpression;

/**
 * a wrapper class of Expression with storage ordinal
 */
public class ExpressionWithOrdinal extends StorageOrdinal {
  protected Expression expression;

  public ExpressionWithOrdinal(Expression expression) {
    this.minOrdinal = Integer.MAX_VALUE;
    this.expression = expression;
  }

  @Override
  public void updateMinOrdinal(Map<String, Integer> columnMapOrdinal) {
    updateMinOrdinal(expression, columnMapOrdinal);
  }

  private void updateMinOrdinal(Expression expression, Map<String, Integer> nameMapOrdinal) {
    if (expression != null && expression.getChildren() != null) {
      if (expression.getChildren().size() == 0) {
        if (expression instanceof ConditionalExpression) {
          List<ColumnExpression> columnList =
              ((ConditionalExpression) expression).getColumnList();
          for (ColumnExpression columnExpression : columnList) {
            updateMinOrdinal(columnExpression.getColumnName(), nameMapOrdinal);
          }
        }
      } else {
        for (Expression subExpression : expression.getChildren()) {
          if (subExpression instanceof ColumnExpression) {
            updateMinOrdinal(((ColumnExpression) subExpression).getColumnName(), nameMapOrdinal);
          } else if (expression instanceof UnknownExpression) {
            UnknownExpression exp = ((UnknownExpression) expression);
            List<ColumnExpression> listOfColExpression = exp.getAllColumnList();
            for (ColumnExpression columnExpression : listOfColExpression) {
              updateMinOrdinal(columnExpression.getColumnName(), nameMapOrdinal);
            }
          } else {
            updateMinOrdinal(subExpression, nameMapOrdinal);
          }
        }
      }
    }
  }

  private void updateMinOrdinal(String columnName, Map<String, Integer> nameMapOrdinal) {
    Integer ordinal = nameMapOrdinal.get(columnName.toLowerCase());
    if (ordinal != null && ordinal < minOrdinal) {
      minOrdinal = ordinal;
    }
  }

  @Override
  public Expression toExpression() {
    return expression;
  }
}
