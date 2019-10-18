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

package org.apache.carbondata.core.datamap;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

/**
 * the filter of DataMap
 */
public class DataMapFilter implements Serializable {

  private CarbonTable table;

  private Expression expression;

  private FilterResolverIntf resolver;

  public DataMapFilter(CarbonTable table, Expression expression) {
    this.table = table;
    this.expression = expression;
    if (expression != null) {
      checkIfFilterColumnExistsInTable();
    }
    resolve();
  }

  private Set<String> extractColumnExpressions(Expression expression) {
    Set<String> columnExpressionList = new HashSet<>();
    for (Expression expressions: expression.getChildren()) {
      if (expressions != null && expressions.getChildren() != null
          && expressions.getChildren().size() > 0) {
        columnExpressionList.addAll(extractColumnExpressions(expressions));
      } else if (expressions instanceof ColumnExpression) {
        columnExpressionList.add(((ColumnExpression) expressions).getColumnName());
      }
    }
    return columnExpressionList;
  }

  private void checkIfFilterColumnExistsInTable() {
    Set<String> columnExpressionList = extractColumnExpressions(expression);
    for (String colExpression : columnExpressionList) {
      if (colExpression.equalsIgnoreCase("positionid")) {
        continue;
      }
      boolean exists = false;
      for (CarbonMeasure carbonMeasure : table.getAllMeasures()) {
        if (!carbonMeasure.isInvisible() && carbonMeasure.getColName()
            .equalsIgnoreCase(colExpression)) {
          exists = true;
        }
      }
      for (CarbonDimension carbonDimension : table.getAllDimensions()) {
        if (!carbonDimension.isInvisible() && carbonDimension.getColName()
            .equalsIgnoreCase(colExpression)) {
          exists = true;
        }
      }
      if (!exists) {
        throw new RuntimeException(
            "Column " + colExpression + " not found in table " + table.getTableUniqueName());
      }
    }
  }

  public DataMapFilter(FilterResolverIntf resolver) {
    this.resolver = resolver;
  }

  private void resolve() {
    if (expression != null) {
      table.processFilterExpression(expression, null, null);
      resolver = CarbonTable.resolveFilter(expression, table.getAbsoluteTableIdentifier());
    }
  }

  public Expression getExpression() {
    return expression;
  }

  public void setExpression(Expression expression) {
    this.expression = expression;
  }

  public FilterResolverIntf getResolver() {
    return resolver;
  }

  public void setResolver(FilterResolverIntf resolver) {
    this.resolver = resolver;
  }

  public boolean isEmpty() {
    return resolver == null;
  }

  public boolean isResolvedOnSegment(SegmentProperties segmentProperties) {
    return false;
  }
}
