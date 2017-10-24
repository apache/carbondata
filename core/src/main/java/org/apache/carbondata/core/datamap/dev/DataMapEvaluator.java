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

package org.apache.carbondata.core.datamap.dev;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.indexstore.schema.FilterType;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanExpression;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;

import org.apache.commons.collections.map.HashedMap;

public class DataMapEvaluator {

  private Expression filterExpression;

  public DataMapEvaluator (Expression filterExpression) {
    this.filterExpression = filterExpression;
  }

  private ExpressionType matchFilterType (FilterType filterType) {
    switch (filterType) {
      case EQUALTO:
        return ExpressionType.EQUALS;
      case GREATER_THAN:
        return ExpressionType.GREATERTHAN;
      case GREATER_THAN_EQUAL:
        return ExpressionType.GREATERTHAN_EQUALTO;
      case LESS_THAN:
        return ExpressionType.LESSTHAN;
      case LESS_THAN_EQUAL:
        return ExpressionType.LESSTHAN_EQUALTO;
      case LIKE:
        return ExpressionType.RANGE;
      default:
          return ExpressionType.UNKNOWN;
    }
  }

  private ExpressionType matchFilterOperator (Expression exp) {
    if (exp instanceof EqualToExpression) {
      return ExpressionType.EQUALS;
    } else if (exp instanceof GreaterThanExpression) {
      return ExpressionType.GREATERTHAN;
    } else if (exp instanceof GreaterThanEqualToExpression) {
      return ExpressionType.GREATERTHAN_EQUALTO;
    } else if (exp instanceof LessThanExpression) {
      return ExpressionType.LESSTHAN;
    } else if (exp instanceof LessThanEqualToExpression) {
      return ExpressionType.LESSTHAN_EQUALTO;
    } else {
      return ExpressionType.UNKNOWN;
    }
  }

  private boolean filterExpressionMap(Expression currentNode, Expression parentNode,
      DataMapExpressionContainer validBlock, String columnName, FilterType filterType) {
    if (null == currentNode) {
      currentNode = filterExpression;
      parentNode = filterExpression;
      validBlock.setDataMapExpressionContainer(null);
    }

    if (((currentNode instanceof ColumnExpression) && ((ColumnExpression) currentNode)
        .getColumnName().equalsIgnoreCase(columnName)) && (matchFilterOperator(parentNode)
        .equals(matchFilterType(filterType)))) {
      validBlock.setDataMapExpressionContainer(parentNode);
      return true;
    }

    for (Expression exp : currentNode.getChildren()) {
      if (null != exp) {
        return filterExpressionMap(exp, currentNode, validBlock, columnName, filterType);
      } else {
        return false;
      }
    }
    return false;
  }

  public Map<DataMapCoveringOptions.DataMapCovered, List<DataMapColumnExpression>> expressionEvaluator(
      DataMapMeta dataMapMeta) {
    // Check each columns and its parent expression is there in the filterExpression or not.
    Map<String, FilterType> dataMapColumns = dataMapMeta.getIndexedColumns();
    Map<String, DataMapColumnExpression> dataMapCoverMap = new HashedMap();
    Map<String, Boolean> dataMapCoverage = new HashedMap();
    Map<DataMapCoveringOptions.DataMapCovered, List<DataMapColumnExpression>>
        dataMapCoverageFinalResult = new HashMap<>();
    Expression validBlock = null;
    for (Map.Entry<String, FilterType> dataMapColumnFilter : dataMapColumns.entrySet()) {
      DataMapExpressionContainer validExprBlock = new DataMapExpressionContainer();
      boolean covered =
          filterExpressionMap(null, null, validExprBlock, dataMapColumnFilter.getKey(),
              dataMapColumnFilter.getValue());
      DataMapColumnExpression dataMapColumnExpression =
          new DataMapColumnExpression(validExprBlock.getExpressionBlock(),
              dataMapColumnFilter.getKey(), dataMapColumnFilter.getValue(), covered);
      dataMapCoverage.put(dataMapColumnFilter.getKey(), covered);
      dataMapCoverMap.put(dataMapColumnFilter.getKey(), dataMapColumnExpression);
    }

    // Check if all the values are set to true or partially true or all are false.
    if (dataMapCoverage.values().containsAll(Arrays.asList(true))) {
      dataMapCoverageFinalResult.put(DataMapCoveringOptions.DataMapCovered.FULLY_COVERED,
          new ArrayList<>(dataMapCoverMap.values()));
      return dataMapCoverageFinalResult;
    } else if (dataMapCoverMap.values().containsAll(Arrays.asList(false))) {
      dataMapCoverageFinalResult.put(DataMapCoveringOptions.DataMapCovered.NOT_COVERED,
          new ArrayList<>(dataMapCoverMap.values()));
      return dataMapCoverageFinalResult;
    } else {
      dataMapCoverageFinalResult.put(DataMapCoveringOptions.DataMapCovered.PARTIALLY_COVERED,
          new ArrayList<>(dataMapCoverMap.values()));
      return dataMapCoverageFinalResult;
    }
  }
}
