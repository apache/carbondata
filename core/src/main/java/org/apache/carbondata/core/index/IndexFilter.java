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

package org.apache.carbondata.core.index;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.executor.util.RestructureUtil;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.conditional.InExpression;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.filter.FilterExpressionProcessor;
import org.apache.carbondata.core.scan.filter.intf.FilterOptimizer;
import org.apache.carbondata.core.scan.filter.optimizer.RangeFilterOptmizer;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.util.ObjectSerializationUtil;

/**
 * the filter of Index
 */
public class IndexFilter implements Serializable {

  private static final long serialVersionUID = 6276855832288220240L;

  private transient CarbonTable table;

  private Expression expression;

  private Expression externalSegmentFilter;

  private FilterResolverIntf externalSegmentResolver;

  private FilterResolverIntf resolver;

  private String serializedExpression;

  private SegmentProperties properties;

  public IndexFilter(CarbonTable table, Expression expression) {
    this(table, expression, false);
  }

  public IndexFilter(CarbonTable table, Expression expression, boolean lazyResolve) {
    this.expression = expression;
    this.table = table;
    resolve(lazyResolve);
    if (expression != null) {
      checkIfFilterColumnExistsInTable();
      initializeExternalSegmentFilter();
      try {
        this.serializedExpression = ObjectSerializationUtil.convertObjectToString(expression);
      } catch (IOException e) {
        throw new RuntimeException("Error while serializing the exception", e);
      }
    }
  }

  public IndexFilter(FilterResolverIntf resolver) {
    this.resolver = resolver;
  }

  public IndexFilter(SegmentProperties properties, CarbonTable table, Expression expression) {
    this(table, expression);
    this.properties = properties;
    resolve(false);
  }

  Expression getNewCopyOfExpression() {
    if (expression != null) {
      try {
        return (Expression) ObjectSerializationUtil
            .convertStringToObject(this.serializedExpression);
      } catch (IOException e) {
        throw new RuntimeException("Error while deserializing the exception", e);
      }
    } else {
      return null;
    }
  }

  public void setTable(CarbonTable table) {
    this.table = table;
  }

  private void initializeExternalSegmentFilter() {
    if ((expression instanceof AndExpression) && (((AndExpression) expression)
        .getRight() instanceof InExpression) && (expression.getChildren().get(1).getChildren()
        .get(0) instanceof ColumnExpression) && (((ColumnExpression) expression.getChildren().get(1)
        .getChildren().get(0))).getColumnName()
        .equalsIgnoreCase(CarbonCommonConstants.POSITION_ID)) {
      externalSegmentFilter = ((AndExpression) expression).getLeft();
      if (externalSegmentFilter != null) {
        processFilterExpression(null, null);
        externalSegmentResolver = resolver != null ? resolver.getLeft() : resolveFilter().getLeft();
      }
    }
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

  /**
   * Process the FilterExpression and create FilterResolverIntf.
   *
   * @param lazyResolve whether to create FilterResolverIntf immediately or not.
   *                   Pass true if IndexFilter object is created before checking the valid
   *                   segments for pruning.
   */
  public void resolve(boolean lazyResolve) {
    if (expression != null) {
      processFilterExpression();
      if (!lazyResolve) {
        resolver = resolveFilter();
      }
    }
  }

  public Expression getExpression() {
    return expression;
  }

  public void setExpression(Expression expression) {
    this.expression = expression;
    initializeExternalSegmentFilter();
  }

  public FilterResolverIntf getResolver() {
    if (resolver == null) {
      resolver = resolveFilter();
    }
    return resolver;
  }

  public boolean isEmpty() {
    return resolver == null;
  }

  public boolean isResolvedOnSegment(SegmentProperties segmentProperties) {
    if (expression == null || table == null) {
      return true;
    }
    if (!table.isTransactionalTable()) {
      return false;
    }
    return !(table.hasColumnDrift() && RestructureUtil
        .hasColumnDriftOnSegment(table, segmentProperties));
  }

  Expression getExternalSegmentFilter() {
    if (externalSegmentFilter == null) {
      return expression;
    }
    return externalSegmentFilter;
  }

  public FilterResolverIntf getExternalSegmentResolver() {
    if (externalSegmentResolver == null) {
      return resolver;
    }
    return externalSegmentResolver;
  }

  public void processFilterExpression() {
    processFilterExpression(null, null);
  }

  public void processFilterExpression(boolean[] isFilterDimensions,
      boolean[] isFilterMeasures) {
    processFilterExpressionWithoutRange(isFilterDimensions, isFilterMeasures);
    if (null != expression) {
      // Optimize Filter Expression and fit RANGE filters is conditions apply.
      FilterOptimizer rangeFilterOptimizer = new RangeFilterOptmizer(expression);
      rangeFilterOptimizer.optimizeFilter();
    }
  }

  public void processFilterExpressionWithoutRange(boolean[] isFilterDimensions,
      boolean[] isFilterMeasures) {
    QueryModel.FilterProcessVO processVO;
    if (properties != null) {
      processVO =
          new QueryModel.FilterProcessVO(properties.getDimensions(), properties.getMeasures(),
              new ArrayList<CarbonDimension>());
    } else {
      processVO =
          new QueryModel.FilterProcessVO(
              table.getVisibleDimensions(),
              table.getVisibleMeasures(),
              table.getImplicitDimensions());
    }
    QueryModel.processFilterExpression(processVO, expression, isFilterDimensions, isFilterMeasures,
        table);
  }

  /**
   * Resolve the filter expression.
   */
  private FilterResolverIntf resolveFilter() {
    try {
      AbsoluteTableIdentifier absoluteTableIdentifier =
              table != null ? table.getAbsoluteTableIdentifier() : null;
      FilterExpressionProcessor filterExpressionProcessor = new FilterExpressionProcessor();
      return filterExpressionProcessor
          .getFilterResolver(expression, absoluteTableIdentifier);
    } catch (Exception e) {
      throw new RuntimeException("Error while resolving filter expression", e);
    }
  }
}
