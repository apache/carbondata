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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datamap.dev.expr.AndDataMapExprWrapper;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapper;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapperImpl;
import org.apache.carbondata.core.datamap.dev.expr.OrDataMapExprWrapper;
import org.apache.carbondata.core.datamap.status.DataMapStatusDetail;
import org.apache.carbondata.core.datamap.status.DataMapStatusManager;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.MatchExpression;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.TrueConditionalResolverImpl;

/**
 * This chooser does 2 jobs.
 * 1. Based on filter expression it converts the available datamaps to datamap expression.
 *   For example, there are 2 datamaps available on table1
 *   Datamap1 : column1
 *   Datamap2 : column2
 *   Query: select * from table1 where column1 ='a' and column2 =b
 *   For the above query, we create datamap expression as AndDataMapExpression(Datamap1, DataMap2).
 *   So for the above query both the datamaps are included and the output of them will be
 *   applied AND condition to improve the performance
 *
 * 2. It chooses the datamap out of available datamaps based on simple logic.
 *   Like if there is filter condition on column1 then for
 *   supposing 2 datamaps(1. column1 2. column1+column2) are supporting this column then we choose
 *   the datamap which has fewer columns that is the first datamap.
 */
@InterfaceAudience.Internal
public class DataMapChooser {

  private CarbonTable carbonTable;
  private List<TableDataMap> cgDataMaps;
  private List<TableDataMap> fgDataMaps;

  public DataMapChooser(CarbonTable carbonTable) throws IOException {
    this.carbonTable = carbonTable;
    // read all datamaps for this table and populate CG and FG datamap list
    List<TableDataMap> visibleDataMaps =
        DataMapStoreManager.getInstance().getAllVisibleDataMap(carbonTable);
    Map<String, DataMapStatusDetail> map = DataMapStatusManager.readDataMapStatusMap();
    cgDataMaps = new ArrayList<>(visibleDataMaps.size());
    fgDataMaps = new ArrayList<>(visibleDataMaps.size());
    for (TableDataMap visibleDataMap : visibleDataMaps) {
      DataMapStatusDetail status = map.get(visibleDataMap.getDataMapSchema().getDataMapName());
      if (status != null && status.isEnabled()) {
        DataMapLevel level = visibleDataMap.getDataMapFactory().getDataMapLevel();
        if (level == DataMapLevel.CG) {
          cgDataMaps.add(visibleDataMap);
        } else {
          fgDataMaps.add(visibleDataMap);
        }
      }
    }
  }

  /**
   * Return a chosen datamap based on input filter. See {@link DataMapChooser}
   */
  public DataMapExprWrapper choose(FilterResolverIntf filter) {
    if (filter != null) {
      Expression expression = filter.getFilterExpression();
      // First check for FG datamaps if any exist
      ExpressionTuple tuple = selectDataMap(expression, fgDataMaps, filter);
      if (tuple.dataMapExprWrapper == null) {
        // Check for CG datamap
        tuple = selectDataMap(expression, cgDataMaps, filter);
      }
      if (tuple.dataMapExprWrapper != null) {
        return tuple.dataMapExprWrapper;
      }
    }
    // Return the default datamap if no other datamap exists.
    return new DataMapExprWrapperImpl(
        DataMapStoreManager.getInstance().getDefaultDataMap(carbonTable), filter);
  }

  /**
   * Return a chosen FG datamap based on input filter. See {@link DataMapChooser}
   */
  public DataMapExprWrapper chooseFGDataMap(FilterResolverIntf resolverIntf) {
    return chooseDataMap(DataMapLevel.FG, resolverIntf);
  }

  /**
   * Return a chosen CG datamap based on input filter. See {@link DataMapChooser}
   */
  public DataMapExprWrapper chooseCGDataMap(FilterResolverIntf resolverIntf) {
    return chooseDataMap(DataMapLevel.CG, resolverIntf);
  }

  DataMapExprWrapper chooseDataMap(DataMapLevel level, FilterResolverIntf resolverIntf) {
    if (resolverIntf != null) {
      Expression expression = resolverIntf.getFilterExpression();
      List<TableDataMap> datamaps = level == DataMapLevel.CG ? cgDataMaps : fgDataMaps;
      if (datamaps.size() > 0) {
        ExpressionTuple tuple = selectDataMap(expression, datamaps, resolverIntf);
        if (tuple.dataMapExprWrapper != null) {
          return tuple.dataMapExprWrapper;
        }
      }
    }
    return null;
  }

  /**
   * Get all datamaps of the table for clearing purpose
   */
  public DataMapExprWrapper getAllDataMapsForClear(CarbonTable carbonTable)
      throws IOException {
    List<TableDataMap> allDataMapFG =
        DataMapStoreManager.getInstance().getAllDataMap(carbonTable);
    DataMapExprWrapper initialExpr = null;
    if (allDataMapFG.size() > 0) {
      initialExpr = new DataMapExprWrapperImpl(allDataMapFG.get(0), null);

      for (int i = 1; i < allDataMapFG.size(); i++) {
        initialExpr = new AndDataMapExprWrapper(initialExpr,
            new DataMapExprWrapperImpl(allDataMapFG.get(i), null), null);
      }
    }
    return initialExpr;
  }

  /**
   * Returns default blocklet datamap
   * @param carbonTable
   * @param resolverIntf
   * @return
   */
  public static DataMapExprWrapper getDefaultDataMap(CarbonTable carbonTable,
      FilterResolverIntf resolverIntf) {
    // Return the default datamap if no other datamap exists.
    return new DataMapExprWrapperImpl(
        DataMapStoreManager.getInstance().getDefaultDataMap(carbonTable), resolverIntf);
  }

  private ExpressionTuple selectDataMap(Expression expression, List<TableDataMap> allDataMap,
      FilterResolverIntf filterResolverIntf) {
    switch (expression.getFilterExpressionType()) {
      case AND:
        if (expression instanceof AndExpression) {
          AndExpression andExpression = (AndExpression) expression;
          ExpressionTuple left = selectDataMap(andExpression.getLeft(), allDataMap,
              filterResolverIntf.getLeft());
          ExpressionTuple right = selectDataMap(andExpression.getRight(), allDataMap,
              filterResolverIntf.getRight());
          Set<ExpressionType> filterExpressionTypes = new HashSet<>();
          // If both left and right has datamap then we can either merge both datamaps to single
          // datamap if possible. Otherwise apply AND expression.
          if (left.dataMapExprWrapper != null && right.dataMapExprWrapper != null) {
            filterExpressionTypes.addAll(left.filterExpressionTypes);
            filterExpressionTypes.addAll(right.filterExpressionTypes);
            List<ColumnExpression> columnExpressions = new ArrayList<>();
            columnExpressions.addAll(left.columnExpressions);
            columnExpressions.addAll(right.columnExpressions);
            // Check if we can merge them to single datamap.
            TableDataMap dataMap =
                chooseDataMap(allDataMap, columnExpressions, filterExpressionTypes);
            TrueConditionalResolverImpl resolver = new TrueConditionalResolverImpl(
                new AndExpression(left.expression, right.expression), false,
                true);
            if (dataMap != null) {
              ExpressionTuple tuple = new ExpressionTuple();
              tuple.columnExpressions = columnExpressions;
              tuple.dataMapExprWrapper = new DataMapExprWrapperImpl(dataMap, resolver);
              tuple.expression = resolver.getFilterExpression();
              return tuple;
            } else {
              // Apply AND expression.
              ExpressionTuple tuple = new ExpressionTuple();
              tuple.columnExpressions = columnExpressions;
              tuple.dataMapExprWrapper = new AndDataMapExprWrapper(left.dataMapExprWrapper,
                  right.dataMapExprWrapper, resolver);
              tuple.expression = resolver.getFilterExpression();
              return tuple;
            }
          } else if (left.dataMapExprWrapper != null) {
            return left;
          } else if (right.dataMapExprWrapper != null) {
            return right;
          } else {
            return left;
          }
        }
        break;
      case OR:
        if (expression instanceof OrExpression) {
          OrExpression orExpression = (OrExpression) expression;
          ExpressionTuple left = selectDataMap(orExpression.getLeft(), allDataMap,
              filterResolverIntf.getLeft());
          ExpressionTuple right = selectDataMap(orExpression.getRight(), allDataMap,
              filterResolverIntf.getRight());
          // If both left and right has datamap then we can either merge both datamaps to single
          // datamap if possible. Otherwise apply OR expression.
          if (left.dataMapExprWrapper != null && right.dataMapExprWrapper != null) {
            TrueConditionalResolverImpl resolver = new TrueConditionalResolverImpl(
                new OrExpression(left.expression, right.expression), false,
                true);
            List<ColumnExpression> columnExpressions = new ArrayList<>();
            columnExpressions.addAll(left.columnExpressions);
            columnExpressions.addAll(right.columnExpressions);
            ExpressionTuple tuple = new ExpressionTuple();
            tuple.columnExpressions = columnExpressions;
            tuple.dataMapExprWrapper = new OrDataMapExprWrapper(left.dataMapExprWrapper,
                right.dataMapExprWrapper, resolver);
            tuple.expression = resolver.getFilterExpression();
            return tuple;
          } else {
            left.dataMapExprWrapper = null;
            return left;
          }
        }
        break;
      default:
        ExpressionTuple tuple = new ExpressionTuple();
        extractColumnExpression(expression, tuple.columnExpressions);
        Set<ExpressionType> filterExpressionTypes = new HashSet<>();
        filterExpressionTypes.add(expression.getFilterExpressionType());
        TrueConditionalResolverImpl resolver = new TrueConditionalResolverImpl(
            filterResolverIntf.getFilterExpression(), false,
            true);
        TableDataMap dataMap =
            chooseDataMap(allDataMap, tuple.columnExpressions, filterExpressionTypes);
        if (dataMap != null) {
          tuple.dataMapExprWrapper = new DataMapExprWrapperImpl(dataMap, resolver);
          tuple.filterExpressionTypes.addAll(filterExpressionTypes);
          tuple.expression = filterResolverIntf.getFilterExpression();
        }
        return tuple;
    }
    return new ExpressionTuple();
  }


  private void extractColumnExpression(Expression expression,
      List<ColumnExpression> columnExpressions) {
    if (expression instanceof ColumnExpression) {
      columnExpressions.add((ColumnExpression) expression);
    } else if (expression instanceof MatchExpression) {
      // this is a special case for lucene
      // build a fake ColumnExpression to filter datamaps which contain target column
      // a Lucene query string is alike "column:query term"
      String[] queryItems = expression.getString().split(":", 2);
      if (queryItems.length == 2) {
        columnExpressions.add(new ColumnExpression(queryItems[0], null));
      }
    } else if (expression != null) {
      List<Expression> children = expression.getChildren();
      if (children != null && children.size() > 0) {
        for (Expression exp : children) {
          if (exp != null && exp.getFilterExpressionType() != ExpressionType.UNKNOWN) {
            extractColumnExpression(exp, columnExpressions);
          }
        }
      }
    }
  }

  private TableDataMap chooseDataMap(List<TableDataMap> allDataMap,
      List<ColumnExpression> columnExpressions, Set<ExpressionType> expressionTypes) {
    List<DataMapTuple> tuples = new ArrayList<>();
    for (TableDataMap dataMap : allDataMap) {
      if (null != dataMap.getDataMapFactory().getMeta() && contains(
          dataMap.getDataMapFactory().getMeta(), columnExpressions, expressionTypes)) {
        tuples.add(
            new DataMapTuple(dataMap.getDataMapFactory().getMeta().getIndexedColumns().size(),
                dataMap));
      }
    }
    if (tuples.size() > 0) {
      Collections.sort(tuples);
      return tuples.get(0).dataMap;
    }
    return null;
  }

  /**
   * Return true if the input datamap contains the column that needed in
   * specified expression
   */
  private boolean contains(DataMapMeta mapMeta, List<ColumnExpression> columnExpressions,
      Set<ExpressionType> expressionTypes) {
    if (mapMeta.getIndexedColumns().size() == 0 || columnExpressions.size() == 0) {
      return false;
    }
    boolean contains = true;
    for (ColumnExpression expression : columnExpressions) {
      if (!mapMeta.getIndexedColumnNames().contains(expression.getColumnName()) ||
          !mapMeta.getOptimizedOperation().containsAll(expressionTypes)) {
        contains = false;
        break;
      }
    }
    return contains;
  }

  private static class ExpressionTuple {

    DataMapExprWrapper dataMapExprWrapper;

    List<ColumnExpression> columnExpressions = new ArrayList<>();

    Set<ExpressionType> filterExpressionTypes = new HashSet<>();

    Expression expression;

  }

  private static class DataMapTuple implements Comparable<DataMapTuple> {

    int order;

    TableDataMap dataMap;

    public DataMapTuple(int order, TableDataMap dataMap) {
      this.order = order;
      this.dataMap = dataMap;
    }

    @Override public int compareTo(DataMapTuple o) {
      return order - o.order;
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DataMapTuple that = (DataMapTuple) o;

      if (order != that.order) return false;
      return dataMap != null ? dataMap.equals(that.dataMap) : that.dataMap == null;
    }

    @Override public int hashCode() {
      int result = order;
      result = 31 * result + (dataMap != null ? dataMap.hashCode() : 0);
      return result;
    }
  }

}
