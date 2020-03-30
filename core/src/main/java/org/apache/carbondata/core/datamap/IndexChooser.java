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
import java.util.Set;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.expr.AndIndexExprWrapper;
import org.apache.carbondata.core.datamap.dev.expr.IndexExprWrapper;
import org.apache.carbondata.core.datamap.dev.expr.IndexExprWrapperImpl;
import org.apache.carbondata.core.datamap.dev.expr.OrIndexExprWrapper;
import org.apache.carbondata.core.datamap.status.IndexStatus;
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
public class IndexChooser {

  private CarbonTable carbonTable;
  private List<TableIndex> cgIndexes;
  private List<TableIndex> fgIndexes;

  public IndexChooser(CarbonTable carbonTable) throws IOException {
    this.carbonTable = carbonTable;
    // read all indexes for this table and populate CG and FG index list
    List<TableIndex> visibleIndexes = carbonTable.getAllVisibleIndexes();
    cgIndexes = new ArrayList<>(visibleIndexes.size());
    fgIndexes = new ArrayList<>(visibleIndexes.size());
    for (TableIndex visibleIndex : visibleIndexes) {
      if (visibleIndex.getDataMapSchema().getProperties().get(CarbonCommonConstants.INDEX_STATUS)
          != null && visibleIndex.getDataMapSchema().getProperties()
          .get(CarbonCommonConstants.INDEX_STATUS).equalsIgnoreCase(IndexStatus.ENABLED.name())) {
        IndexLevel level = visibleIndex.getIndexFactory().getDataMapLevel();
        if (level == IndexLevel.CG) {
          cgIndexes.add(visibleIndex);
        } else {
          fgIndexes.add(visibleIndex);
        }
      }
    }
  }

  /**
   * Return a chosen datamap based on input filter. See {@link IndexChooser}
   */
  public IndexExprWrapper choose(FilterResolverIntf filter) {
    if (filter != null) {
      Expression expression = filter.getFilterExpression();
      // First check for FG datamaps if any exist
      ExpressionTuple tuple = selectDataMap(expression, fgIndexes, filter);
      if (tuple.indexExprWrapper == null) {
        // Check for CG datamap
        tuple = selectDataMap(expression, cgIndexes, filter);
      }
      if (tuple.indexExprWrapper != null) {
        return tuple.indexExprWrapper;
      }
    }
    // Return the default datamap if no other datamap exists.
    return new IndexExprWrapperImpl(
        DataMapStoreManager.getInstance().getDefaultIndex(carbonTable), filter);
  }

  /**
   * Return a chosen FG datamap based on input filter. See {@link IndexChooser}
   */
  public IndexExprWrapper chooseFGDataMap(FilterResolverIntf resolverIntf) {
    return chooseDataMap(IndexLevel.FG, resolverIntf);
  }

  /**
   * Return a chosen CG datamap based on input filter. See {@link IndexChooser}
   */
  public IndexExprWrapper chooseCGDataMap(FilterResolverIntf resolverIntf) {
    return chooseDataMap(IndexLevel.CG, resolverIntf);
  }

  IndexExprWrapper chooseDataMap(IndexLevel level, FilterResolverIntf resolverIntf) {
    if (resolverIntf != null) {
      Expression expression = resolverIntf.getFilterExpression();
      List<TableIndex> datamaps = level == IndexLevel.CG ? cgIndexes : fgIndexes;
      if (datamaps.size() > 0) {
        ExpressionTuple tuple = selectDataMap(expression, datamaps, resolverIntf);
        if (tuple.indexExprWrapper != null) {
          return tuple.indexExprWrapper;
        }
      }
    }
    return null;
  }

  /**
   * Returns default blocklet datamap
   * @param carbonTable
   * @param resolverIntf
   * @return
   */
  public static IndexExprWrapper getDefaultDataMap(CarbonTable carbonTable,
      FilterResolverIntf resolverIntf) {
    // Return the default datamap if no other datamap exists.
    return new IndexExprWrapperImpl(
        DataMapStoreManager.getInstance().getDefaultIndex(carbonTable), resolverIntf);
  }

  private ExpressionTuple selectDataMap(Expression expression, List<TableIndex> allDataMap,
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
          if (left.indexExprWrapper != null && right.indexExprWrapper != null) {
            filterExpressionTypes.addAll(left.filterExpressionTypes);
            filterExpressionTypes.addAll(right.filterExpressionTypes);
            List<ColumnExpression> columnExpressions = new ArrayList<>();
            columnExpressions.addAll(left.columnExpressions);
            columnExpressions.addAll(right.columnExpressions);
            // Check if we can merge them to single datamap.
            TableIndex dataMap =
                chooseDataMap(allDataMap, columnExpressions, filterExpressionTypes);
            TrueConditionalResolverImpl resolver = new TrueConditionalResolverImpl(
                new AndExpression(left.expression, right.expression), false,
                true);
            if (dataMap != null) {
              ExpressionTuple tuple = new ExpressionTuple();
              tuple.columnExpressions = columnExpressions;
              tuple.indexExprWrapper = new IndexExprWrapperImpl(dataMap, resolver);
              tuple.expression = resolver.getFilterExpression();
              return tuple;
            } else {
              // Apply AND expression.
              ExpressionTuple tuple = new ExpressionTuple();
              tuple.columnExpressions = columnExpressions;
              tuple.indexExprWrapper = new AndIndexExprWrapper(left.indexExprWrapper,
                  right.indexExprWrapper, resolver);
              tuple.expression = resolver.getFilterExpression();
              return tuple;
            }
          } else if (left.indexExprWrapper != null) {
            return left;
          } else if (right.indexExprWrapper != null) {
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
          if (left.indexExprWrapper != null && right.indexExprWrapper != null) {
            TrueConditionalResolverImpl resolver = new TrueConditionalResolverImpl(
                new OrExpression(left.expression, right.expression), false,
                true);
            List<ColumnExpression> columnExpressions = new ArrayList<>();
            columnExpressions.addAll(left.columnExpressions);
            columnExpressions.addAll(right.columnExpressions);
            ExpressionTuple tuple = new ExpressionTuple();
            tuple.columnExpressions = columnExpressions;
            tuple.indexExprWrapper = new OrIndexExprWrapper(left.indexExprWrapper,
                right.indexExprWrapper, resolver);
            tuple.expression = resolver.getFilterExpression();
            return tuple;
          } else {
            left.indexExprWrapper = null;
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
        TableIndex dataMap =
            chooseDataMap(allDataMap, tuple.columnExpressions, filterExpressionTypes);
        if (dataMap != null) {
          tuple.indexExprWrapper = new IndexExprWrapperImpl(dataMap, resolver);
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

  private TableIndex chooseDataMap(List<TableIndex> allDataMap,
      List<ColumnExpression> columnExpressions, Set<ExpressionType> expressionTypes) {
    List<IndexTuple> tuples = new ArrayList<>();
    for (TableIndex dataMap : allDataMap) {
      if (null != dataMap.getIndexFactory().getMeta() && contains(
          dataMap.getIndexFactory().getMeta(), columnExpressions, expressionTypes)) {
        tuples.add(
            new IndexTuple(dataMap.getIndexFactory().getMeta().getIndexedColumns().size(),
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
  private boolean contains(IndexMeta mapMeta, List<ColumnExpression> columnExpressions,
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

    IndexExprWrapper indexExprWrapper;

    List<ColumnExpression> columnExpressions = new ArrayList<>();

    Set<ExpressionType> filterExpressionTypes = new HashSet<>();

    Expression expression;

  }

  private static class IndexTuple implements Comparable<IndexTuple> {

    int order;

    TableIndex dataMap;

    public IndexTuple(int order, TableIndex dataMap) {
      this.order = order;
      this.dataMap = dataMap;
    }

    @Override
    public int compareTo(IndexTuple o) {
      return order - o.order;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      IndexTuple that = (IndexTuple) o;

      if (order != that.order) return false;
      return dataMap != null ? dataMap.equals(that.dataMap) : that.dataMap == null;
    }

    @Override
    public int hashCode() {
      int result = order;
      result = 31 * result + (dataMap != null ? dataMap.hashCode() : 0);
      return result;
    }
  }

}
