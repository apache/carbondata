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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.index.dev.expr.AndIndexExprWrapper;
import org.apache.carbondata.core.index.dev.expr.IndexExprWrapper;
import org.apache.carbondata.core.index.dev.expr.IndexExprWrapperImpl;
import org.apache.carbondata.core.index.dev.expr.OrIndexExprWrapper;
import org.apache.carbondata.core.index.status.IndexStatus;
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
 * 1. Based on filter expression it converts the available Indexs to Index expression.
 *   For example, there are 2 Indexes available on table1
 *   Index1 : column1
 *   Index2 : column2
 *   Query: select * from table1 where column1 ='a' and column2 =b
 *   For the above query, we create Index expression as AndIndexExpression(Index1, Index2).
 *   So for the above query both the Indexes are included and the output of them will be
 *   applied AND condition to improve the performance
 *
 * 2. It chooses the Index out of available Indexes based on simple logic.
 *   Like if there is filter condition on column1 then for
 *   supposing 2 Indexes(1. column1 2. column1+column2) are supporting this column then we choose
 *   the Index which has fewer columns that is the first Index.
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
      if (visibleIndex.getIndexSchema().getProperties().get(CarbonCommonConstants.INDEX_STATUS)
          != null && visibleIndex.getIndexSchema().getProperties()
          .get(CarbonCommonConstants.INDEX_STATUS).equalsIgnoreCase(IndexStatus.ENABLED.name())) {
        IndexLevel level = visibleIndex.getIndexFactory().getIndexLevel();
        if (level == IndexLevel.CG) {
          cgIndexes.add(visibleIndex);
        } else {
          fgIndexes.add(visibleIndex);
        }
      }
    }
  }

  /**
   * Return a chosen Index based on input filter. See {@link IndexChooser}
   */
  public IndexExprWrapper choose(FilterResolverIntf filter) {
    if (filter != null) {
      Expression expression = filter.getFilterExpression();
      // First check for FG Indexes if any exist
      ExpressionTuple tuple = selectIndex(expression, fgIndexes, filter);
      if (tuple.indexExprWrapper == null) {
        // Check for CG Index
        tuple = selectIndex(expression, cgIndexes, filter);
      }
      if (tuple.indexExprWrapper != null) {
        return tuple.indexExprWrapper;
      }
    }
    // Return the default Index if no other Index exists.
    return new IndexExprWrapperImpl(
        IndexStoreManager.getInstance().getDefaultIndex(carbonTable), filter);
  }

  /**
   * Return a chosen FG Index based on input filter. See {@link IndexChooser}
   */
  public IndexExprWrapper chooseFGIndex(FilterResolverIntf resolverIntf) {
    return chooseIndex(IndexLevel.FG, resolverIntf);
  }

  /**
   * Return a chosen CG Index based on input filter. See {@link IndexChooser}
   */
  public IndexExprWrapper chooseCGIndex(FilterResolverIntf resolverIntf) {
    return chooseIndex(IndexLevel.CG, resolverIntf);
  }

  IndexExprWrapper chooseIndex(IndexLevel level, FilterResolverIntf resolverIntf) {
    if (resolverIntf != null) {
      Expression expression = resolverIntf.getFilterExpression();
      List<TableIndex> indexes = level == IndexLevel.CG ? cgIndexes : fgIndexes;
      if (indexes.size() > 0) {
        ExpressionTuple tuple = selectIndex(expression, indexes, resolverIntf);
        if (tuple.indexExprWrapper != null) {
          return tuple.indexExprWrapper;
        }
      }
    }
    return null;
  }

  /**
   * Returns default blocklet index
   * @param carbonTable
   * @param resolverIntf
   * @return
   */
  public static IndexExprWrapper getDefaultIndex(CarbonTable carbonTable,
      FilterResolverIntf resolverIntf) {
    // Return the default index if no other index exists.
    return new IndexExprWrapperImpl(
        IndexStoreManager.getInstance().getDefaultIndex(carbonTable), resolverIntf);
  }

  private ExpressionTuple selectIndex(Expression expression, List<TableIndex> allIndex,
      FilterResolverIntf filterResolverIntf) {
    switch (expression.getFilterExpressionType()) {
      case AND:
        if (expression instanceof AndExpression) {
          AndExpression andExpression = (AndExpression) expression;
          ExpressionTuple left = selectIndex(andExpression.getLeft(), allIndex,
              filterResolverIntf.getLeft());
          ExpressionTuple right = selectIndex(andExpression.getRight(), allIndex,
              filterResolverIntf.getRight());
          Set<ExpressionType> filterExpressionTypes = new HashSet<>();
          // If both left and right has Index then we can either merge both Indexes to single
          // Index if possible. Otherwise apply AND expression.
          if (left.indexExprWrapper != null && right.indexExprWrapper != null) {
            filterExpressionTypes.addAll(left.filterExpressionTypes);
            filterExpressionTypes.addAll(right.filterExpressionTypes);
            List<ColumnExpression> columnExpressions = new ArrayList<>();
            columnExpressions.addAll(left.columnExpressions);
            columnExpressions.addAll(right.columnExpressions);
            // Check if we can merge them to single Index.
            TableIndex index =
                chooseIndex(allIndex, columnExpressions, filterExpressionTypes);
            TrueConditionalResolverImpl resolver = new TrueConditionalResolverImpl(
                new AndExpression(left.expression, right.expression), false,
                true);
            if (index != null) {
              ExpressionTuple tuple = new ExpressionTuple();
              tuple.columnExpressions = columnExpressions;
              tuple.indexExprWrapper = new IndexExprWrapperImpl(index, resolver);
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
          ExpressionTuple left = selectIndex(orExpression.getLeft(), allIndex,
              filterResolverIntf.getLeft());
          ExpressionTuple right = selectIndex(orExpression.getRight(), allIndex,
              filterResolverIntf.getRight());
          // If both left and right has Index then we can either merge both Indexes to single
          // Index if possible. Otherwise apply OR expression.
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
        TableIndex index =
            chooseIndex(allIndex, tuple.columnExpressions, filterExpressionTypes);
        if (index != null) {
          tuple.indexExprWrapper = new IndexExprWrapperImpl(index, resolver);
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
      // build a fake ColumnExpression to filter Indexes which contain target column
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

  private TableIndex chooseIndex(List<TableIndex> allIndex,
      List<ColumnExpression> columnExpressions, Set<ExpressionType> expressionTypes) {
    List<IndexTuple> tuples = new ArrayList<>();
    for (TableIndex index : allIndex) {
      if (null != index.getIndexFactory().getMeta() && contains(
          index.getIndexFactory().getMeta(), columnExpressions, expressionTypes)) {
        tuples.add(
            new IndexTuple(index.getIndexFactory().getMeta().getIndexedColumns().size(),
                index));
      }
    }
    if (tuples.size() > 0) {
      Collections.sort(tuples);
      return tuples.get(0).index;
    }
    return null;
  }

  /**
   * Return true if the input Index contains the column that needed in
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

    TableIndex index;

    public IndexTuple(int order, TableIndex index) {
      this.order = order;
      this.index = index;
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
      return index != null ? index.equals(that.index) : that.index == null;
    }

    @Override
    public int hashCode() {
      int result = order;
      result = 31 * result + (index != null ? index.hashCode() : 0);
      return result;
    }
  }

}
