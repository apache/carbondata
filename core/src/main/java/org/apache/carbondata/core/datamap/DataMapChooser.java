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
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datamap.dev.expr.AndDataMapExprWrapper;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapper;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapperImpl;
import org.apache.carbondata.core.datamap.dev.expr.OrDataMapExprWrapper;
import org.apache.carbondata.core.datamap.status.DataMapStatusDetail;
import org.apache.carbondata.core.datamap.status.DataMapStatusManager;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

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
      DataMapExprWrapper dataMapExprWrapper = selectDataMap(expression, fgDataMaps, filter);
      if (dataMapExprWrapper == null) {
        // Check for CG datamap
        dataMapExprWrapper = selectDataMap(expression, cgDataMaps, filter);
      }
      if (dataMapExprWrapper != null) {
        return dataMapExprWrapper;
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

  private DataMapExprWrapper chooseDataMap(DataMapLevel level, FilterResolverIntf resolverIntf) {
    if (resolverIntf != null) {
      Expression expression = resolverIntf.getFilterExpression();
      List<TableDataMap> datamaps = level == DataMapLevel.CG ? cgDataMaps : fgDataMaps;
      if (datamaps.size() > 0) {
        return selectDataMap(expression, datamaps, resolverIntf);
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

  private DataMapExprWrapper selectDataMap(Expression expression, List<TableDataMap> allDataMap,
      FilterResolverIntf filterResolverIntf) {
    switch (expression.getFilterExpressionType()) {
      case AND:
        if (expression instanceof AndExpression) {
          AndExpression andExpression = (AndExpression) expression;
          DataMapExprWrapper left = selectDataMap(andExpression.getLeft(), allDataMap,
              filterResolverIntf.getLeft());
          DataMapExprWrapper right = selectDataMap(andExpression.getRight(), allDataMap,
              filterResolverIntf.getRight());
          if (left != null && right != null) {
            // if both child of AndExpression are supported by same datamap
            // merge two DataMapExprWrapper into one
            if (left instanceof DataMapExprWrapperImpl && right instanceof DataMapExprWrapperImpl) {
              String leftDmName = ((DataMapExprWrapperImpl)left).getDataMap()
                  .getDataMapSchema().getDataMapName();
              String rightDmName = ((DataMapExprWrapperImpl)right)
                  .getDataMap().getDataMapSchema().getDataMapName();
              if (leftDmName.equalsIgnoreCase(rightDmName)) {
                FilterResolverIntf resolver = CarbonTable.resolveFilter(new AndExpression(
                    left.getFilterResolverIntf().getFilterExpression(),
                    right.getFilterResolverIntf().getFilterExpression()),
                    carbonTable.getAbsoluteTableIdentifier());
                return new DataMapExprWrapperImpl(
                    ((DataMapExprWrapperImpl)left).getDataMap(), resolver);
              }
            }
            // Else apply AND expression
            DataMapExprWrapper andDataMapExprWrapper = new AndDataMapExprWrapper(left,
                right, filterResolverIntf); // TODO: is this filterResolverIntf has no used?
            return andDataMapExprWrapper;
          } else if (left != null) {
            return left;
          } else {
            return right;
          }
        }
        break;
      case OR:
        if (expression instanceof OrExpression) {
          OrExpression orExpression = (OrExpression) expression;
          DataMapExprWrapper left = selectDataMap(orExpression.getLeft(), allDataMap,
              filterResolverIntf.getLeft());
          DataMapExprWrapper right = selectDataMap(orExpression.getRight(), allDataMap,
              filterResolverIntf.getRight());
          if (left != null && right != null) {
            // Apply OR expression
            DataMapExprWrapper orDataMapExprWrapper = new OrDataMapExprWrapper(left,
                right, filterResolverIntf);
            return orDataMapExprWrapper;
          } else {
            return null;
          }
        }
        break;
      default:
        TableDataMap dataMap = chooseDataMap(allDataMap, expression);
        if (dataMap != null) {
          return new DataMapExprWrapperImpl(dataMap, filterResolverIntf);
        }
        break;
    }
    return null;
  }

  private TableDataMap chooseDataMap(List<TableDataMap> allDataMap, Expression expression) {
    List<DataMapTuple> tuples = new ArrayList<>();
    for (TableDataMap dataMap : allDataMap) {
      if (null != dataMap.getDataMapFactory().getMeta() &&
          dataMap.getDataMapFactory().isSupport(expression)) {
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
