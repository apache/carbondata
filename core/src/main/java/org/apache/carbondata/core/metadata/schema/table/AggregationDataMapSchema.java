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

package org.apache.carbondata.core.metadata.schema.table;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ParentColumnTableRelation;
import org.apache.carbondata.core.preagg.TimeSeriesFunctionEnum;

/**
 * data map schema class for pre aggregation
 */
public class AggregationDataMapSchema extends DataMapSchema {

  private static final long serialVersionUID = 5900935117929888412L;
  /**
   * map of parent column name to set of child column column without
   * aggregation function
   */
  private Map<String, Set<ColumnSchema>> parentToNonAggChildMapping;

  /**
   * map of parent column name to set of child columns column with
   * aggregation function
   */
  private Map<String, Set<ColumnSchema>> parentToAggChildMapping;

  /**
   * map of parent column name to set of aggregation function applied in
   * in parent column
   */
  private Map<String, Set<String>> parentColumnToAggregationsMapping;

  /**
   * whether its a timeseries data map
   */
  private boolean isTimeseriesDataMap;

  /**
   * below ordinal will be used during sorting the data map
   * to support rollup for loading
   */
  private int ordinal = Integer.MAX_VALUE;

  // Dont remove transient otherwise serialization for carbonTable will fail using
  // JavaSerialization in spark.
  private transient Set aggExpToColumnMapping;

  AggregationDataMapSchema(String dataMapName, String className) {
    super(dataMapName, className);
  }

  public void setChildSchema(TableSchema childSchema) {
    super.setChildSchema(childSchema);
    List<ColumnSchema> listOfColumns = getChildSchema().getListOfColumns();
    fillNonAggFunctionColumns(listOfColumns);
    fillAggFunctionColumns(listOfColumns);
    fillParentNameToAggregationMapping(listOfColumns);
  }

  /**
   * Below method will be used to get the columns on which aggregate function
   * and time series function is not applied
   * @param columnName
   *                parent column name
   * @return child column schema
   */
  public ColumnSchema getNonAggNonTimeseriesChildColBasedByParent(String columnName) {
    Set<ColumnSchema> columnSchemas = parentToNonAggChildMapping.get(columnName);
    if (null != columnSchemas) {
      Iterator<ColumnSchema> iterator = columnSchemas.iterator();
      while (iterator.hasNext()) {
        ColumnSchema next = iterator.next();
        if ((null == next.getAggFunction() || next.getAggFunction().isEmpty()) && null == next
            .getTimeSeriesFunction() || next.getTimeSeriesFunction().isEmpty()) {
          return next;
        }
      }
    }
    return null;
  }

  /**
   * Below method will be used to get the columns on which aggregate function is not applied
   * @param columnName
   *                parent column name
   * @return child column schema
   */
  public ColumnSchema getNonAggChildColBasedByParent(String columnName) {
    Set<ColumnSchema> columnSchemas = parentToNonAggChildMapping.get(columnName);
    if (null != columnSchemas) {
      Iterator<ColumnSchema> iterator = columnSchemas.iterator();
      while (iterator.hasNext()) {
        ColumnSchema next = iterator.next();
        if ((null == next.getAggFunction() || next.getAggFunction().isEmpty())) {
          return next;
        }
      }
    }
    return null;
  }

  /**
   * Below method will be used to get the columns on which aggregate function is not applied
   *
   * @param columnName parent column name
   * @return child column schema
   */
  public ColumnSchema getTimeseriesChildColBasedByParent(String columnName,
      String timeseriesFunction) {
    Set<ColumnSchema> columnSchemas = parentToNonAggChildMapping.get(columnName);
    if (null != columnSchemas) {
      Iterator<ColumnSchema> iterator = columnSchemas.iterator();
      while (iterator.hasNext()) {
        ColumnSchema next = iterator.next();
        if (timeseriesFunction.equals(next.getTimeSeriesFunction())) {
          return next;
        }
      }
    }
    return null;
  }

  /**
   * Below method will be used to get the column schema based on parent column name
   * @param columName
   *                parent column name
   * @return child column schema
   */
  public ColumnSchema getChildColByParentColName(String columName) {
    List<ColumnSchema> listOfColumns = childSchema.getListOfColumns();
    for (ColumnSchema columnSchema : listOfColumns) {
      List<ParentColumnTableRelation> parentColumnTableRelations =
          columnSchema.getParentColumnTableRelations();
      if (null != parentColumnTableRelations && parentColumnTableRelations.size() == 1
          && parentColumnTableRelations.get(0).getColumnName().equalsIgnoreCase(columName) &&
          columnSchema.getColumnName().endsWith(columName)) {
        return columnSchema;
      }
    }
    return null;
  }

  /**
   * Below method will be used to get the child column schema based on parent name and aggregate
   * function applied on column
   * @param columnName
   *                  parent column name
   * @param aggFunction
   *                  aggregate function applied
   * @return child column schema
   */
  public ColumnSchema getAggChildColByParent(String columnName,
      String aggFunction) {
    Set<ColumnSchema> columnSchemas = parentToAggChildMapping.get(columnName);
    if (null != columnSchemas) {
      Iterator<ColumnSchema> iterator = columnSchemas.iterator();
      while (iterator.hasNext()) {
        ColumnSchema next = iterator.next();
        if (null != next.getAggFunction() && next.getAggFunction().equalsIgnoreCase(aggFunction)) {
          return next;
        }
      }
    }
    return null;
  }

  /**
   * Below method will be used to get the column schema based on parent column name
   * @param columName
   *                parent column name
   * @param timeseriesFunction
   *                timeseries function applied on column
   * @return child column schema
   */
  public ColumnSchema getTimeseriesChildColByParent(String columName, String timeseriesFunction) {
    List<ColumnSchema> listOfColumns = childSchema.getListOfColumns();
    for (ColumnSchema columnSchema : listOfColumns) {
      List<ParentColumnTableRelation> parentColumnTableRelations =
          columnSchema.getParentColumnTableRelations();
      if (null != parentColumnTableRelations && parentColumnTableRelations.size() == 1
          && parentColumnTableRelations.get(0).getColumnName().equalsIgnoreCase(columName)
          && timeseriesFunction.equalsIgnoreCase(columnSchema.getTimeSeriesFunction())) {
        return columnSchema;
      }
    }
    return null;
  }

  /**
   * Method to prepare mapping of parent to list of aggregation function applied on that column
   * @param listOfColumns
   *        child column schema list
   */
  private void fillParentNameToAggregationMapping(List<ColumnSchema> listOfColumns) {
    parentColumnToAggregationsMapping = new HashMap<>();
    for (ColumnSchema column : listOfColumns) {
      if (null != column.getAggFunction() && !column.getAggFunction().isEmpty()) {
        List<ParentColumnTableRelation> parentColumnTableRelations =
            column.getParentColumnTableRelations();
        if (null != parentColumnTableRelations && parentColumnTableRelations.size() == 1) {
          String columnName = column.getParentColumnTableRelations().get(0).getColumnName();
          Set<String> aggFunctions = parentColumnToAggregationsMapping.get(columnName);
          if (null == aggFunctions) {
            aggFunctions = new HashSet<>();
            parentColumnToAggregationsMapping.put(columnName, aggFunctions);
          }
          aggFunctions.add(column.getAggFunction());
        }
      }
    }
  }

  /**
   * Below method will be used prepare mapping between parent column to non aggregation function
   * columns
   * @param listOfColumns
   *                    list of child columns
   */
  private void fillNonAggFunctionColumns(List<ColumnSchema> listOfColumns) {
    parentToNonAggChildMapping = new HashMap<>();
    for (ColumnSchema column : listOfColumns) {
      if (!isTimeseriesDataMap) {
        isTimeseriesDataMap =
            null != column.getTimeSeriesFunction() && !column.getTimeSeriesFunction().isEmpty();
        if (isTimeseriesDataMap) {
          this.ordinal =
              TimeSeriesFunctionEnum.valueOf(column.getTimeSeriesFunction().toUpperCase())
                  .getOrdinal();
        }
      }
      if (null == column.getAggFunction() || column.getAggFunction().isEmpty()) {
        fillMappingDetails(column, parentToNonAggChildMapping);
      }
    }
  }

  private void fillMappingDetails(ColumnSchema column,
      Map<String, Set<ColumnSchema>> map) {
    List<ParentColumnTableRelation> parentColumnTableRelations =
        column.getParentColumnTableRelations();
    if (null != parentColumnTableRelations && parentColumnTableRelations.size() == 1) {
      String columnName = column.getParentColumnTableRelations().get(0).getColumnName();
      Set<ColumnSchema> columnSchemas = map.get(columnName);
      if (null == columnSchemas) {
        columnSchemas = new HashSet<>();
        map.put(columnName, columnSchemas);
      }
      columnSchemas.add(column);
    }
  }

  /**
   * Below method will be used to fill parent to list of aggregation column mapping
   * @param listOfColumns
   *        list of child columns
   */
  private void fillAggFunctionColumns(List<ColumnSchema> listOfColumns) {
    parentToAggChildMapping = new HashMap<>();
    for (ColumnSchema column : listOfColumns) {
      if (null != column.getAggFunction() && !column.getAggFunction().isEmpty()) {
        fillMappingDetails(column, parentToAggChildMapping);
      }
    }
  }

  public boolean isTimeseriesDataMap() {
    return isTimeseriesDataMap;
  }

  /**
   * Below method is to support rollup during loading the data in pre aggregate table
   * In case of timeseries year level table data loading can be done using month level table or any
   * time series level below year level for example day,hour minute, second.
   * @TODO need to handle for pre aggregate table without timeseries
   *
   * @param aggregationDataMapSchema
   * @return whether aggregation data map can be selected or not
   */
  public boolean canSelectForRollup(AggregationDataMapSchema aggregationDataMapSchema) {
    List<ColumnSchema> listOfColumns = childSchema.getListOfColumns();
    for (ColumnSchema column : listOfColumns) {
      List<ParentColumnTableRelation> parentColumnTableRelations =
          column.getParentColumnTableRelations();
      //@TODO handle scenario when aggregate datamap columns is derive from multiple column
      // which is not supported currently
      if (null != parentColumnTableRelations && parentColumnTableRelations.size() == 1) {
        if (null != column.getAggFunction() && !column.getAggFunction().isEmpty()) {
          if (null == aggregationDataMapSchema
              .getAggChildColByParent(parentColumnTableRelations.get(0).getColumnName(),
                  column.getAggFunction())) {
            return false;
          }
        } else {
          if (null == aggregationDataMapSchema.getNonAggChildColBasedByParent(
              parentColumnTableRelations.get(0).getColumnName())) {
            return false;
          }
        }
      } else {
        // in case of any expression one column can be derived from multiple column
        // in that case we cannot do rollup so hit the maintable
        return false;
      }
    }
    return true;
  }

  public int getOrdinal() {
    return ordinal;
  }

  /**
   * Below method will be used to get the aggregation column based on index
   * It will return the first aggregation column found based on index
   * @param searchStartIndex
   *  start index
   * @param sortedColumnSchema
   * list of sorted table columns
   * @return found column list
   *
   */
  public ColumnSchema getAggColumnBasedOnIndex(int searchStartIndex,
      List<ColumnSchema> sortedColumnSchema) {
    ColumnSchema columnSchema = null;
    for (int i = searchStartIndex; i < sortedColumnSchema.size(); i++) {
      if (!sortedColumnSchema.get(i).getAggFunction().isEmpty()) {
        columnSchema = sortedColumnSchema.get(i);
        break;
      }
    }
    return columnSchema;
  }

  public synchronized Set getAggExpToColumnMapping() {
    return aggExpToColumnMapping;
  }

  public synchronized void setAggExpToColumnMapping(Set aggExpToColumnMapping) {
    if (null == this.aggExpToColumnMapping) {
      this.aggExpToColumnMapping = aggExpToColumnMapping;
    }
  }

  public DataMapClassProvider getProvider() {
    return isTimeseriesDataMap ?
        DataMapClassProvider.TIMESERIES : DataMapClassProvider.PREAGGREGATE;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    AggregationDataMapSchema that = (AggregationDataMapSchema) o;
    return that == this;
  }

  @Override public int hashCode() {
    return super.hashCode();
  }
}
