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
package org.apache.carbondata.core.preagg;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.metadata.schema.table.AggregationDataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

/**
 * Below class will be used to select the aggregate table based
 * query plan. Rules for selecting the aggregate table is below:
 * 1. Select all aggregate table based on projection
 * 2. select aggregate table based on filter exp,
 * 2. select if aggregate tables based on aggregate columns
 */
public class AggregateTableSelector {

  /**
   * current query plan
   */
  private QueryPlan queryPlan;

  /**
   * parent table
   */
  private CarbonTable parentTable;

  public AggregateTableSelector(QueryPlan queryPlan, CarbonTable parentTable) {
    this.queryPlan = queryPlan;
    this.parentTable = parentTable;
  }

  /**
   * Below method will be used to select pre aggregate tables based on query plan
   * Rules for selecting the aggregate table is below:
   * 1. Select all aggregate table based on projection
   * 2. select aggregate table based on filter exp,
   * 2. select if aggregate tables based on aggregate columns
   *
   * @return selected pre aggregate table schema
   */
  public List<DataMapSchema> selectPreAggDataMapSchema() {
    List<QueryColumn> projectionColumn = queryPlan.getProjectionColumn();
    List<QueryColumn> aggColumns = queryPlan.getAggregationColumns();
    List<QueryColumn> filterColumns = queryPlan.getFilterColumns();
    List<DataMapSchema> dataMapSchemaList = parentTable.getTableInfo().getDataMapSchemaList();
    List<DataMapSchema> selectedDataMapSchema = new ArrayList<>();
    boolean isMatch;
    // match projection columns
    if (null != projectionColumn && !projectionColumn.isEmpty()) {
      for (DataMapSchema dmSchema : dataMapSchemaList) {
        AggregationDataMapSchema aggregationDataMapSchema = (AggregationDataMapSchema) dmSchema;
        isMatch = true;
        for (QueryColumn queryColumn : projectionColumn) {
          ColumnSchema columnSchemaByParentName =
              getColumnSchema(queryColumn, aggregationDataMapSchema);
          if (null == columnSchemaByParentName) {
            isMatch = false;
          }
        }
        if (isMatch) {
          selectedDataMapSchema.add(dmSchema);
        }
      }
      // if projection column is present but selected table list size is zero then
      if (selectedDataMapSchema.size() == 0) {
        return selectedDataMapSchema;
      }
    }

    // match filter columns
    if (null != filterColumns && !filterColumns.isEmpty()) {
      List<DataMapSchema> dmSchemaToIterate =
          selectedDataMapSchema.isEmpty() ? dataMapSchemaList : selectedDataMapSchema;
      selectedDataMapSchema = new ArrayList<>();
      for (DataMapSchema dmSchema : dmSchemaToIterate) {
        isMatch = true;
        for (QueryColumn queryColumn : filterColumns) {
          AggregationDataMapSchema aggregationDataMapSchema = (AggregationDataMapSchema) dmSchema;
          ColumnSchema columnSchemaByParentName =
              getColumnSchema(queryColumn, aggregationDataMapSchema);
          if (null == columnSchemaByParentName) {
            isMatch = false;
          }
        }
        if (isMatch) {
          selectedDataMapSchema.add(dmSchema);
        }
      }
      // if filter column is present and selection size is zero then return
      if (selectedDataMapSchema.size() == 0) {
        return selectedDataMapSchema;
      }
    }
    // match aggregation columns
    if (null != aggColumns && !aggColumns.isEmpty()) {
      List<DataMapSchema> dmSchemaToIterate =
          selectedDataMapSchema.isEmpty() ? dataMapSchemaList : selectedDataMapSchema;
      selectedDataMapSchema = new ArrayList<>();
      for (DataMapSchema dmSchema : dmSchemaToIterate) {
        isMatch = true;
        for (QueryColumn queryColumn : aggColumns) {
          AggregationDataMapSchema aggregationDataMapSchema = (AggregationDataMapSchema) dmSchema;
          if (!aggregationDataMapSchema
              .isColumnWithAggFunctionExists(queryColumn.getColumnSchema().getColumnName(),
                  queryColumn.getAggFunction())) {
            isMatch = false;
          }
        }
        if (isMatch) {
          selectedDataMapSchema.add(dmSchema);
        }
      }
    }
    return selectedDataMapSchema;
  }

  /**
   * Below method will be used to get column schema for projection and
   * filter query column
   *
   * @param queryColumn              query column
   * @param aggregationDataMapSchema selected data map schema
   * @return column schema
   */
  private ColumnSchema getColumnSchema(QueryColumn queryColumn,
      AggregationDataMapSchema aggregationDataMapSchema) {
    ColumnSchema columnSchemaByParentName = null;
    if (!queryColumn.getTimeseriesFunction().isEmpty()) {
      columnSchemaByParentName = aggregationDataMapSchema
          .getTimeseriesChildColBasedByParent(queryColumn.getColumnSchema().getColumnName(),
              queryColumn.getTimeseriesFunction());
    } else {
      columnSchemaByParentName = aggregationDataMapSchema
          .getNonAggNonTimeseriesChildColBasedByParent(
              queryColumn.getColumnSchema().getColumnName());
    }
    return columnSchemaByParentName;
  }
}
