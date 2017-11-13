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

import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ParentColumnTableRelation;

/**
 * data map schema class for pre aggregation
 */
public class AggregationDataMapSchema extends DataMapSchema {

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

  public AggregationDataMapSchema(String dataMapName, String className) {
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
        if (null == next.getAggFunction() || next.getAggFunction().isEmpty()) {
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
      if (parentColumnTableRelations.get(0).getColumnName().equals(columName)) {
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
   * Below method is to check if parent column with matching aggregate function
   * @param parentColumnName
   *                    parent column name
   * @param aggFunction
   *                    aggregate function
   * @return is matching
   */
  public boolean isColumnWithAggFunctionExists(String parentColumnName, String aggFunction) {
    Set<String> aggFunctions = parentColumnToAggregationsMapping.get(parentColumnName);
    if (null != aggFunctions && aggFunctions.contains(aggFunction)) {
      return true;
    }
    return false;
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

}
