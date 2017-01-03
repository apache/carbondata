/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.core.carbon.metadata.schema.table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.carbon.metadata.schema.BucketingInfo;
import org.apache.carbondata.core.carbon.metadata.schema.SchemaEvolution;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.constants.CarbonCommonConstants;

/**
 * Persisting the table information
 */
public class TableSchema implements Serializable {

  /**
   * serialization version
   */
  private static final long serialVersionUID = -1928614587722507026L;

  /**
   * table id
   */
  private String tableId;

  /**
   * table Name
   */
  private String tableName;

  /**
   * Columns in the table
   */
  private List<ColumnSchema> listOfColumns;

  /**
   * History of schema evolution of this table
   */
  private SchemaEvolution schemaEvalution;

  /**
   * contains all key value pairs for table properties set by user in craete DDL
   */
  private Map<String, String> tableProperties;

  /**
   * Information about bucketing of fields and number of buckets
   */
  private BucketingInfo bucketingInfo;

  public TableSchema() {
    this.listOfColumns = new ArrayList<ColumnSchema>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  }

  /**
   * @return the tableId
   */
  public String getTableId() {
    return tableId;
  }

  /**
   * @param tableId the tableId to set
   */
  public void setTableId(String tableId) {
    this.tableId = tableId;
  }

  /**
   * @return the listOfColumns
   */
  public List<ColumnSchema> getListOfColumns() {
    return listOfColumns;
  }

  /**
   * @param listOfColumns the listOfColumns to set
   */
  public void setListOfColumns(List<ColumnSchema> listOfColumns) {
    this.listOfColumns = listOfColumns;
  }

  /**
   * @return the schemaEvalution
   */
  public SchemaEvolution getSchemaEvalution() {
    return schemaEvalution;
  }

  /**
   * @param schemaEvalution the schemaEvalution to set
   */
  public void setSchemaEvalution(SchemaEvolution schemaEvalution) {
    this.schemaEvalution = schemaEvalution;
  }

  /**
   * @return the tableName
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName the tableName to set
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * to get the column schema present in the table by name
   *
   * @param columnName
   * @return column schema if matches the name
   */
  public ColumnSchema getColumnSchemaByName(String columnName) {
    for (ColumnSchema tableColumn : listOfColumns) {
      if (tableColumn.getColumnName().equals(columnName)) {
        return tableColumn;
      }
    }
    return null;
  }

  /**
   * to get the column schema present in the table by unique id
   *
   * @param columnUniqueId
   * @return column schema if matches the id
   */
  public ColumnSchema getColumnSchemaById(String columnUniqueId) {
    for (ColumnSchema tableColumn : listOfColumns) {
      if (tableColumn.getColumnUniqueId().equalsIgnoreCase(columnUniqueId)) {
        return tableColumn;
      }
    }
    return null;
  }

  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((tableId == null) ? 0 : tableId.hashCode());
    result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
    return result;
  }

  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TableSchema other = (TableSchema) obj;
    if (tableId == null) {
      if (other.tableId != null) {
        return false;
      }
    } else if (!tableId.equals(other.tableId)) {
      return false;
    }
    if (tableName == null) {
      if (other.tableName != null) {
        return false;
      }
    } else if (!tableName.equals(other.tableName)) {

      return false;
    }
    return true;
  }

  /**
   * @return
   */
  public Map<String, String> getTableProperties() {
    return tableProperties;
  }

  /**
   * @param tableProperties
   */
  public void setTableProperties(Map<String, String> tableProperties) {
    this.tableProperties = tableProperties;
  }

  public BucketingInfo getBucketingInfo() {
    return bucketingInfo;
  }

  public void setBucketingInfo(BucketingInfo bucketingInfo) {
    this.bucketingInfo = bucketingInfo;
  }
}
