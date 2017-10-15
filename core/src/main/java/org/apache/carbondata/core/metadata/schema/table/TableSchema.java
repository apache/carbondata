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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.schema.BucketingInfo;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.SchemaEvolution;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnRelation;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ParentColumnTableRelation;

/**
 * Persisting the table information
 */
public class TableSchema implements Serializable, Writable {

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

  /**
   * Information about partition type, partition column, numbers
   */
  private PartitionInfo partitionInfo;

  /**
   * to maintain the relation between child to parent column
   * relation this will be used during dataloading and query
   * to select parent table columns
   */
  private List<ColumnRelation> columnRelations;

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

  public List<ColumnRelation> getColumnRelations() {
    return columnRelations;
  }

  public void setColumnRelations(List<ColumnRelation> columnRelations) {
    this.columnRelations = columnRelations;
  }

  /**
   * @param tableName the tableName to set
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
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

  public PartitionInfo getPartitionInfo() {
    return partitionInfo;
  }

  public void setPartitionInfo(PartitionInfo partitionInfo) {
    this.partitionInfo = partitionInfo;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(tableId);
    out.writeUTF(tableName);
    out.writeInt(listOfColumns.size());
    for (ColumnSchema column : listOfColumns) {
      column.write(out);
    }

    if (null != partitionInfo) {
      out.writeBoolean(true);
      partitionInfo.write(out);
    } else {
      out.writeBoolean(false);
    }
    if (null != bucketingInfo) {
      out.writeBoolean(true);
      bucketingInfo.write(out);
    } else {
      out.writeBoolean(false);
    }
    // code to read column relation in case of aggregate table
    // to maintain the relation between child table column
    // parent table column
    boolean isColumnRelationsExists = null != this.columnRelations;
    if (isColumnRelationsExists) {
      out.writeShort(this.columnRelations.size());
      for (int i = 0; i < this.columnRelations.size(); i++) {
        this.columnRelations.get(i).write(out);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.tableId = in.readUTF();
    this.tableName = in.readUTF();
    int listSize = in.readInt();
    this.listOfColumns = new ArrayList<>(listSize);
    for (int i = 0; i < listSize; i++) {
      ColumnSchema schema = new ColumnSchema();
      schema.readFields(in);
      this.listOfColumns.add(schema);
    }

    boolean partitionExists = in.readBoolean();
    if (partitionExists) {
      this.partitionInfo = new PartitionInfo();
      this.partitionInfo.readFields(in);
    }

    boolean bucketingExists = in.readBoolean();
    if (bucketingExists) {
      this.bucketingInfo = new BucketingInfo();
      this.bucketingInfo.readFields(in);
    }
    // code to read column relation in case of aggregate table
    // to maintain the relation between child table column
    // parent table column
    boolean isColumnRelationExists = in.readBoolean();
    if (isColumnRelationExists) {
      short numberOfColumnRelation = in.readShort();
      this.columnRelations = new ArrayList<>();
      for (int i = 0; i < numberOfColumnRelation; i++) {
        ColumnRelation columnRelation = new ColumnRelation(null, null);
        columnRelation.readFields(in);
        columnRelations.add(columnRelation);
      }
    }
  }

  /**
   * Below methdo will be used to build child schema object which will be stored in
   * parent table
   * @param databaseName
   *        name of the database
   * @param queryString
   *        aggergate table select query string which will be used
   *        during data loading
   *
   * @return child schema object
   */
  public ChildSchema buildChildSchema(String databaseName, String queryString) {
    RelationIdentifier relationIdentifier =
        new RelationIdentifier(databaseName, tableName, tableId);
    Map<String, String> properties = new HashMap<>();
    properties.put("CHILD_SELECT QUERY", queryString);
    RelationProperties relationProperties =
        new RelationProperties(RelationType.AGGREGATION, properties);
    return new ChildSchema(relationIdentifier, this, relationProperties);
  }

  public List<ParentColumnTableRelation> getParentColumnTableRelation(String columnId) {
    if (null != columnRelations) {
      for (ColumnRelation columnRelation : columnRelations) {
        if (columnRelation.getColumnId().equals(columnId)) {
          if (columnRelation.getColumnTableRelationList().size() == 1) {
            return columnRelation.getColumnTableRelationList();
          }
        }
      }
    }
    return null;
  }
}
