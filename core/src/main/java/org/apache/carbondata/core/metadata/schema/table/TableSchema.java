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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.schema.BucketingInfo;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.SchemaEvolution;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import com.google.gson.annotations.SerializedName;

/**
 * Persisting the table information
 */
public class TableSchema implements Serializable, Writable, Cloneable {

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
  // the old version the field name for schemaEvolution was schemaEvalution, so to de-serialization
  // old schema provided the old field name in the alternate filed using annotation
  @SerializedName(value = "schemaEvolution", alternate = "schemaEvalution")
  private SchemaEvolution schemaEvolution;

  /**
   * contains all key value pairs for table properties set by user in create DDL
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

  public TableSchema() {
    this.listOfColumns = new ArrayList<ColumnSchema>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    this.tableProperties = new HashMap<String, String>();
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
   * @return the schemaEvolution
   */
  public SchemaEvolution getSchemaEvolution() {
    return schemaEvolution;
  }

  /**
   * @param schemaEvolution the schemaEvolution to set
   */
  public void setSchemaEvolution(SchemaEvolution schemaEvolution) {
    this.schemaEvolution = schemaEvolution;
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
    if (tableName != null) {
      tableName = tableName.toLowerCase(Locale.getDefault());
    }
    this.tableName = tableName;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((tableId == null) ? 0 : tableId.hashCode());
    result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
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

    out.writeInt(tableProperties.size());
    for (Map.Entry<String, String> entry : tableProperties.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeUTF(entry.getValue());
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

    int propertySize = in.readInt();
    this.tableProperties = new HashMap<String, String>(propertySize);
    for (int i = 0; i < propertySize; i++) {
      String key = in.readUTF();
      String value = in.readUTF();
      this.tableProperties.put(key, value);
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
  }

  /**
   * Create a {@link TableSchemaBuilder} to create {@link TableSchema}
   */
  public static TableSchemaBuilder builder() {
    return new TableSchemaBuilder();
  }

  /**
   * make a deep copy of TableSchema
   */
  @Override
  public Object clone() throws CloneNotSupportedException {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(bos);
      this.write(dos);
      dos.close();
      bos.close();

      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
      TableSchema tableSchema = (TableSchema) super.clone();
      tableSchema.readFields(dis);
      return tableSchema;
    } catch (IOException e) {
      throw new RuntimeException("Error occur while cloning TableSchema", e);
    }
  }

}
