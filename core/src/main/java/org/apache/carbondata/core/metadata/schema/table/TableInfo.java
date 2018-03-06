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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ParentColumnTableRelation;

/**
 * Store the information about the table.
 * it stores the fact table as well as aggregate table present in the schema
 */
public class TableInfo implements Serializable, Writable {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(TableInfo.class.getName());

  /**
   * serialization version
   */
  private static final long serialVersionUID = -5034287968314105193L;

  /**
   * name of the database;
   */
  private String databaseName;

  /**
   * table name to group fact table and aggregate table
   */
  private String tableUniqueName;

  /**
   * fact table information
   */
  private TableSchema factTable;

  /**
   * last updated time to update the table if any changes
   */
  private long lastUpdatedTime;

  /**
   * store location of the table, it will be set in identifier.tablePath also
   */
  private String tablePath;

  // this identifier is a lazy field which will be created when it is used first time
  private AbsoluteTableIdentifier identifier;

  private List<DataMapSchema> dataMapSchemaList;

  private List<RelationIdentifier> parentRelationIdentifiers;

  public TableInfo() {
    dataMapSchemaList = new ArrayList<>();
  }

  /**
   * @return the factTable
   */
  public TableSchema getFactTable() {
    return factTable;
  }

  /**
   * @param factTable the factTable to set
   */
  public void setFactTable(TableSchema factTable) {
    this.factTable = factTable;
    updateParentRelationIdentifier();
  }

  private void updateParentRelationIdentifier() {
    Set<RelationIdentifier> parentRelationIdentifiers = new HashSet<>();
    this.parentRelationIdentifiers = new ArrayList<>();
    List<ColumnSchema> listOfColumns = this.factTable.getListOfColumns();
    for (ColumnSchema columnSchema : listOfColumns) {
      List<ParentColumnTableRelation> parentColumnTableRelations =
          columnSchema.getParentColumnTableRelations();
      if (null != parentColumnTableRelations) {
        for (int i = 0; i < parentColumnTableRelations.size(); i++) {
          parentRelationIdentifiers.add(parentColumnTableRelations.get(i).getRelationIdentifier());
        }
      }
    }
    this.parentRelationIdentifiers.addAll(parentRelationIdentifiers);
  }

  /**
   * @return the databaseName
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * @param databaseName the databaseName to set
   */
  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  /**
   * @return the tableUniqueName
   */
  public String getTableUniqueName() {
    return tableUniqueName;
  }

  /**
   * @param tableUniqueName the tableUniqueName to set
   */
  public void setTableUniqueName(String tableUniqueName) {
    this.tableUniqueName = tableUniqueName;
  }

  /**
   * @return the lastUpdatedTime
   */
  public long getLastUpdatedTime() {
    return lastUpdatedTime;
  }

  /**
   * @param lastUpdatedTime the lastUpdatedTime to set
   */
  public void setLastUpdatedTime(long lastUpdatedTime) {
    this.lastUpdatedTime = lastUpdatedTime;
  }

  public void setTablePath(String tablePath) {
    this.tablePath = tablePath;
  }

  public List<DataMapSchema> getDataMapSchemaList() {
    return dataMapSchemaList;
  }

  public void setDataMapSchemaList(List<DataMapSchema> dataMapSchemaList) {
    this.dataMapSchemaList = dataMapSchemaList;
  }

  /**
   * to generate the hash code
   */
  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((databaseName == null) ? 0 : databaseName.hashCode());
    result = prime * result + ((tableUniqueName == null) ? 0 : tableUniqueName.hashCode());
    return result;
  }

  /**
   * Overridden equals method
   */
  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof TableInfo)) {
      return false;
    }
    TableInfo other = (TableInfo) obj;
    if (null == databaseName || null == other.databaseName) {
      return false;
    }

    if (null == tableUniqueName || null == other.tableUniqueName) {
      return false;
    }

    if (!tableUniqueName.equals(other.tableUniqueName)) {
      return false;
    }
    return true;
  }

  /**
   * This method will return the table size. Default table block size will be considered
   * in case not specified by the user
   */
  int getTableBlockSizeInMB() {
    String tableBlockSize = null;
    // In case of old store there will not be any map for table properties so table properties
    // will be null
    Map<String, String> tableProperties = getFactTable().getTableProperties();
    if (null != tableProperties) {
      tableBlockSize = tableProperties.get(CarbonCommonConstants.TABLE_BLOCKSIZE);
    }
    if (null == tableBlockSize) {
      tableBlockSize = CarbonCommonConstants.BLOCK_SIZE_DEFAULT_VAL;
      LOGGER.info("Table block size not specified for " + getTableUniqueName()
          + ". Therefore considering the default value "
          + CarbonCommonConstants.BLOCK_SIZE_DEFAULT_VAL + " MB");
    }
    return Integer.parseInt(tableBlockSize);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(databaseName);
    out.writeUTF(tableUniqueName);
    factTable.write(out);
    out.writeLong(lastUpdatedTime);
    out.writeUTF(getOrCreateAbsoluteTableIdentifier().getTablePath());
    boolean isChildSchemaExists =
        null != dataMapSchemaList && dataMapSchemaList.size() > 0;
    out.writeBoolean(isChildSchemaExists);
    if (isChildSchemaExists) {
      out.writeShort(dataMapSchemaList.size());
      for (int i = 0; i < dataMapSchemaList.size(); i++) {
        dataMapSchemaList.get(i).write(out);
      }
    }
    boolean isParentTableRelationIndentifierExists =
        null != parentRelationIdentifiers && parentRelationIdentifiers.size() > 0;
    out.writeBoolean(isParentTableRelationIndentifierExists);
    if (isParentTableRelationIndentifierExists) {
      out.writeShort(parentRelationIdentifiers.size());
      for (int i = 0; i < parentRelationIdentifiers.size(); i++) {
        parentRelationIdentifiers.get(i).write(out);
      }
    }
  }

  @Override public void readFields(DataInput in) throws IOException {
    this.databaseName = in.readUTF();
    this.tableUniqueName = in.readUTF();
    this.factTable = new TableSchema();
    this.factTable.readFields(in);
    this.lastUpdatedTime = in.readLong();
    this.tablePath = in.readUTF();
    boolean isChildSchemaExists = in.readBoolean();
    this.dataMapSchemaList = new ArrayList<>();
    if (isChildSchemaExists) {
      short numberOfChildTable = in.readShort();
      for (int i = 0; i < numberOfChildTable; i++) {
        DataMapSchema childSchema = new DataMapSchema();
        childSchema.readFields(in);
        DataMapSchema dataMapSchema = DataMapSchemaFactory.INSTANCE
            .getDataMapSchema(childSchema.getDataMapName(), childSchema.getProviderName());
        dataMapSchema.setChildSchema(childSchema.getChildSchema());
        dataMapSchema.setRelationIdentifier(childSchema.getRelationIdentifier());
        dataMapSchema.setProperties(childSchema.getProperties());
        dataMapSchemaList.add(dataMapSchema);
      }
    }
    boolean isParentTableRelationIdentifierExists = in.readBoolean();
    if (isParentTableRelationIdentifierExists) {
      short parentTableIdentifiersListSize = in.readShort();
      this.parentRelationIdentifiers = new ArrayList<>();
      for (int i = 0; i < parentTableIdentifiersListSize; i++) {
        RelationIdentifier relationIdentifier = new RelationIdentifier(null, null, null);
        relationIdentifier.readFields(in);
        this.parentRelationIdentifiers.add(relationIdentifier);
      }
    }
  }

  public AbsoluteTableIdentifier getOrCreateAbsoluteTableIdentifier() {
    if (identifier == null) {
      CarbonTableIdentifier carbontableIdentifier =
          new CarbonTableIdentifier(databaseName, factTable.getTableName(), factTable.getTableId());
      identifier = AbsoluteTableIdentifier.from(tablePath, carbontableIdentifier);
    }
    return identifier;
  }

  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    this.write(new DataOutputStream(bao));
    return bao.toByteArray();
  }

  public static TableInfo deserialize(byte[] bytes) throws IOException {
    TableInfo tableInfo = new TableInfo();
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
    tableInfo.readFields(in);
    return tableInfo;
  }

  public List<RelationIdentifier> getParentRelationIdentifiers() {
    return parentRelationIdentifiers;
  }

}
