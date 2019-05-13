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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider;
import org.apache.carbondata.core.metadata.schema.datamap.DataMapProperty;

import static org.apache.carbondata.core.constants.CarbonCommonConstants.INDEX_COLUMNS;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.StringUtils;

/**
 * It is the new schama of datamap and it has less fields compare to {{@link DataMapSchema}}
 */
public class DataMapSchema implements Serializable, Writable {

  private static final long serialVersionUID = -8394577999061329687L;

  protected String dataMapName;

  /**
   * There are two kind of DataMaps:
   * 1. Index DataMap: provider name is class name of implementation class of DataMapFactory
   * 2. OLAP DataMap: provider name is one of the {@link DataMapClassProvider#shortName}
   */
  // the old version the field name for providerName was className, so to de-serialization
  // old schema provided the old field name in the alternate filed using annotation
  @SerializedName(value = "providerName", alternate = "className")
  protected String providerName;

  /**
   * identifiers of the mapped table
   */
  protected RelationIdentifier relationIdentifier;

  /**
   * Query which is used to create a datamap. This is optional in case of index datamap.
   */
  protected String ctasQuery;

  /**
   * relation properties
   */
  protected Map<String, String> properties;

  /**
   * Identifiers of parent tables
   */
  protected List<RelationIdentifier> parentTables;

  /**
   * child table schema
   */
  protected TableSchema childSchema;

  /**
   * main table column list mapped to datamap table
   */
  private Map<String, Set<String>> mainTableColumnList;

  /**
   * DataMap table column order map as per Select query
   */
  private Map<Integer, String> columnsOrderMap;

  public DataMapSchema(String dataMapName, String providerName) {
    this.dataMapName = dataMapName;
    this.providerName = providerName;
  }

  public DataMapSchema() {
  }

  public String getDataMapName() {
    return dataMapName;
  }

  public void setDataMapName(String dataMapName) {
    this.dataMapName = dataMapName;
  }

  public String getProviderName() {
    return providerName;
  }

  public void setProviderName(String providerName) {
    this.providerName = providerName;
  }

  public RelationIdentifier getRelationIdentifier() {
    return relationIdentifier;
  }

  public void setRelationIdentifier(RelationIdentifier relationIdentifier) {
    this.relationIdentifier = relationIdentifier;
  }

  public String getCtasQuery() {
    return ctasQuery;
  }

  public void setCtasQuery(String ctasQuery) {
    this.ctasQuery = ctasQuery;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public void setPropertiesJson(Gson gson, String propertiesJson) {
    if (propertiesJson != null) {
      this.properties = gson.fromJson(propertiesJson, Map.class);
    }
  }

  public void setParentTables(List<RelationIdentifier> parentTables) {
    this.parentTables = parentTables;
  }

  public List<RelationIdentifier> getParentTables() {
    return parentTables;
  }

  public TableSchema getChildSchema() {
    return childSchema;
  }

  public void setChildSchema(TableSchema childSchema) {
    this.childSchema = childSchema;
  }

  /**
   * Return true if this datamap is an Index DataMap
   * @return
   */
  public boolean isIndexDataMap() {
    if (providerName.equalsIgnoreCase(DataMapClassProvider.PREAGGREGATE.getShortName()) ||
        providerName.equalsIgnoreCase(DataMapClassProvider.TIMESERIES.getShortName()) ||
        providerName.equalsIgnoreCase(DataMapClassProvider.MV.getShortName()) ||
        ctasQuery != null) {
      return false;
    } else {
      return true;
    }
  }

  /**
   * Return true if this datamap is lazy (created with DEFERRED REBUILD syntax)
   */
  public boolean isLazy() {
    String deferredRebuild = getProperties().get(DataMapProperty.DEFERRED_REBUILD);
    return deferredRebuild != null && deferredRebuild.equalsIgnoreCase("true");
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(dataMapName);
    out.writeUTF(providerName);
    boolean isRelationIdentifierExists = null != relationIdentifier;
    out.writeBoolean(isRelationIdentifierExists);
    if (isRelationIdentifierExists) {
      this.relationIdentifier.write(out);
    }
    boolean isChildSchemaExists = null != this.childSchema;
    out.writeBoolean(isChildSchemaExists);
    if (isChildSchemaExists) {
      this.childSchema.write(out);
    }
    if (properties == null) {
      out.writeShort(0);
    } else {
      out.writeShort(properties.size());
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        out.writeUTF(entry.getKey());
        out.writeUTF(entry.getValue());
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.dataMapName = in.readUTF();
    this.providerName = in.readUTF();
    boolean isRelationIdentifierExists = in.readBoolean();
    if (isRelationIdentifierExists) {
      this.relationIdentifier = new RelationIdentifier(null, null, null);
      this.relationIdentifier.readFields(in);
    }
    boolean isChildSchemaExists = in.readBoolean();
    if (isChildSchemaExists) {
      this.childSchema = new TableSchema();
      this.childSchema.readFields(in);
    }

    int mapSize = in.readShort();
    this.properties = new HashMap<>(mapSize);
    for (int i = 0; i < mapSize; i++) {
      String key = in.readUTF();
      String value = in.readUTF();
      this.properties.put(key, value);
    }
  }

  /**
   * Return the list of column name
   */
  public String[] getIndexColumns()
      throws MalformedDataMapCommandException {
    String columns = getProperties().get(INDEX_COLUMNS);
    if (columns == null) {
      columns = getProperties().get(INDEX_COLUMNS.toLowerCase());
    }
    if (columns == null) {
      throw new MalformedDataMapCommandException(INDEX_COLUMNS + " DMPROPERTY is required");
    } else if (StringUtils.isBlank(columns)) {
      throw new MalformedDataMapCommandException(INDEX_COLUMNS + " DMPROPERTY is blank");
    } else {
      return columns.split(",", -1);
    }
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DataMapSchema that = (DataMapSchema) o;
    return Objects.equals(dataMapName, that.dataMapName);
  }

  @Override public int hashCode() {
    return Objects.hash(dataMapName);
  }

  public Map<String, Set<String>> getMainTableColumnList() {
    return mainTableColumnList;
  }

  public void setMainTableColumnList(Map<String, Set<String>> mainTableColumnList) {
    this.mainTableColumnList = mainTableColumnList;
  }

  public Map<Integer, String> getColumnsOrderMap() {
    return columnsOrderMap;
  }

  public void setColumnsOrderMap(Map<Integer, String> columnsOrderMap) {
    this.columnsOrderMap = columnsOrderMap;
  }
}
