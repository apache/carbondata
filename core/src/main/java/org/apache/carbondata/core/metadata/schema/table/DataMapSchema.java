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

import org.apache.carbondata.common.Strings;
import org.apache.carbondata.common.exceptions.sql.MalformedIndexCommandException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.IndexUtil;
import org.apache.carbondata.core.datamap.status.DataMapStatus;
import org.apache.carbondata.core.datamap.status.DataMapStatusDetail;
import org.apache.carbondata.core.datamap.status.DataMapStatusManager;
import org.apache.carbondata.core.datamap.status.MVSegmentStatusUtil;
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider;
import org.apache.carbondata.core.metadata.schema.datamap.DataMapProperty;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;

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
   * 1. Index Index: provider name is class name of implementation class of IndexFactory
   * 2. MV Index: provider name is class name of {@code MVDataMapProvider}
   */
  // the old version the field name for providerName was className, so to de-serialization
  // old schema provided the old field name in the alternate filed using annotation
  @SerializedName(value = "providerName", alternate = "className")
  protected String providerName;

  /**
   * For MV, this is the identifier of the MV table.
   * For Index, this is the identifier of the main table.
   */
  protected RelationIdentifier relationIdentifier;

  /**
   * SQL query string used to create MV
   */
  protected String ctasQuery;

  /**
   * Properties provided by user
   */
  protected Map<String, String> properties;

  /**
   * Identifiers of parent tables of the MV
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
   * Index table column order map as per Select query
   */
  private Map<Integer, String> columnsOrderMap;

  /**
   * timeseries query
   */
  private boolean isTimeSeries;

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
   * Return true if this is an Index
   * @return
   */
  public boolean isIndex() {
    if (providerName.equalsIgnoreCase(DataMapClassProvider.MV.getShortName()) ||
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
      throws MalformedIndexCommandException {
    String columns = getProperties().get(INDEX_COLUMNS);
    if (columns == null) {
      columns = getProperties().get(INDEX_COLUMNS.toLowerCase());
    }
    if (columns == null) {
      throw new MalformedIndexCommandException(INDEX_COLUMNS + " DMPROPERTY is required");
    } else if (StringUtils.isBlank(columns)) {
      throw new MalformedIndexCommandException(INDEX_COLUMNS + " DMPROPERTY is blank");
    } else {
      return columns.split(",", -1);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DataMapSchema that = (DataMapSchema) o;
    return Objects.equals(dataMapName, that.dataMapName);
  }

  @Override
  public int hashCode() {
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

  public boolean isTimeSeries() {
    return isTimeSeries;
  }

  public void setTimeSeries(boolean timeSeries) {
    isTimeSeries = timeSeries;
  }

  /**
   * Return true if this Index can support incremental build
   */
  public boolean canBeIncrementalBuild() {
    String prop = getProperties().get(DataMapProperty.FULL_REFRESH);
    return prop == null || prop.equalsIgnoreCase("false");
  }

  public String getPropertiesAsString() {
    String[] properties = getProperties().entrySet().stream()
        // ignore internal used property
        .filter(p ->
            !p.getKey().equalsIgnoreCase(DataMapProperty.DEFERRED_REBUILD) &&
            !p.getKey().equalsIgnoreCase(DataMapProperty.FULL_REFRESH))
        .map(p -> "'" + p.getKey() + "'='" + p.getValue() + "'")
        .sorted()
        .toArray(String[]::new);
    return Strings.mkString(properties, ",");
  }

  public String getUniqueTableName() {
    return relationIdentifier.getDatabaseName() + CarbonCommonConstants.POINT +
        relationIdentifier.getTableName();
  }

  public DataMapStatus getStatus() throws IOException {
    DataMapStatusDetail[] details = DataMapStatusManager.getEnabledDataMapStatusDetails();
    for (DataMapStatusDetail detail : details) {
      if (detail.getDataMapName().equalsIgnoreCase(this.getDataMapName())) {
        return DataMapStatus.ENABLED;
      }
    }
    return DataMapStatus.DISABLED;
  }

  public String getSyncStatus() {
    LoadMetadataDetails[] loads =
        SegmentStatusManager.readLoadMetadata(
            CarbonTablePath.getMetadataPath(this.getRelationIdentifier().getTablePath()));
    if (!isIndex() && loads.length > 0) {
      for (int i = loads.length - 1; i >= 0; i--) {
        LoadMetadataDetails load = loads[i];
        if (load.getSegmentStatus().equals(SegmentStatus.SUCCESS)) {
          Map<String, List<String>> segmentMaps =
              MVSegmentStatusUtil.getSegmentMap(load.getExtraInfo());
          Map<String, String> syncInfoMap = new HashMap<>();
          for (Map.Entry<String, List<String>> entry : segmentMaps.entrySet()) {
            // when in join scenario, one table is loaded and one more is not loaded,
            // then put value as NA
            if (entry.getValue().isEmpty()) {
              syncInfoMap.put(entry.getKey(), "NA");
            } else {
              syncInfoMap.put(entry.getKey(), IndexUtil.getMaxSegmentID(entry.getValue()));
            }
          }
          String loadEndTime;
          if (load.getLoadEndTime() == CarbonCommonConstants.SEGMENT_LOAD_TIME_DEFAULT) {
            loadEndTime = "NA";
          } else {
            loadEndTime = new java.sql.Timestamp(load.getLoadEndTime()).toString();
          }
          syncInfoMap.put(CarbonCommonConstants.LOAD_SYNC_TIME, loadEndTime);
          return new Gson().toJson(syncInfoMap);
        }
      }
    }
    return "NA";
  }
}
