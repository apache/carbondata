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

package org.apache.carbondata.core.view;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.carbondata.common.Strings;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.metadata.schema.table.Writable;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.gson.Gson;

/**
 * It is the new schama of mv and it has less fields compare to {{@link MaterializedViewSchema}}
 */
public class MaterializedViewSchema implements Serializable, Writable {

  private static final long serialVersionUID = 1030007239497486385L;

  /**
   * For MV, this is the identifier of the MV table.
   */
  protected RelationIdentifier identifier;

  /**
   * SQL query string used to create MV
   */
  private String query;

  /**
   * Properties provided by user
   */
  protected Map<String, String> properties;

  /**
   * Identifiers of parent tables of the MV
   */
  private List<RelationIdentifier> associatedTables;

  /**
   * main table column list mapped to mv table
   */
  private Map<String, Set<String>> associatedTableColumnList;

  /**
   * DataMap table column order map as per Select query
   */
  private Map<Integer, String> columnsOrderMap;

  /**
   * timeseries query
   */
  private boolean timeSeries;

  private transient volatile MaterializedViewManager manager;

  public MaterializedViewSchema(MaterializedViewManager manager) {
    this.manager = manager;
  }

  public RelationIdentifier getIdentifier() {
    return identifier;
  }

  public void setIdentifier(RelationIdentifier identifier) {
    this.identifier = identifier;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public void setAssociatedTables(List<RelationIdentifier> associatedTables) {
    this.associatedTables = associatedTables;
  }

  public List<RelationIdentifier> getAssociatedTables() {
    return associatedTables;
  }

  /**
   * Return true if this datamap is lazy (created with DEFERRED REBUILD syntax)
   */
  public boolean isRefreshOnManual() {
    String refreshTriggerMode = getProperties().get(MaterializedViewProperty.REFRESH_TRIGGER_MODE);
    return refreshTriggerMode != null &&
        refreshTriggerMode.equalsIgnoreCase(
            MaterializedViewProperty.REFRESH_TRIGGER_MODE_ON_MANUAL);
  }

  /**
   * Return true if this DataMap can support incremental build
   */
  public boolean isRefreshIncremental() {
    String refreshMode = getProperties().get(MaterializedViewProperty.REFRESH_MODE);
    return refreshMode != null && refreshMode.equalsIgnoreCase(
        MaterializedViewProperty.REFRESH_MODE_INCREMENTAL);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    boolean isRelationIdentifierExists = null != identifier;
    out.writeBoolean(isRelationIdentifierExists);
    if (isRelationIdentifierExists) {
      this.identifier.write(out);
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
    boolean isRelationIdentifierExists = in.readBoolean();
    if (isRelationIdentifierExists) {
      this.identifier = new RelationIdentifier(null, null, null);
      this.identifier.readFields(in);
    }

    int mapSize = in.readShort();
    this.properties = new HashMap<>(mapSize);
    for (int i = 0; i < mapSize; i++) {
      String key = in.readUTF();
      String value = in.readUTF();
      this.properties.put(key, value);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MaterializedViewSchema that = (MaterializedViewSchema) o;
    return Objects.equals(this.identifier, that.identifier);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.identifier);
  }

  public Map<String, Set<String>> getAssociatedTableColumns() {
    return associatedTableColumnList;
  }

  public void setAssociatedTableColumnList(Map<String, Set<String>> associatedTableColumnList) {
    this.associatedTableColumnList = associatedTableColumnList;
  }

  public Map<Integer, String> getColumnsOrderMap() {
    return columnsOrderMap;
  }

  public void setColumnsOrderMap(Map<Integer, String> columnsOrderMap) {
    this.columnsOrderMap = columnsOrderMap;
  }

  public boolean isTimeSeries() {
    return timeSeries;
  }

  public void setTimeSeries(boolean timeSeries) {
    this.timeSeries = timeSeries;
  }

  public String getPropertiesAsString() {
    String[] properties = getProperties().entrySet().stream()
        .map(p -> "'" + p.getKey() + "'='" + p.getValue() + "'")
        .sorted()
        .toArray(String[]::new);
    return Strings.mkString(properties, ",");
  }

  public MaterializedViewStatus getStatus() throws IOException {
    List<MaterializedViewStatusDetail> details = this.manager.getEnabledStatusDetails(
          this.identifier.getDatabaseName());
    for (MaterializedViewStatusDetail detail : details) {
      if (detail.getIdentifier().equals(this.getIdentifier())) {
        return MaterializedViewStatus.ENABLED;
      }
    }
    return MaterializedViewStatus.DISABLED;
  }

  public String getSyncStatus() {
    LoadMetadataDetails[] loads =
        SegmentStatusManager.readLoadMetadata(
            CarbonTablePath.getMetadataPath(this.getIdentifier().getTablePath()));
    if (loads.length > 0) {
      for (int i = loads.length - 1; i >= 0; i--) {
        LoadMetadataDetails load = loads[i];
        if (load.getSegmentStatus().equals(SegmentStatus.SUCCESS)) {
          Map<String, List<String>> segmentMaps =
              new Gson().fromJson(load.getExtraInfo(), Map.class);
          Map<String, String> syncInfoMap = new HashMap<>();
          for (Map.Entry<String, List<String>> entry : segmentMaps.entrySet()) {
            // when in join scenario, one table is loaded and one more is not loaded,
            // then put value as NA
            if (entry.getValue().isEmpty()) {
              syncInfoMap.put(entry.getKey(), "NA");
            } else {
              syncInfoMap.put(entry.getKey(), getMaxSegmentID(entry.getValue()));
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

  private static String getMaxSegmentID(List<String> segmentList) {
    double[] segment = new double[segmentList.size()];
    int i = 0;
    for (String id : segmentList) {
      segment[i] = Double.parseDouble(id);
      i++;
    }
    Arrays.sort(segment);
    String maxId = Double.toString(segment[segmentList.size() - 1]);
    if (maxId.endsWith(".0")) {
      maxId = maxId.substring(0, maxId.indexOf("."));
    }
    return maxId;
  }

  public void setManager(MaterializedViewManager manager) {
    this.manager = manager;
  }

}
