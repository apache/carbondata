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
package org.apache.carbondata.core.datamap.dev;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapLevel;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.TableDataMap;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.events.Event;

import org.apache.commons.lang.StringUtils;

/**
 * Interface for datamap of index type, it is responsible for creating the datamap.
 */
public abstract class IndexDataMap<T extends DataMap> {

  protected static final String INDEX_COLUMNS = "index_columns";

  /**
   * Initialization of Datamap factory with the carbonTable and datamap name
   */
  public abstract void init(CarbonTable carbonTable, DataMapSchema dataMapSchema)
      throws IOException, MalformedDataMapCommandException;

  /**
   * Return a new write for this datamap
   */
  public abstract DataMapWriter createWriter(Segment segment, String writeDirectoryPath);

  /**
   * Get the datamap for segmentid
   */
  public abstract List<T> getDataMaps(Segment segment) throws IOException;

  /**
   * Get datamaps for distributable object.
   */
  public abstract List<T> getDataMaps(DataMapDistributable distributable)
      throws IOException;

  /**
   * Get all distributable objects of a segmentid
   * @return
   */
  public abstract List<DataMapDistributable> toDistributable(Segment segment);

  /**
   *
   * @param event
   */
  public abstract void fireEvent(Event event);

  /**
   * Clears datamap of the segment
   */
  public abstract void clear(Segment segment);

  /**
   * Clear all datamaps from memory
   */
  public abstract void clear();

  /**
   * Return metadata of this datamap
   */
  public abstract DataMapMeta getMeta();

  /**
   *  Type of datamap whether it is FG or CG
   */
  public abstract DataMapLevel getDataMapType();

  /**
   * delete datamap data if any
   */
  public abstract void deleteDatamapData();

  /**
   * Validate INDEX_COLUMNS property and return a array containing index column name
   * Following will be validated
   * 1. require INDEX_COLUMNS property
   * 2. INDEX_COLUMNS can't contains illegal argument(empty, blank)
   * 3. INDEX_COLUMNS can't contains duplicate same columns
   * 4. INDEX_COLUMNS should be exists in table columns
   */
  public List<String> validateAndGetIndexedColumns(DataMapSchema dataMapSchema,
      CarbonTable carbonTable) throws MalformedDataMapCommandException, IOException {
    String columnsStr = dataMapSchema.getProperties().get(INDEX_COLUMNS);
    if (columnsStr == null || StringUtils.isBlank(columnsStr)) {
      throw new MalformedDataMapCommandException(INDEX_COLUMNS + " DMPROPERTY is required.");
    }
    String[] indexColumns = columnsStr.split(",", -1);
    for (int i = 0; i < indexColumns.length; i++) {
      indexColumns[i] = indexColumns[i].trim().toLowerCase();
    }
    for (int i = 0; i < indexColumns.length; i++) {
      if (indexColumns[i].isEmpty()) {
        throw new MalformedDataMapCommandException(INDEX_COLUMNS + " contains invalid column name");
      }
      for (int j = i + 1; j < indexColumns.length; j++) {
        if (indexColumns[i].equals(indexColumns[j])) {
          throw new MalformedDataMapCommandException(String.format(
              "%s has duplicate column '%s'", INDEX_COLUMNS, indexColumns[i]));
        }
      }
    }

    List<TableDataMap> datamaps = DataMapStoreManager.getInstance().getAllDataMap(carbonTable);
    Set<String> existingIndexColumn = new HashSet<>();
    if (datamaps.size() > 0) {
      for (TableDataMap datamap : datamaps) {
          String columns = datamap.getDataMapSchema().getProperties().get(INDEX_COLUMNS);
          existingIndexColumn.addAll(Arrays.asList(columns.split(",", -1)));
      }

      for (String column : indexColumns) {
        if (existingIndexColumn.contains(column)) {
          throw new MalformedDataMapCommandException(
              String.format("column '%s' already has datamap created", column));
        }
      }
    }

    List<String> indexedCarbonColumns = new ArrayList<>(indexColumns.length);
    for (String indexColumn : indexColumns) {
      CarbonColumn column = carbonTable.getColumnByName(carbonTable.getTableName(), indexColumn);
      if (null == column) {
        throw new MalformedDataMapCommandException(String.format(
            "column '%s' does not exist in table. Please check create DataMap statement.",
            indexColumn));
      }
      indexedCarbonColumns.add(column.getColName());
    }
    return indexedCarbonColumns;
  }

}
