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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapLevel;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.features.TableOperation;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.events.Event;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.INDEX_COLUMNS;

import org.apache.commons.lang.StringUtils;

/**
 * Interface for datamap of index type, it is responsible for creating the datamap.
 */
public abstract class DataMapFactory<T extends DataMap> {

  protected CarbonTable carbonTable;
  protected DataMapSchema dataMapSchema;

  public DataMapFactory(CarbonTable carbonTable, DataMapSchema dataMapSchema) {
    this.carbonTable = carbonTable;
    this.dataMapSchema = dataMapSchema;
  }

  /**
   * Create a new write for this datamap, to write new data into the specified segment and shard
   */
  public abstract DataMapWriter createWriter(Segment segment, String shardName)
      throws IOException;

  /**
   * Create a new Refresher for this datamap, to rebuild the specified
   * segment and shard data in the main table.
   */
  public abstract DataMapRefresher createRefresher(Segment segment, String shardName)
      throws IOException;

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
  public abstract DataMapLevel getDataMapLevel();

  /**
   * delete datamap data if any
   */
  public abstract void deleteDatamapData();

  /**
   * This function should return true is the input operation enum will make the datamap become stale
   */
  public abstract boolean willBecomeStale(TableOperation operation);

  /**
   * Validate INDEX_COLUMNS property and return a array containing index column name
   * Following will be validated
   * 1. require INDEX_COLUMNS property
   * 2. INDEX_COLUMNS can't contains illegal argument(empty, blank)
   * 3. INDEX_COLUMNS can't contains duplicate same columns
   * 4. INDEX_COLUMNS should be exists in table columns
   */
  public void validateIndexedColumns(DataMapSchema dataMapSchema,
      CarbonTable carbonTable) throws MalformedDataMapCommandException {
    List<CarbonColumn> indexColumns = carbonTable.getIndexedColumns(dataMapSchema);
    Set<String> unique = new HashSet<>();
    for (CarbonColumn indexColumn : indexColumns) {
      unique.add(indexColumn.getColName());
    }
    if (unique.size() != indexColumns.size()) {
      throw new MalformedDataMapCommandException(INDEX_COLUMNS + " has duplicate column");
    }
  }

  public static String[] getIndexColumns(DataMapSchema dataMapSchema)
      throws MalformedDataMapCommandException {
    String columns = dataMapSchema.getProperties().get(INDEX_COLUMNS);
    if (columns == null) {
      columns = dataMapSchema.getProperties().get(INDEX_COLUMNS.toLowerCase());
    }
    if (columns == null) {
      throw new MalformedDataMapCommandException(INDEX_COLUMNS + " DMPROPERTY is required");
    } else if (StringUtils.isBlank(columns)) {
      throw new MalformedDataMapCommandException(INDEX_COLUMNS + " DMPROPERTY is blank");
    } else {
      return columns.split(",", -1);
    }
  }

}
