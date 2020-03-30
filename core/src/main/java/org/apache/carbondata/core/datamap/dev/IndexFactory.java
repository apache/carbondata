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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.exceptions.sql.MalformedIndexCommandException;
import org.apache.carbondata.core.datamap.*;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainIndex;
import org.apache.carbondata.core.datamap.dev.expr.IndexInputSplitWrapper;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.features.TableOperation;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.events.Event;

/**
 * Factory class for creating the index.
 */
public abstract class IndexFactory<T extends Index> {

  private CarbonTable carbonTable;
  private DataMapSchema dataMapSchema;

  public IndexFactory(CarbonTable carbonTable, DataMapSchema dataMapSchema) {
    this.carbonTable = carbonTable;
    this.dataMapSchema = dataMapSchema;
  }

  public CarbonTable getCarbonTable() {
    return carbonTable;
  }

  public void setCarbonTable(CarbonTable carbonTable) {
    this.carbonTable = carbonTable;
  }

  public DataMapSchema getDataMapSchema() {
    return dataMapSchema;
  }

  public IndexInputSplitWrapper toDistributableSegment(Segment segment,
      DataMapSchema schema, AbsoluteTableIdentifier identifier, String uniqueId) {
    return null;
  }

  /**
   * Create a new write for this datamap, to write new data into the specified segment and shard
   */
  public abstract IndexWriter createWriter(Segment segment, String shardName,
      SegmentProperties segmentProperties) throws IOException;

  /**
   * Create a new IndexBuilder for this index, to rebuild the specified
   * segment and shard data in the main table.
   * TODO: refactor to unify with IndexWriter
   */
  public abstract IndexBuilder createBuilder(Segment segment, String shardName,
      SegmentProperties segmentProperties) throws IOException;

  /**
   * Get the index for all segments
   */
  public Map<Segment, List<CoarseGrainIndex>> getIndexes(List<Segment> segments,
      IndexFilter filter) throws IOException {
    Map<Segment, List<CoarseGrainIndex>> dataMaps = new HashMap<>();
    for (Segment segment : segments) {
      dataMaps.put(segment, (List<CoarseGrainIndex>) this.getIndexes(segment));
    }
    return dataMaps;
  }

  /**
   * Get the index for all segments with matched partitions. Load index to cache, only if it
   * matches the partition.
   */
  public Map<Segment, List<CoarseGrainIndex>> getIndexes(List<Segment> segments,
      List<PartitionSpec> partitionSpecs, IndexFilter indexFilter) throws IOException {
    Map<Segment, List<CoarseGrainIndex>> dataMaps = new HashMap<>();
    for (Segment segment : segments) {
      dataMaps.put(segment, (List<CoarseGrainIndex>) this.getIndexes(segment, partitionSpecs));
    }
    return dataMaps;
  }

  /**
   * Get the index for segmentId
   */
  public abstract List<T> getIndexes(Segment segment) throws IOException;

  /**
   * Get the index for segmentId with matched partitions
   */
  public abstract List<T> getIndexes(Segment segment, List<PartitionSpec> partitions)
      throws IOException;

  /**
   * Get index for distributable object.
   */
  public abstract List<T> getIndexes(IndexInputSplit distributable) throws IOException;

  /**
   * Get all distributable objects of a segmentId
   * @return
   */
  public abstract List<IndexInputSplit> toDistributable(Segment segment);

  /**
   *
   * @param event
   */
  public abstract void fireEvent(Event event);

  /**
   * Clear all index for a segment from memory
   */
  public void clear(String segmentNo) {

  }

  /**
   * Clear all index from memory
   */
  public abstract void clear();

  /**
   * Return metadata of this index
   */
  public abstract IndexMeta getMeta();

  /**
   *  Type of index whether it is FG or CG
   */
  public abstract IndexLevel getDataMapLevel();

  /**
   * delete index data in the specified segment
   */
  public abstract void deleteIndexData(Segment segment) throws IOException;

  /**
   * delete index data if any
   */
  public abstract void deleteIndexData();

  /**
   * delete index data if any
   */
  public void deleteSegmentIndexData(String segmentNo) throws IOException {

  }

  /**
   * This function should return true is the input operation enum will make the index become stale
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
  public void validate() throws MalformedIndexCommandException {
    List<CarbonColumn> indexColumns =
        carbonTable.getIndexedColumns(dataMapSchema.getIndexColumns());
    Set<String> unique = new HashSet<>();
    for (CarbonColumn indexColumn : indexColumns) {
      unique.add(indexColumn.getColName());
    }
    if (unique.size() != indexColumns.size()) {
      throw new MalformedIndexCommandException("index column list has duplicate column");
    }
  }

  /**
   * whether to block operation on corresponding table or column.
   * For example, bloomfilter datamap will block changing datatype for bloomindex column.
   * By default it will not block any operation.
   *
   * @param operation table operation
   * @param targets objects which the operation impact on
   * @return true the operation will be blocked;false the operation will not be blocked
   */
  public boolean isOperationBlocked(TableOperation operation, Object... targets) {
    return false;
  }

  /**
   * whether this datamap support rebuild
   */
  public boolean supportRebuild() {
    return false;
  }

  public String getCacheSize() {
    return null;
  }
}
