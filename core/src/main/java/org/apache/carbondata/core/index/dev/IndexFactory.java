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

package org.apache.carbondata.core.index.dev;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.features.TableOperation;
import org.apache.carbondata.core.index.IndexFilter;
import org.apache.carbondata.core.index.IndexInputSplit;
import org.apache.carbondata.core.index.IndexLevel;
import org.apache.carbondata.core.index.IndexMeta;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.index.dev.cgindex.CoarseGrainIndex;
import org.apache.carbondata.core.index.dev.expr.IndexInputSplitWrapper;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;
import org.apache.carbondata.events.Event;

import org.apache.hadoop.fs.Path;

/**
 * Factory class for creating the index.
 */
public abstract class IndexFactory<T extends Index> {
  private CarbonTable carbonTable;
  private IndexSchema indexSchema;

  public IndexFactory(CarbonTable carbonTable, IndexSchema indexSchema) {
    this.carbonTable = carbonTable;
    this.indexSchema = indexSchema;
  }

  public CarbonTable getCarbonTable() {
    return carbonTable;
  }

  public void setCarbonTable(CarbonTable carbonTable) {
    this.carbonTable = carbonTable;
  }

  public IndexSchema getIndexSchema() {
    return indexSchema;
  }

  public IndexInputSplitWrapper toDistributableSegment(Segment segment,
      IndexSchema schema, AbsoluteTableIdentifier identifier, String uniqueId) {
    return null;
  }

  /**
   * Create a new write for this index, to write new data into the specified segment and shard
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
    Map<Segment, List<CoarseGrainIndex>> indexes = new HashMap<>();
    for (Segment segment : segments) {
      indexes.put(segment, (List<CoarseGrainIndex>) this.getIndexes(segment));
    }
    return indexes;
  }

  /**
   * Get the index for all segments with matched partitions. Load index to cache, only if it
   * matches the partition.
   */
  public Map<Segment, List<CoarseGrainIndex>> getIndexes(List<Segment> segments,
      Set<Path> partitionLocations, IndexFilter indexFilter) throws IOException {
    Map<Segment, List<CoarseGrainIndex>> indexes = new HashMap<>();
    for (Segment segment : segments) {
      indexes.put(segment, (List<CoarseGrainIndex>) this.getIndexes(segment, partitionLocations));
    }
    return indexes;
  }

  /**
   * Get the index for segmentId
   */
  public abstract List<T> getIndexes(Segment segment) throws IOException;

  /**
   * Get the index for segmentId with matched partitions
   */
  public abstract List<T> getIndexes(Segment segment, Set<Path> partitionLocations)
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
  public abstract IndexLevel getIndexLevel();

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
   * whether to block operation on corresponding table or column.
   * For example, bloom filter index will block changing datatype for bloom index column.
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
   * whether this index support rebuild
   */
  public boolean supportRebuild() {
    return false;
  }

  public String getCacheSize() {
    return null;
  }

}
