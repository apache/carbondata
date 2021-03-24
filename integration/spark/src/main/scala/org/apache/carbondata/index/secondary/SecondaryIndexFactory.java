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

package org.apache.carbondata.index.secondary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.sql.MalformedIndexCommandException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.features.TableOperation;
import org.apache.carbondata.core.index.IndexFilter;
import org.apache.carbondata.core.index.IndexInputSplit;
import org.apache.carbondata.core.index.IndexMeta;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.index.dev.IndexBuilder;
import org.apache.carbondata.core.index.dev.IndexWriter;
import org.apache.carbondata.core.index.dev.cgindex.CoarseGrainIndex;
import org.apache.carbondata.core.index.dev.cgindex.CoarseGrainIndexFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.events.Event;
import org.apache.carbondata.index.secondary.SecondaryIndexModel.PositionReferenceInfo;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * Index Factory for Secondary Index.
 */
@InterfaceAudience.Internal
public class SecondaryIndexFactory extends CoarseGrainIndexFactory {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(SecondaryIndexFactory.class.getName());
  private IndexMeta indexMeta;

  public SecondaryIndexFactory(CarbonTable carbonTable, IndexSchema indexSchema)
      throws MalformedIndexCommandException {
    super(carbonTable, indexSchema);
    List<ExpressionType> operations = new ArrayList<>(Arrays.asList(ExpressionType.values()));
    indexMeta = new IndexMeta(indexSchema.getIndexName(),
        carbonTable.getIndexedColumns(indexSchema.getIndexColumns()), operations);
    LOGGER.info("Created Secondary Index Factory instance for " + indexSchema.getIndexName());
  }

  @Override
  public IndexWriter createWriter(Segment segment, String shardName,
      SegmentProperties segmentProperties) throws IOException {
    throw new UnsupportedOperationException("Not supported for Secondary Index");
  }

  @Override
  public IndexBuilder createBuilder(Segment segment, String shardName,
      SegmentProperties segmentProperties) throws IOException {
    throw new UnsupportedOperationException("Not supported for Secondary Index");
  }

  @Override
  public IndexMeta getMeta() {
    return indexMeta;
  }

  private Map<Segment, List<CoarseGrainIndex>> getIndexes(List<Segment> segments,
      PositionReferenceInfo positionReferenceInfo) throws IOException {
    Map<Segment, List<CoarseGrainIndex>> indexes = new HashMap<>();
    List<String> allSegmentIds =
        segments.stream().map(Segment::getSegmentNo).collect(Collectors.toList());
    for (Segment segment : segments) {
      indexes.put(segment, this.getIndexes(segment, allSegmentIds, positionReferenceInfo));
    }
    return indexes;
  }

  private List<CoarseGrainIndex> getIndexes(Segment segment, List<String> allSegmentIds,
      PositionReferenceInfo positionReferenceInfo) throws IOException {
    List<CoarseGrainIndex> indexes = new ArrayList<>();
    SecondaryIndex secondaryIndex = new SecondaryIndex();
    secondaryIndex.init(
        new SecondaryIndexModel(getIndexSchema().getIndexName(), segment.getSegmentNo(),
            allSegmentIds, positionReferenceInfo, segment.getConfiguration()));
    secondaryIndex.setDefaultIndexPrunedBlocklet(segment.getDefaultIndexPrunedBlocklets());
    secondaryIndex.validateSegmentList(getCarbonTable().getTablePath()
        .replace(getCarbonTable().getTableName(), getIndexSchema().getIndexName()));
    indexes.add(secondaryIndex);
    return indexes;
  }

  @Override
  public Map<Segment, List<CoarseGrainIndex>> getIndexes(List<Segment> segments, IndexFilter filter)
      throws IOException {
    return getIndexes(segments, new PositionReferenceInfo());
  }

  @Override
  public Map<Segment, List<CoarseGrainIndex>> getIndexes(List<Segment> segments,
      Set<Path> partitionLocations, IndexFilter filter) throws IOException {
    return getIndexes(segments, new PositionReferenceInfo());
  }

  @Override
  public List<CoarseGrainIndex> getIndexes(Segment segment) throws IOException {
    List<String> allSegmentIds = new ArrayList<>();
    allSegmentIds.add(segment.getSegmentNo());
    return getIndexes(segment, allSegmentIds, new PositionReferenceInfo());
  }

  @Override
  public List<CoarseGrainIndex> getIndexes(Segment segment, Set<Path> partitionLocations)
      throws IOException {
    return getIndexes(segment);
  }

  @Override
  public List<CoarseGrainIndex> getIndexes(IndexInputSplit distributable) throws IOException {
    throw new UnsupportedOperationException("Not supported for Secondary Index");
  }

  @Override
  public List<IndexInputSplit> toDistributable(Segment segment) {
    throw new UnsupportedOperationException("Not supported for Secondary Index");
  }

  @Override
  public void fireEvent(Event event) {
  }

  @Override
  public void clear(String segment) {
  }

  @Override
  public synchronized void clear() {
  }

  @Override
  public void deleteIndexData(Segment segment) throws IOException {
  }

  @Override
  public void deleteIndexData() {
  }

  @Override
  public boolean willBecomeStale(TableOperation operation) {
    return false;
  }

  @Override
  public String getCacheSize() {
    return "0:0";
  }
}
