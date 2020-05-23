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

package org.apache.carbondata.index.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.features.TableOperation;
import org.apache.carbondata.core.index.IndexDistributable;
import org.apache.carbondata.core.index.IndexMeta;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.index.dev.IndexBuilder;
import org.apache.carbondata.core.index.dev.IndexModel;
import org.apache.carbondata.core.index.dev.IndexWriter;
import org.apache.carbondata.core.index.dev.cgindex.CoarseGrainIndex;
import org.apache.carbondata.core.index.dev.cgindex.CoarseGrainIndexFactory;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.events.Event;

/**
 * Min Max Index Factory
 */
public class MinMaxIndexIndexFactory extends CoarseGrainIndexFactory {
  private static final Logger LOGGER = LogServiceFactory.getLogService(
      MinMaxIndexIndexFactory.class.getName());
  private IndexMeta indexMeta;
  private String indexName;
  private AbsoluteTableIdentifier identifier;

  public MinMaxIndexIndexFactory(CarbonTable carbonTable, IndexSchema indexSchema) {
    super(carbonTable, indexSchema);

    // this is an example for indexSchema, we can choose the columns and operations that
    // will be supported by this indexSchema. Furthermore, we can add cache-support for this indexSchema.

    // columns that will be indexed
    List<CarbonColumn> allColumns = getCarbonTable().getCreateOrderColumn();

    // operations that will be supported on the indexed columns
    List<ExpressionType> optOperations = new ArrayList<>();
    optOperations.add(ExpressionType.EQUALS);
    optOperations.add(ExpressionType.GREATERTHAN);
    optOperations.add(ExpressionType.GREATERTHAN_EQUALTO);
    optOperations.add(ExpressionType.LESSTHAN);
    optOperations.add(ExpressionType.LESSTHAN_EQUALTO);
    optOperations.add(ExpressionType.NOT_EQUALS);
    LOGGER.error("support operations: " + StringUtils.join(optOperations, ", "));
    this.indexMeta = new IndexMeta(allColumns, optOperations);
  }

  /**
   * createWriter will return the MinMaxDataWriter.
   *
   * @param segment
   * @param shardName
   * @return
   */
  @Override
  public IndexWriter createWriter(Segment segment, String shardName,
      SegmentProperties segmentProperties) {
    return new MinMaxDataWriter(getCarbonTable(), getIndexSchema(), segment, shardName,
        indexMeta.getIndexedColumns());
  }

  @Override
  public IndexBuilder createBuilder(Segment segment, String shardName,
      SegmentProperties segmentProperties) throws IOException {
    return null;
  }

  /**
   * getSecondaryIndexes Factory method Initializes the Min Max Index and returns.
   *
   * @param segment
   * @return
   * @throws IOException
   */
  @Override
  public List<MinMaxIndex> getIndexes(Segment segment)
      throws IOException {
    List<MinMaxIndex> indexes = new ArrayList<>();
    // Form a MinMaxIndex.
    MinMaxIndex index = new MinMaxIndex();
    try {
      index.init(new IndexModel(
          MinMaxDataWriter.genIndexStorePath(
              CarbonTablePath.getSegmentPath(
                  identifier.getTablePath(), segment.getSegmentNo()), indexName), new Configuration(false)));
    } catch (MemoryException ex) {
      throw new IOException(ex);
    }
    indexes.add(index);
    return indexes;
  }

  /**
   * @param segment
   * @return
   */
  @Override
  public List<IndexDistributable> toDistributable(Segment segment) {
    return null;
  }

  /**
   * Clear the Index.
   *
   * @param segment
   */
  @Override
  public void clear(String segmentNo) {
  }

  /**
   * Clearing the index.
   */
  @Override
  public void clear() {
  }

  @Override
  public List<CoarseGrainIndex> getIndexes(IndexDistributable distributable)
      throws IOException {
    return getIndexes(distributable.getSegment());
  }

  @Override
  public void fireEvent(Event event) {

  }

  @Override
  public IndexMeta getMeta() {
    return this.indexMeta;
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
}