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

package org.apache.carbondata.datamap.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.IndexBuilder;
import org.apache.carbondata.core.datamap.dev.IndexModel;
import org.apache.carbondata.core.datamap.dev.IndexWriter;
import org.apache.carbondata.core.datamap.dev.cgindex.CoarseGrainIndex;
import org.apache.carbondata.core.datamap.dev.cgindex.CoarseGrainIndexFactory;
import org.apache.carbondata.core.index.IndexDistributable;
import org.apache.carbondata.core.index.IndexMeta;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.features.TableOperation;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.events.Event;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * Min Max Index Factory
 */
public class MinMaxIndexFactory extends CoarseGrainIndexFactory {
  private static final Logger LOGGER = LogServiceFactory.getLogService(
      MinMaxIndexFactory.class.getName());
  private IndexMeta dataMapMeta;
  private String dataMapName;
  private AbsoluteTableIdentifier identifier;

  public MinMaxIndexFactory(CarbonTable carbonTable, DataMapSchema dataMapSchema) {
    super(carbonTable, dataMapSchema);

    // this is an example for index, we can choose the columns and operations that
    // will be supported by this index. Furthermore, we can add cache-support for this index.

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
    LOGGER.error("MinMaxDataMap support operations: " + StringUtils.join(optOperations, ", "));
    this.dataMapMeta = new IndexMeta(allColumns, optOperations);
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
    return new MinMaxDataWriter(getCarbonTable(), getDataMapSchema(), segment, shardName,
        dataMapMeta.getIndexedColumns());
  }

  @Override
  public IndexBuilder createBuilder(Segment segment, String shardName,
      SegmentProperties segmentProperties) throws IOException {
    return null;
  }

  /**
   * getIndexes Factory method Initializes the Min Max Data Map and returns.
   *
   * @param segment
   * @return
   * @throws IOException
   */
  @Override
  public List<CoarseGrainIndex> getIndexes(Segment segment)
      throws IOException {
    List<CoarseGrainIndex> dataMapList = new ArrayList<>();
    // Form a index of Type MinMaxIndex.
    MinMaxIndex index = new MinMaxIndex();
    index.init(new IndexModel(
        MinMaxDataWriter.genDataMapStorePath(
            CarbonTablePath.getSegmentPath(
                identifier.getTablePath(), segment.getSegmentNo()),
            dataMapName), new Configuration(false)));
    dataMapList.add(index);
    return dataMapList;
  }

  @Override
  public List<CoarseGrainIndex> getIndexes(Segment segment, List<PartitionSpec> partitions)
      throws IOException {
    throw new UnsupportedOperationException();
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
   * Clear the Index of specified segment
   *
   */
  @Override
  public void clear(String segmentNo) {
  }

  /**
   * Clearing the data map.
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
    return this.dataMapMeta;
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