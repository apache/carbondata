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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.DataMapBuilder;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMap;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMapFactory;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.features.TableOperation;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.events.Event;

/**
 * Min Max DataMap Factory
 */
public class MinMaxIndexDataMapFactory extends CoarseGrainDataMapFactory {
  private static final Logger LOGGER = LogServiceFactory.getLogService(
      MinMaxIndexDataMapFactory.class.getName());
  private DataMapMeta dataMapMeta;
  private String dataMapName;
  private AbsoluteTableIdentifier identifier;

  public MinMaxIndexDataMapFactory(CarbonTable carbonTable, DataMapSchema dataMapSchema) {
    super(carbonTable, dataMapSchema);

    // this is an example for datamap, we can choose the columns and operations that
    // will be supported by this datamap. Furthermore, we can add cache-support for this datamap.

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
    this.dataMapMeta = new DataMapMeta(allColumns, optOperations);
  }

  /**
   * createWriter will return the MinMaxDataWriter.
   *
   * @param segment
   * @param shardName
   * @return
   */
  @Override
  public DataMapWriter createWriter(Segment segment, String shardName,
      SegmentProperties segmentProperties) {
    return new MinMaxDataWriter(getCarbonTable(), getDataMapSchema(), segment, shardName,
        dataMapMeta.getIndexedColumns());
  }

  @Override
  public DataMapBuilder createBuilder(Segment segment, String shardName,
      SegmentProperties segmentProperties) throws IOException {
    return null;
  }

  /**
   * getDataMaps Factory method Initializes the Min Max Data Map and returns.
   *
   * @param segment
   * @return
   * @throws IOException
   */
  @Override
  public List<CoarseGrainDataMap> getDataMaps(Segment segment)
      throws IOException {
    List<CoarseGrainDataMap> dataMapList = new ArrayList<>();
    // Form a dataMap of Type MinMaxIndexDataMap.
    MinMaxIndexDataMap dataMap = new MinMaxIndexDataMap();
    try {
      dataMap.init(new DataMapModel(
          MinMaxDataWriter.genDataMapStorePath(
              CarbonTablePath.getSegmentPath(
                  identifier.getTablePath(), segment.getSegmentNo()),
              dataMapName), new Configuration(false)));
    } catch (MemoryException ex) {
      throw new IOException(ex);
    }
    dataMapList.add(dataMap);
    return dataMapList;
  }

  /**
   * @param segment
   * @return
   */
  @Override
  public List<DataMapDistributable> toDistributable(Segment segment) {
    return null;
  }

  /**
   * Clear the DataMap.
   *
   * @param segment
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
  public List<CoarseGrainDataMap> getDataMaps(DataMapDistributable distributable)
      throws IOException {
    return getDataMaps(distributable.getSegment());
  }

  @Override
  public void fireEvent(Event event) {

  }

  @Override
  public DataMapMeta getMeta() {
    return this.dataMapMeta;
  }

  @Override
  public void deleteDatamapData(Segment segment) throws IOException {

  }

  @Override
  public void deleteDatamapData() {

  }

  @Override
  public boolean willBecomeStale(TableOperation operation) {
    return false;
  }
}