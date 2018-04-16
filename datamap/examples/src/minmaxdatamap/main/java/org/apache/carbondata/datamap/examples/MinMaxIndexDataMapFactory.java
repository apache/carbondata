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
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMap;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMapFactory;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.events.Event;

/**
 * Min Max DataMap Factory
 */
public class MinMaxIndexDataMapFactory extends CoarseGrainDataMapFactory {

  private AbsoluteTableIdentifier identifier;

  @Override public void init(CarbonTable carbonTable, DataMapSchema dataMapSchema) {
    this.identifier = carbonTable.getAbsoluteTableIdentifier();
  }

  /**
   * createWriter will return the MinMaxDataWriter.
   *
   * @param segment
   * @return
   */
  @Override public DataMapWriter createWriter(Segment segment, String writeDirectoryPath) {
    return new MinMaxDataWriter(identifier, segment, writeDirectoryPath);
  }

  /**
   * getDataMaps Factory method Initializes the Min Max Data Map and returns.
   *
   * @param segment
   * @param readCommittedScope
   * @return
   * @throws IOException
   */
  @Override
  public List<CoarseGrainDataMap> getDataMaps(Segment segment,
      ReadCommittedScope readCommittedScope)
      throws IOException {
    List<CoarseGrainDataMap> dataMapList = new ArrayList<>();
    // Form a dataMap of Type MinMaxIndexDataMap.
    MinMaxIndexDataMap dataMap = new MinMaxIndexDataMap();
    try {
      dataMap.init(new DataMapModel(
          CarbonTablePath.getSegmentPath(identifier.getTablePath(), segment.getSegmentNo())));
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
  @Override public List<DataMapDistributable> toDistributable(Segment segment) {
    return null;
  }

  /**
   * Clear the DataMap.
   *
   * @param segment
   */
  @Override public void clear(Segment segment) {
  }

  /**
   * Clearing the data map.
   */
  @Override public void clear() {
  }

  @Override public List<CoarseGrainDataMap> getDataMaps(DataMapDistributable distributable,
      ReadCommittedScope readCommittedScope)
      throws IOException {
    return null;
  }

  @Override public void fireEvent(Event event) {

  }

  @Override public DataMapMeta getMeta() {
    return new DataMapMeta(new ArrayList<String>(Arrays.asList("c2")),
        new ArrayList<ExpressionType>());
  }
}