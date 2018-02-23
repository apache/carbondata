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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.dev.AbstractDataMapWriter;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.cgdatamap.AbstractCoarseGrainIndexDataMap;
import org.apache.carbondata.core.datamap.dev.cgdatamap.AbstractCoarseGrainIndexDataMapFactory;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.events.Event;

/**
 * Min Max DataMap Factory
 */
public class MinMaxIndexDataMapFactory extends AbstractCoarseGrainIndexDataMapFactory {

  private AbsoluteTableIdentifier identifier;

  @Override public void init(AbsoluteTableIdentifier identifier, DataMapSchema dataMapSchema) {
    this.identifier = identifier;
  }

  /**
   * createWriter will return the MinMaxDataWriter.
   *
   * @param segmentId
   * @return
   */
  @Override public AbstractDataMapWriter createWriter(String segmentId, String dataWritePath) {
    return new MinMaxDataWriter(identifier, segmentId, dataWritePath);
  }

  /**
   * getDataMaps Factory method Initializes the Min Max Data Map and returns.
   *
   * @param segmentId
   * @return
   * @throws IOException
   */
  @Override public List<AbstractCoarseGrainIndexDataMap> getDataMaps(String segmentId)
      throws IOException {
    List<AbstractCoarseGrainIndexDataMap> dataMapList = new ArrayList<>();
    // Form a dataMap of Type MinMaxIndexDataMap.
    MinMaxIndexDataMap dataMap = new MinMaxIndexDataMap();
    try {
      dataMap.init(new DataMapModel(
          identifier.getTablePath() + "/Fact/Part0/Segment_" + segmentId + File.separator));
    } catch (MemoryException ex) {

    }
    dataMapList.add(dataMap);
    return dataMapList;
  }

  /**
   * @param segmentId
   * @return
   */
  @Override public List<DataMapDistributable> toDistributable(String segmentId) {
    return null;
  }

  /**
   * Clear the DataMap.
   *
   * @param segmentId
   */
  @Override public void clear(String segmentId) {
  }

  /**
   * Clearing the data map.
   */
  @Override public void clear() {
  }

  @Override public List<AbstractCoarseGrainIndexDataMap> getDataMaps(DataMapDistributable distributable)
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