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

package org.apache.carbondata.examples.rtree;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.events.ChangeEvent;
import org.apache.carbondata.core.indexstore.schema.FilterType;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;

public class RTreeDataMapFactory implements DataMapFactory {
  private String x, y;
  private DataMapMeta meta;

  private AbsoluteTableIdentifier identifier;
  private String dataMapName;

  @Override
  public void init(AbsoluteTableIdentifier identifier, String dataMapName) {
    this.identifier = identifier;
    this.dataMapName = dataMapName;
    this.meta = new DataMapMeta(Arrays.asList("c1", "c2"), FilterType.EQUALTO);
  }

  @Override
  public DataMapWriter createWriter(String segmentId) {
    return new RTreeDataMapWriter(
        identifier.getTablePath() + "/Fact/Part0/Segment_" + segmentId + File.separator);
  }

  @Override
  public List<DataMap> getDataMaps(String segmentId) throws IOException {
    List<DataMap> dataMapList = new ArrayList<>();
    // Form a dataMap of Type MinMaxDataMap.
    RTreeDataMap dataMap = new RTreeDataMap();
    try {
      dataMap.init(
          identifier.getTablePath() + "/Fact/Part0/Segment_" + segmentId + File.separator);
    } catch (MemoryException ex) {
      System.err.println(ex.getMessage());
    }
    dataMapList.add(dataMap);
    return dataMapList;
  }

  @Override
  public DataMap getDataMap(DataMapDistributable distributable) {
    return null;
  }

  @Override
  public List<DataMapDistributable> toDistributable(String segmentId) {
    return null;
  }

  @Override
  public void fireEvent(ChangeEvent event) {
  }

  @Override
  public void clear(String segmentId) {
  }

  @Override
  public void clear() {
  }

  @Override
  public DataMapMeta getMeta() {
    return meta;
  }
}
