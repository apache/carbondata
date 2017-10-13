package org.apache.carbondata.examples;/*
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

import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.events.ChangeEvent;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;
import org.apache.carbondata.core.indexstore.schema.FilterType;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Min Max DataMap Factory
 */
public class LuceneDataMapFactory implements DataMapFactory {

  private AbsoluteTableIdentifier identifier;

  @Override public void init(AbsoluteTableIdentifier identifier, String dataMapName) {
    this.identifier = identifier;
  }

  /**
   * createWriter will return the MinMaxDataWriter.
   *
   * @param segmentId
   * @return
   */
  @Override public DataMapWriter createWriter(String segmentId) {
    return new LuceneDataMapWriter();
  }

  /**
   * getDataMaps Factory method Initializes the Min Max Data Map and returns.
   *
   * @param segmentId
   * @return
   * @throws IOException
   */
  @Override public List<DataMap> getDataMaps(String segmentId) throws IOException {

    List<DataMap> dataMapList = new ArrayList<>();
    // Form a dataMap of Type LuceneDataMap.
    LuceneDataMap dataMap = new LuceneDataMap();
    try {
      String path = identifier.getTablePath() + "/Fact/Part0/Segment_" + segmentId + File.separator;
      CarbonFile[] carbonfile = getCarbonMinMaxIndexFiles(path);
      dataMap.init(carbonfile[0].getPath());
    } catch (MemoryException ex) {

    }
    dataMapList.add(dataMap);
    return dataMapList;
  }

  private CarbonFile[] getCarbonMinMaxIndexFiles(String filePath) {
    String path = filePath.substring(0, filePath.lastIndexOf("/") + 1);
    CarbonFile carbonFile = FileFactory.getCarbonFile(path);
    return carbonFile.listFiles(new CarbonFileFilter() {
     @Override public boolean accept(CarbonFile file) {
                return file.getName().endsWith(".luceneIndex");
              }
     });
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

  @Override public DataMap getDataMap(DataMapDistributable distributable) {
    return null;
  }

  @Override public void fireEvent(ChangeEvent event) {

  }

  @Override public DataMapMeta getMeta() {
    return new DataMapMeta(new ArrayList<String>(Arrays.asList("c2")), FilterType.EQUALTO);
  }
}