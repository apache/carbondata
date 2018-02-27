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

package org.apache.carbondata.datamap.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapLevel;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.fgdatamap.FineGrainDataMap;
import org.apache.carbondata.core.memory.MemoryException;

/**
 * CG level of lucene DataMap
 */
@InterfaceAudience.Internal
public class LuceneFineGrainDataMapFactory extends LuceneDataMapFactoryBase<FineGrainDataMap> {

  /**
   * Get the datamap for segmentid
   */
  public List<FineGrainDataMap> getDataMaps(String segmentId) throws IOException {
    List<FineGrainDataMap> lstDataMap = new ArrayList<>();
    FineGrainDataMap dataMap = new LuceneFineGrainDataMap(analyzer);
    try {
      dataMap.init(new DataMapModel(
          LuceneDataMapWriter.genDataMapStorePath(
              tableIdentifier.getTablePath(), segmentId, dataMapName)));
    } catch (MemoryException e) {
      LOGGER.error("failed to get lucene datamap , detail is {}" + e.getMessage());
      return lstDataMap;
    }
    lstDataMap.add(dataMap);
    return lstDataMap;
  }

  /**
   * Get datamaps for distributable object.
   */
  public List<FineGrainDataMap> getDataMaps(DataMapDistributable distributable)
      throws IOException {
    return getDataMaps(distributable.getSegmentId());
  }

  @Override
  public DataMapLevel getDataMapType() {
    return DataMapLevel.FG;
  }

}
