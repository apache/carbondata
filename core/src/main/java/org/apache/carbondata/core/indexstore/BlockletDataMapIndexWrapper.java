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

package org.apache.carbondata.core.indexstore;

import java.io.Serializable;
import java.util.List;

import org.apache.carbondata.core.cache.Cacheable;
import org.apache.carbondata.core.indexstore.blockletindex.BlockDataMap;

/**
 * A cacheable wrapper of datamaps
 */
public class BlockletDataMapIndexWrapper implements Cacheable, Serializable {

  private List<BlockDataMap> dataMaps;

  // size of the wrapper. basically the total size of the datamaps this wrapper is holding
  private long wrapperSize;

  public BlockletDataMapIndexWrapper(List<BlockDataMap> dataMaps) {
    this.dataMaps = dataMaps;
    this.wrapperSize = 0L;
    // add the size of each and every datamap in this wrapper
    for (BlockDataMap dataMap : dataMaps) {
      this.wrapperSize += dataMap.getMemorySize();
    }
  }

  @Override public long getFileTimeStamp() {
    return 0;
  }

  @Override public int getAccessCount() {
    return 0;
  }

  @Override public long getMemorySize() {
    return wrapperSize;
  }

  public List<BlockDataMap> getDataMaps() {
    return dataMaps;
  }
}