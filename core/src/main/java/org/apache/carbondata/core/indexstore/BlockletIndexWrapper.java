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
import org.apache.carbondata.core.index.dev.Index;
import org.apache.carbondata.core.indexstore.blockletindex.BlockIndex;

/**
 * A cacheable wrapper of datamaps
 */
public class BlockletIndexWrapper implements Cacheable, Serializable {

  private static final long serialVersionUID = -2859075086955465810L;

  private List<BlockIndex> dataMaps;

  private String segmentId;

  // size of the wrapper. basically the total size of the datamaps this wrapper is holding
  private long wrapperSize;

  public BlockletIndexWrapper(String segmentId, List<BlockIndex> dataMaps) {
    this.dataMaps = dataMaps;
    this.wrapperSize = 0L;
    this.segmentId = segmentId;
    // add the size of each and every datamap in this wrapper
    for (BlockIndex dataMap : dataMaps) {
      this.wrapperSize += dataMap.getMemorySize();
    }
  }

  @Override
  public int getAccessCount() {
    return 0;
  }

  @Override
  public long getMemorySize() {
    return wrapperSize;
  }

  @Override
  public void invalidate() {
    for (Index index : dataMaps) {
      index.clear();
    }
  }

  public List<BlockIndex> getDataMaps() {
    return dataMaps;
  }

  public String getSegmentId() {
    return segmentId;
  }
}