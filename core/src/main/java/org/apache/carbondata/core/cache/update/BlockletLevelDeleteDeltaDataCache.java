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

package org.apache.carbondata.core.cache.update;

import org.roaringbitmap.RoaringBitmap;

/**
 * This class maintains delete delta data cache of each blocklet along with the block timestamp
 */
public class BlockletLevelDeleteDeltaDataCache {
  private RoaringBitmap deleteDelataDataCache;
  private String timeStamp;

  public BlockletLevelDeleteDeltaDataCache(int[] deleteDeltaFileData, String timeStamp) {
    deleteDelataDataCache = RoaringBitmap.bitmapOf(deleteDeltaFileData);
    this.timeStamp=timeStamp;
  }

  public boolean contains(int key) {
    return deleteDelataDataCache.contains(key);
  }

  public int getSize() {
    return deleteDelataDataCache.getCardinality();
  }

  public String getCacheTimeStamp() {
    return timeStamp;
  }
}

