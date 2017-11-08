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

import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;

import org.roaringbitmap.RoaringBitmap;

/**
 * This class maintains delete delta data cache of each blocklet along with the block timestamp
 */
public class BlockletLevelDeleteDeltaDataCache {
  private Map<Integer, RoaringBitmap> deleteDelataDataCache =
      new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  private String timeStamp;

  public BlockletLevelDeleteDeltaDataCache(Map<Integer, Integer[]> deleteDeltaFileData,
      String timeStamp) {
    for (Map.Entry<Integer, Integer[]> entry : deleteDeltaFileData.entrySet()) {
      int[] dest = new int[entry.getValue().length];
      int i = 0;
      for (Integer val : entry.getValue()) {
        dest[i++] = val.intValue();
      }
      deleteDelataDataCache.put(entry.getKey(), RoaringBitmap.bitmapOf(dest));
    }
    this.timeStamp = timeStamp;
  }

  public String getCacheTimeStamp() {
    return timeStamp;
  }
}

