/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.processing.surrogatekeysgenerator.lru;

import java.util.HashMap;
import java.util.Map;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.carbondata.processing.schema.metadata.ArrayWrapper;

public class CarbonSeqGenCacheHolder {
    /**
     * Measure max surrogate key map
     */
    protected Map<String, Integer> measureMaxSurroagetMap;
    /**
     * Cache should be map only. because, multiple levels can map to same
     * database column. This case duplicate storage should be avoided.
     */
    private Map<String, Map<String, Integer>> memberCache;
    /**
     * Year Cache
     */
    private Map<String, Map<String, Integer>> timeDimCache;
    /**
     * max
     */
    private int[] max;
    /**
     * timeDimMax
     */
    private int[] timDimMax;
    /**
     * lastAccessTime
     */
    private long lastAccessTime;
    /**
     * hierCache
     */
    private Map<String, Int2ObjectMap<int[]>> hierCache =
            new HashMap<String, Int2ObjectMap<int[]>>(16);
    /**
     * hierCacheReverse
     */
    private Map<String, Map<ArrayWrapper, Integer>> hierCacheReverse =
            new HashMap<String, Map<ArrayWrapper, Integer>>(16);

    /**
     * @return the memberCache
     */
    public Map<String, Map<String, Integer>> getMemberCache() {
        return memberCache;
    }

    /**
     * @param memberCache the memberCache to set
     */
    public void setMemberCache(Map<String, Map<String, Integer>> memberCache) {
        this.memberCache = memberCache;
    }

    /**
     * @return the timeDimCache
     */
    public Map<String, Map<String, Integer>> getTimeDimCache() {
        return timeDimCache;
    }

    /**
     * @param timeDimCache the timeDimCache to set
     */
    public void setTimeDimCache(Map<String, Map<String, Integer>> timeDimCache) {
        this.timeDimCache = timeDimCache;
    }

    /**
     * @return the max
     */
    public int[] getMax() {
        return max;
    }

    /**
     * @param max the max to set
     */
    public void setMax(int[] max) {
        this.max = max;
    }

    /**
     * @return the timDimMax
     */
    public int[] getTimDimMax() {
        return timDimMax;
    }

    /**
     * @param timDimMax the timDimMax to set
     */
    public void setTimDimMax(int[] timDimMax) {
        this.timDimMax = timDimMax;
    }

    /**
     * @return the hierCache
     */
    public Map<String, Int2ObjectMap<int[]>> getHierCache() {
        return hierCache;
    }

    /**
     * @param hierCache the hierCache to set
     */
    public void setHierCache(Map<String, Int2ObjectMap<int[]>> hierCache) {
        this.hierCache = hierCache;
    }

    /**
     * @return the hierCacheReverse
     */
    public Map<String, Map<ArrayWrapper, Integer>> getHierCacheReverse() {
        return hierCacheReverse;
    }

    /**
     * @param hierCacheReverse the hierCacheReverse to set
     */
    public void setHierCacheReverse(Map<String, Map<ArrayWrapper, Integer>> hierCacheReverse) {
        this.hierCacheReverse = hierCacheReverse;
    }

    /**
     * @return the lastAccessTime
     */
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    /**
     * @param lastAccessTime the lastAccessTime to set
     */
    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    /**
     * @return the measureMaxSurroagetMap
     */
    public Map<String, Integer> getMeasureMaxSurroagetMap() {
        return measureMaxSurroagetMap;
    }

    /**
     * @param measureMaxSurroagetMap the measureMaxSurroagetMap to set
     */
    public void setMeasureMaxSurroagetMap(Map<String, Integer> measureMaxSurroagetMap) {
        this.measureMaxSurroagetMap = measureMaxSurroagetMap;
    }
}
