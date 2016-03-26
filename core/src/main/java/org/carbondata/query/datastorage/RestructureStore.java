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

package org.carbondata.query.datastorage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.metadata.SliceMetaData;

public class RestructureStore implements Comparable<RestructureStore> {
    /**
     *
     */
    private Map<String, List<InMemoryCube>> slices =
            new ConcurrentHashMap<String, List<InMemoryCube>>(
                    MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

    /**
     * Map of table name and SliceMetaData (Fact table, all aggregate tables)
     */
    private Map<String, SliceMetaData> sliceMetaCacheMap =
            new ConcurrentHashMap<String, SliceMetaData>(
                    MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

    private Map<String, String> sliceMetaPathMap =
            new ConcurrentHashMap<String, String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

    /**
     *
     */
    private String folderName;

    private int restructureId;

    public RestructureStore(final String name, final int restructureId) {
        folderName = name;
        this.restructureId = restructureId;
    }

    /**
     * @param activeSlices
     */
    public void getActiveSlices(List<InMemoryCube> activeSlices) {
        for (Entry<String, List<InMemoryCube>> entry : slices.entrySet()) {
            for (InMemoryCube slice : entry.getValue()) {
                if (slice.isActive()) {
                    activeSlices.add(slice);
                }
            }
        }
    }

    /**
     * @param ids
     * @param slicesByIds
     */
    public void getSlicesByIds(List<Long> ids, List<InMemoryCube> slicesByIds) {
        for (Entry<String, List<InMemoryCube>> entry : slices.entrySet()) {
            for (InMemoryCube slice : entry.getValue()) {
                if (ids.indexOf(slice.getID()) != -1) {
                    if (slice.isActive()) {
                        slicesByIds.add(slice);
                    }
                }
            }
        }
    }

    /**
     * @param sliceIds
     */
    public void getActiveSliceIds(List<Long> sliceIds) {
        for (Entry<String, List<InMemoryCube>> entry : slices.entrySet()) {
            for (InMemoryCube slice : entry.getValue()) {
                if (slice.isActive()) {
                    sliceIds.add(slice.getID());
                }
            }
        }
    }

    /**
     * @return
     */
    public boolean isSlicesAvailable() {
        return slices.size() > 0;
    }

    /**
     * @return the slices
     */
    public List<InMemoryCube> getSlices(String tableName) {
        return slices.get(tableName);
    }

    /**
     * @param slices the slices to set
     */
    public synchronized void setSlice(InMemoryCube slice, String tableName) {
        List<InMemoryCube> sliceList = slices.get(tableName);
        if (sliceList == null) {
            sliceList = new ArrayList<InMemoryCube>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            this.slices.put(tableName, sliceList);
        }

        for (Iterator<InMemoryCube> iterator = sliceList.iterator(); iterator.hasNext(); ) {
            InMemoryCube loadedSlice = iterator.next();
            if (loadedSlice.getCubeSlicePathInfo().getLoadPath()
                    .equals(slice.getCubeSlicePathInfo().getLoadPath())) {
                iterator.remove();
                break;
            }
        }

        sliceList.add(slice);
    }

    /**
     * @param slices the slices to remove
     */
    public void removeSlice(InMemoryCube slice, String tableName) {
        List<InMemoryCube> sliceList = slices.get(tableName);
        if (sliceList != null) {
            sliceList.remove(slice);
        }
    }

    /**
     * @return the sliceMetaCacheMap
     */
    public Map<String, SliceMetaData> getSliceMetaCacheMap() {
        return sliceMetaCacheMap;
    }

    public void setSliceMetaCacheMap(Map<String, SliceMetaData> sliceMetaCacheMap) {
        this.sliceMetaCacheMap = sliceMetaCacheMap;
    }

    /**
     * @return the sliceMetaCacheMap
     */
    public SliceMetaData getSliceMetaCache(String tableName) {
        return sliceMetaCacheMap.get(tableName);
    }

    /**
     * @param sliceMetaCacheMap the sliceMetaCacheMap to set
     */
    public void setSliceMetaCache(SliceMetaData sliceMetaData, String tableName) {
        this.sliceMetaCacheMap.put(tableName, sliceMetaData);
    }

    /**
     * @return the folderName
     */
    public String getFolderName() {
        return folderName;
    }

    /**
     * @param folderName the folderName to set
     */
    //    public void setFolderName(String folderName)
    //    {
    //        this.folderName = folderName;
    //    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((folderName == null) ? 0 : folderName.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override public boolean equals(Object obj) {
        if (obj instanceof RestructureStore) {
            if (this == obj) {
                return true;
            }
            RestructureStore other = (RestructureStore) obj;
            if (folderName == null) {
                if (other.folderName != null) {
                    return false;
                }
            } else if (!folderName.equals(other.folderName)) {
                return false;
            }
            return true;
        } else {
            return false;
        }

    }

    public void setSlices(Map<String, List<InMemoryCube>> slices) {
        this.slices = slices;
    }

    public int getRestructureId() {
        return restructureId;
    }

    public void setRestructureId(int restructureId) {
        this.restructureId = restructureId;
    }

    /**
     * @return Returns the sliceMetaPathMap.
     */
    public Map<String, String> getSliceMetaPathMap() {
        return sliceMetaPathMap;
    }

    /**
     * @param sliceMetaPathMap The sliceMetaPathMap to set.
     */
    public void setSliceMetaPathMap(Map<String, String> sliceMetaPathMap) {
        this.sliceMetaPathMap = sliceMetaPathMap;
    }

    public String getSliceMetadataPath(String tableName) {
        return this.sliceMetaPathMap.get(tableName);
    }

    public void setSliceMetaPathCache(String sliceMetaDataPath, String tableName) {
        this.sliceMetaPathMap.put(tableName, sliceMetaDataPath);
    }

    @Override public int compareTo(RestructureStore arg0) {
        // TODO Auto-generated method stub
        return restructureId - arg0.restructureId;
    }
}
