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

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.metadata.SliceMetaData;

public class RestructureStore implements Comparable<RestructureStore> {
    /**
     *
     */
    private Map<String, List<InMemoryTable>> slices =
            new ConcurrentHashMap<String, List<InMemoryTable>>(
                    CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    /**
     * Map of table name and SliceMetaData (Fact table, all aggregate tables)
     */
    private Map<String, SliceMetaData> sliceMetaCacheMap =
            new ConcurrentHashMap<String, SliceMetaData>(
                    CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    private Map<String, String> sliceMetaPathMap =
            new ConcurrentHashMap<String, String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    /**
     *
     */
    private String folderName;

    private int restructureId;
    /**
     * Read/Write lock instance to allow concurrent read/write of segment cache.
     */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    /**
     * Read lock to allow concurrent read of segment cache.
     */
    private final Lock readLock = readWriteLock.readLock();
    /**
     * Write lock instance to allow concurrent write of segment cache.
     */
    private final Lock writeLock = readWriteLock.writeLock();

    public RestructureStore(final String name, final int restructureId) {
        folderName = name;
        this.restructureId = restructureId;
    }

    /**
     * @param activeSlices
     */
    public void getActiveSlices(List<InMemoryTable> activeSlices) {
        for (Entry<String, List<InMemoryTable>> entry : slices.entrySet()) {
            for (InMemoryTable slice : entry.getValue()) {
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
    public void getSlicesByIds(List<Long> ids, List<InMemoryTable> slicesByIds) {
        for (Entry<String, List<InMemoryTable>> entry : slices.entrySet()) {
            for (InMemoryTable slice : entry.getValue()) {
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
        for (Entry<String, List<InMemoryTable>> entry : slices.entrySet()) {
            for (InMemoryTable slice : entry.getValue()) {
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
    public List<InMemoryTable> getSlices(String tableName) {
        return slices.get(tableName);
    }

    public synchronized void setSlice(InMemoryTable slice, String tableName) {
        List<InMemoryTable> sliceList = slices.get(tableName);
        if (sliceList == null) {
            sliceList = new ArrayList<InMemoryTable>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
            this.slices.put(tableName, sliceList);
        }

        for (Iterator<InMemoryTable> iterator = sliceList.iterator(); iterator.hasNext(); ) {
            InMemoryTable loadedSlice = iterator.next();
            if (loadedSlice.getTableSlicePathInfo().getLoadPath()
                    .equals(slice.getTableSlicePathInfo().getLoadPath())) {
                iterator.remove();
                break;
            }
        }

        sliceList.add(slice);
    }

    /**
     * @return the sliceMetaCacheMap
     */
    public SliceMetaData getSliceMetaCache(String tableName) {
        return sliceMetaCacheMap.get(tableName);
    }

    public void setSliceMetaCache(SliceMetaData sliceMetaData, String tableName) {
        this.sliceMetaCacheMap.put(tableName, sliceMetaData);
    }

    /**
     * @return the folderName
     */
    public String getFolderName() {
        return folderName;
    }

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

    public void setSlices(Map<String, List<InMemoryTable>> slices) {
        this.slices = slices;
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
        return restructureId - arg0.restructureId;
    }

    /**
     * Populates the list of the active segments in activeInMemTableList.
     */
    public void getActiveSlicesByTableName(List<InMemoryTable> activeInMemTableList,
            String factTableName) {
        List<InMemoryTable> inMemoryTables = slices.get(factTableName);
        if (null != inMemoryTables) {
            try {
                readLock.lock();
                for (InMemoryTable inMemoryTable : inMemoryTables) {
                    if (inMemoryTable.isActive()) {
                        activeInMemTableList.add(inMemoryTable);
                    }
                }
            } finally {
                readLock.unlock();
            }

        }
    }

    /**
     * Removes segments for the given table from the store
     */
    public void removeSlice(List<InMemoryTable> listOfSliceToBeRemoved, String tableName) {
        try {
            writeLock.lock();
            List<InMemoryTable> inMemoryTables = slices.get(tableName);
            for (InMemoryTable inMemoryTable : listOfSliceToBeRemoved) {
                inMemoryTables.remove(inMemoryTable);
            }
        } finally {
            writeLock.unlock();
        }

    }

    /**
     * Sort the segments based on the segment name
     */
    public void sortSliceBasedOnLoadName(String tableName) {

        try {
            writeLock.lock();
            List<InMemoryTable> inMemoryTables = slices.get(tableName);
            if (null != inMemoryTables) {
                Collections.sort(inMemoryTables);
            }
        } finally {
            writeLock.unlock();
        }
    }
}
