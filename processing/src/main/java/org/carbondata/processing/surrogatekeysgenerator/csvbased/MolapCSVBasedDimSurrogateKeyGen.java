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

package org.carbondata.processing.surrogatekeysgenerator.csvbased;

import java.io.File;
import java.io.OutputStream;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.carbondata.processing.datatypes.GenericDataType;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.file.manager.composite.FileData;
import org.carbondata.core.file.manager.composite.IFileManagerComposite;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.processing.schema.metadata.ArrayWrapper;
import org.carbondata.processing.schema.metadata.MolapInfo;
import org.carbondata.processing.util.MolapDataProcessorLogEvent;
import org.carbondata.core.util.MolapUtil;
import org.carbondata.core.writer.HierarchyValueWriterForCSV;
import org.carbondata.core.writer.LevelValueWriter;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.pentaho.di.core.exception.KettleException;

public abstract class MolapCSVBasedDimSurrogateKeyGen {

    private static final LogService LOGGER =
            LogServiceFactory.getLogService(MolapCSVBasedDimSurrogateKeyGen.class.getName());
    /**
     * max
     */
    protected int[] max;
    /**
     * connection
     */
    protected Connection connection;
    /**
     * hierInsertFileNames
     */
    protected Map<String, String> hierInsertFileNames;
    /**
     * dimInsertFileNames
     */
    protected String[] dimInsertFileNames;
    /**
     * molapInfo
     */
    protected MolapInfo molapInfo;
    /**
     * measureValWriter
     */
    protected Map<String, LevelValueWriter> measureValWriterMap;
    protected IFileManagerComposite measureFilemanager;
    /**
     * primary key max surrogate key map
     */
    protected Map<String, Integer> primaryKeysMaxSurroagetMap;
    /**
     * Measure max surrogate key map
     */
    protected Map<String, Integer> measureMaxSurroagetMap;
    /**
     * File manager
     */
    protected IFileManagerComposite fileManager;
    /**
     * dimensionWriter
     */
    protected LevelValueWriter[] dimensionWriter;
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
     * dimsFiles
     */
    private String[] dimsFiles;
    /**
     * timeDimMax
     */
    private int[] timDimMax;
    /**
     * hierCache
     */
    private Map<String, Int2ObjectMap<int[]>> hierCache =
            new HashMap<String, Int2ObjectMap<int[]>>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    /**
     *
     */
    private Map<String, Map<ArrayWrapper, Integer>> hierCacheReverse =
            new HashMap<String, Map<ArrayWrapper, Integer>>(
                    MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    /**
     * rwLock2
     */
    private ReentrantReadWriteLock rwLock2 = new ReentrantReadWriteLock();
    /**
     * wLock2
     */
    protected Lock wLock2 = rwLock2.writeLock();
    /**
     * Store Folder Name with Load number.
     */
    private String storeFolderWithLoadNumber;

    /**
     * @param molapInfo MolapInfo With all the required details for surrogate key generation and
     *                  hierarchy entries.
     */
    public MolapCSVBasedDimSurrogateKeyGen(MolapInfo molapInfo) {
        this.molapInfo = molapInfo;

        setDimensionTables(molapInfo.getDimColNames());
        setHierFileNames(molapInfo.getHierTables());
    }

    public Integer generateSurrogateKeys(String tuples, String columnNames, int index,
            Object[] props) throws KettleException {
        Integer key = null;
        Map<String, Integer> cache = memberCache.get(columnNames);

        key = cache.get(tuples);
        if (key == null) {
            synchronized (cache) {
                key = cache.get(tuples);
                if (null == key) {
                    key = getSurrogateFromStore(tuples, index, props);
                    cache.put(tuples, key);
                }
            }

        }
        return key;
    }

    public void closeMeasureLevelValWriter() {

        if (null == measureFilemanager || null == measureValWriterMap) {
            return;
        }
        int fileMangerSize = measureFilemanager.size();

        for (int i = 0; i < fileMangerSize; i++) {
            FileData memberFile = (FileData) measureFilemanager.get(i);
            String msrLvlInProgressFileName = memberFile.getFileName();
            LevelValueWriter measureValueWriter = measureValWriterMap.get(msrLvlInProgressFileName);
            if (null == measureValueWriter) {
                continue;
            }

            // now write the byte array in the file.
            OutputStream bufferedOutputStream = measureValueWriter.getBufferedOutputStream();
            if (null == bufferedOutputStream) {
                continue;

            }
            MolapUtil.closeStreams(bufferedOutputStream);
            measureValueWriter.clearOutputStream();
            String storePath = memberFile.getStorePath();
            String levelFileName = measureValueWriter.getMemberFileName();
            int counter = measureValueWriter.getCounter();

            String changedFileName = levelFileName + (counter - 1);

            String inProgFileName = changedFileName + MolapCommonConstants.FILE_INPROGRESS_STATUS;

            File currentFile = new File(storePath + File.separator + inProgFileName);
            File destFile = new File(storePath + File.separator + changedFileName);

            if (!currentFile.exists()) {
                continue;
            }
            if (currentFile.length() == 0) {
                boolean isDeleted = currentFile.delete();
                if (!isDeleted) {
                    LOGGER.debug(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                            "Failed to delete file " + currentFile.getName());
                }
            }

            if (!currentFile.renameTo(destFile)) {
                LOGGER.debug(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Failed to rename from " + currentFile.getName() + " to " + destFile
                                .getName());
            }
        }

    }

    public Integer generateSurrogateKeysForTimeDims(String tuples, String columnName, int index,
            Object[] props) throws KettleException {
        Integer key = null;
        Map<String, Integer> cache = memberCache.get(columnName);

        key = cache.get(tuples);
        if (key == null) {
            if (timDimMax[index] >= molapInfo.getMaxKeys()[index]) {
                if (MolapCommonConstants.MEMBER_DEFAULT_VAL.equals(tuples)) {
                    tuples = null;
                }
                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Invalid cardinality. Key size exceeded cardinality for: " + molapInfo
                                .getDimColNames()[index] + ": MemberValue: " + tuples);
                return -1;
            }
            timDimMax[index]++;
            Map<String, Integer> timeCache = timeDimCache.get(columnName);
            // Extract properties from tuple
            // Need to create a new surrogate key.
            key = getSurrogateFromStore(tuples, index, props);
            cache.put(tuples, key);
            if (null != timeCache) {
                timeCache.put(tuples, key);
            }
        } else {

            return updateSurrogateToStore(tuples, columnName, index, key, props);
        }
        return key;
    }

    public void checkHierExists(int[] val, String hier, int primaryKey) throws KettleException {
        Int2ObjectMap<int[]> cache = hierCache.get(hier);

        int[] hCache = cache.get(primaryKey);
        if (hCache != null && Arrays.equals(hCache, val)) {
            return;
        } else {
            wLock2.lock();
            try {
                // Store in cache
                cache.put(primaryKey, val);
            } finally {
                wLock2.unlock();
            }
        }
    }

    public void checkNormalizedHierExists(int[] val, String hier,
            HierarchyValueWriterForCSV hierWriter) throws KettleException {
        Map<ArrayWrapper, Integer> cache = hierCacheReverse.get(hier);

        ArrayWrapper wrapper = new ArrayWrapper(val);
        Integer hCache = cache.get(wrapper);
        if (hCache != null) {
            return;
        } else {
            wLock2.lock();
            try {
                getNormalizedHierFromStore(val, hier, 1, hierWriter);
                // Store in cache
                cache.put(wrapper, 1);
            } finally {
                wLock2.unlock();
            }
        }
    }

    public void close() throws Exception {
        if (null != connection) {
            connection.close();
        }
    }

    public abstract void writeHeirDataToFileAndCloseStreams()
            throws KettleException, KeyGenException;

    public abstract void writeDataToFileAndCloseStreams() throws KettleException, KeyGenException;

    /**
     * Search entry and insert if not found in store.
     *
     * @param val
     * @param hier
     * @return
     * @throws KeyGenException
     * @throws KettleException
     */
    protected abstract byte[] getHierFromStore(int[] val, String hier, int primaryKey)
            throws KettleException;

    /**
     * Search entry and insert if not found in store.
     *
     * @param val
     * @param hier
     * @return
     * @throws KeyGenException
     * @throws KettleException
     */
    protected abstract byte[] getNormalizedHierFromStore(int[] val, String hier, int primaryKey,
            HierarchyValueWriterForCSV hierWriter) throws KettleException;

    /**
     * Search entry and insert if not found in store.
     *
     * @param value
     * @param index
     * @param properties - Ordinal column, name column and all other properties
     * @return
     * @throws KettleException
     */
    protected abstract int getSurrogateFromStore(String value, int index, Object[] properties)
            throws KettleException;

    /**
     * Search entry and insert if not found in store.
     *
     * @param value
     * @param columnName
     * @param index
     * @param properties - Ordinal column, name column and all other properties
     * @return
     * @throws KettleException
     */
    protected abstract int updateSurrogateToStore(String value, String columnName, int index,
            int key, Object[] properties) throws KettleException;

    /**
     * generate the surroagate key for the primary keys.
     *
     * @return
     * @throws KettleException
     */
    public abstract int getSurrogateKeyForPrimaryKey(String value, String fileName,
            LevelValueWriter levelValueWriter) throws KettleException;

    /**
     * generate the surroagate key for the measure values.
     *
     * @return
     * @throws KettleException
     */
    public abstract int getSurrogateForMeasure(String tuple, String columnName, int index)
            throws KettleException;

    private Int2ObjectMap<int[]> getHCache(String hName) {
        Int2ObjectMap<int[]> hCache = hierCache.get(hName);
        if (hCache == null) {
            hCache = new Int2ObjectOpenHashMap<int[]>();
            hierCache.put(hName, hCache);
        }

        return hCache;
    }

    private Map<ArrayWrapper, Integer> getHCacheReverse(String hName) {
        Map<ArrayWrapper, Integer> hCache = hierCacheReverse.get(hName);
        if (hCache == null) {
            hCache = new HashMap<ArrayWrapper, Integer>(
                    MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
            hierCacheReverse.put(hName, hCache);
        }

        return hCache;
    }

    private void setHierFileNames(Set<String> set) {
        hierInsertFileNames =
                new HashMap<String, String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (String s : set) {
            hierInsertFileNames.put(s, s + MolapCommonConstants.HIERARCHY_FILE_EXTENSION);

            // fix hierStream is null issue
            getHCache(s);
            getHCacheReverse(s);
        }
    }

    private void setDimensionTables(String[] dimeFileNames) {
        int noOfPrimitiveDims = 0;
        List<String> dimFilesForPrimitives = new ArrayList<String>();
        memberCache = new ConcurrentHashMap<String, Map<String, Integer>>();
        for (int i = 0; i < dimeFileNames.length; i++) {
            GenericDataType complexType = molapInfo.getComplexTypesMap()
                    .get(dimeFileNames[i].substring(molapInfo.getTableName().length() + 1));
            if (complexType != null) {
                List<GenericDataType> primitiveChild = new ArrayList<GenericDataType>();
                complexType.getAllPrimitiveChildren(primitiveChild);
                for (GenericDataType eachPrimitive : primitiveChild) {
                    memberCache.put(molapInfo.getTableName() + "_" + eachPrimitive.getName(),
                            new ConcurrentHashMap<String, Integer>());
                    dimFilesForPrimitives
                            .add(molapInfo.getTableName() + "_" + eachPrimitive.getName());
                    eachPrimitive.setSurrogateIndex(noOfPrimitiveDims);
                    noOfPrimitiveDims++;
                }
            } else {
                memberCache.put(dimeFileNames[i], new ConcurrentHashMap<String, Integer>());
                dimFilesForPrimitives.add(dimeFileNames[i]);
                noOfPrimitiveDims++;
            }
        }
        max = new int[noOfPrimitiveDims];
        this.dimsFiles = dimFilesForPrimitives.toArray(new String[dimFilesForPrimitives.size()]);

        createRespectiveDimFilesForDimTables();
    }

    private void createRespectiveDimFilesForDimTables() {
        int dimCount = this.dimsFiles.length;
        dimInsertFileNames = new String[dimCount];
        System.arraycopy(dimsFiles, 0, dimInsertFileNames, 0, dimCount);
    }

    /**
     * isCacheFilled
     *
     * @param columnNames
     * @return boolean
     */
    public abstract boolean isCacheFilled(String[] columnNames);

    /**
     * @return Returns the storeFolderWithLoadNumber.
     */
    public String getStoreFolderWithLoadNumber() {
        return storeFolderWithLoadNumber;
    }

    /**
     * @param storeFolderWithLoadNumber The storeFolderWithLoadNumber to set.
     */
    public void setStoreFolderWithLoadNumber(String storeFolderWithLoadNumber) {
        this.storeFolderWithLoadNumber = storeFolderWithLoadNumber;
    }

    /**
     * @return Returns the memberCache.
     */
    public Map<String, Map<String, Integer>> getMemberCache() {
        return memberCache;
    }

    /**
     * @param memberCache The memberCache to set.
     */
    public void setMemberCache(Map<String, Map<String, Integer>> memberCache) {
        this.memberCache = memberCache;
    }

    /**
     * @return Returns the timeDimCache.
     */
    public Map<String, Map<String, Integer>> getTimeDimCache() {
        return timeDimCache;
    }

    /**
     * @param timeDimCache The timeDimCache to set.
     */
    public void setTimeDimCache(Map<String, Map<String, Integer>> timeDimCache) {
        this.timeDimCache = timeDimCache;
    }

    /**
     * @return Returns the dimsFiles.
     */
    public String[] getDimsFiles() {
        return dimsFiles;
    }

    /**
     * @param dimsFiles The dimsFiles to set.
     */
    public void setDimsFiles(String[] dimsFiles) {
        this.dimsFiles = dimsFiles;
    }

    /**
     * @return Returns the hierCache.
     */
    public Map<String, Int2ObjectMap<int[]>> getHierCache() {
        return hierCache;
    }

    /**
     * @param hierCache The hierCache to set.
     */
    public void setHierCache(Map<String, Int2ObjectMap<int[]>> hierCache) {
        this.hierCache = hierCache;
    }

    /**
     * @return Returns the timDimMax.
     */
    public int[] getTimDimMax() {
        return timDimMax;
    }

    /**
     * @param timDimMax The timDimMax to set.
     */
    public void setTimDimMax(int[] timDimMax) {
        this.timDimMax = timDimMax;
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

    public int[] getMax() {
        return max;
    }

    public void setMax(int[] max) {
        this.max = max;
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
