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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.core.metadata.CarbonMetadata.Cube;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.metadata.CarbonSchemaReader;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;
import org.carbondata.core.vo.HybridStoreModel;
import org.carbondata.query.datastorage.cache.CarbonLRULevelCache;
import org.carbondata.query.datastorage.cache.LevelInfo;
import org.carbondata.query.util.CacheUtil;
import org.carbondata.query.util.CarbonEngineLogEvent;

public class InMemoryTable implements Comparable<InMemoryTable> {
    /**
     *
     */
    private static final byte ACTIVE = 0;
    /**
     *
     */
    private static final byte READY_TO_CLEAN = 1;
    /**
     * Attribute for Carbon LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(InMemoryTable.class.getName());


    private static AtomicLong counter = new AtomicLong(0);
    /**
     *
     */
    private String cubeName;
    private String schemaName;
    /**
     * uniqueName with schemaName_cubeName
     */
    private String cubeUniqueName;
    /**
     * Map of table name and data store (Fact table, all aggregate tables)
     */
    private Map<String, TableDataStore> dataCacheMap =
            new HashMap<String, TableDataStore>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    /**
     * All dimensions and its cache
     */
    private Map<String, DimensionHierarichyStore> dimesionCache =
            new HashMap<String, DimensionHierarichyStore>(
                    CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    /**
     * Can hold members, each level
     * column name and the cache
     */
    private Map<String, MemberStore> membersCache =
            new HashMap<String, MemberStore>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    /**
     * carbon cube
     */
    private CarbonDef.Cube carbonCube;
    private CarbonDef.Schema schema;
    /**
     *
     */
    private RestructureStore rsStore;
    /**
     *
     */
    private TableSlicePathInfo tableSlicePathInfo;
    /**
     * All the cubes loaded from file or Slice are slices. Only cube loaded
     * initially from persistent store is main cube as of now.
     */
    private boolean isSlice = true;
    /**
     *
     */
    private long id;
    /**
     *
     */
    private byte cubeStatus;
    /**
     * factTableName
     */
    private String factTableName;
    private String loadName;
    private int[] dimensionCardinality;
    private KeyGenerator keyGenerator;
    private String tableName;
    private CarbonLRULevelCache levelCache;
    private Cube metaCube;
    private HybridStoreModel hybridStoreModel;
    /**
     *  segment modification time
     */
    private long modificationTime;
    /**
     * File store path
     */
    private String fileStore;

    public InMemoryTable(CarbonDef.Schema schema, CarbonDef.Cube cube, Cube metaCube,
            String tableName, String fileStore, long modificationTime) {
        this.cubeName = cube.name;
        this.carbonCube = cube;
        this.schema = schema;
        this.metaCube = metaCube;
        this.schemaName = schema.name;
        this.cubeUniqueName = this.schemaName + '_' + this.cubeName;
        this.id = counter.incrementAndGet();
        this.factTableName = CarbonSchemaReader.getFactTableName(cube);
        this.levelCache = CarbonLRULevelCache.getInstance();
        this.tableName = tableName;
        this.fileStore = fileStore;
        this.modificationTime = modificationTime;
    }



    /**
     * CarbonCube
     */
    public CarbonDef.Cube getCarbonCube() {
        return carbonCube;
    }

    /**
     * @return
     */
    public boolean isSlice() {
        return isSlice;
    }

    /**
     * @return
     */
    public TableSlicePathInfo getTableSlicePathInfo() {
        return tableSlicePathInfo;
    }

    /**
     * Getter for data cache reference. CubeDataCache provide the details
     * access/process methods for fact table/aggregate tables.
     */
    public TableDataStore getDataCache(String tableName) {
        return dataCacheMap.get(tableName);
    }

    /**
     * @return the cubeName
     */
    public String getCubeName() {
        return cubeName;
    }

    /**
     * @param dimension
     * @return
     */
    public DimensionHierarichyStore getDimensionAndHierarchyCache(String dimension) {
        return dimesionCache.get(dimension);
    }

    /**
     * getStartKey
     *
     * @param tableName
     * @return byte[]
     */
    public byte[] getStartKey(String tableName) {
        return dataCacheMap.get(tableName).getStartKey();
    }

    /**
     * getKeyGenerator
     *
     * @param tableName
     * @return KeyGenerator
     */
    public KeyGenerator getKeyGenerator(String tableName) {
        return keyGenerator;
    }

    /**
     * Load the cube cache from a file storage.
     *
     */
    public void loadCacheFromFile(boolean loadOnlyLevelFiles) {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        String cubeUniqueName = schemaName + '_' + cubeName;
        CarbonFile file = FileFactory.getCarbonFile(fileStore, FileFactory.getFileType(fileStore));
        if (file.isDirectory()) {
            getDimensionCardinality(file, tableName);
            List<Dimension> dimensions = metaCube.getDimensions(tableName);

            boolean[] dimensionStoreType = new boolean[dimensionCardinality.length];
            List<Integer> NoDictionaryDimOrdinals = new ArrayList<Integer>();
            for (Dimension dimension : dimensions) {
                if (dimension.isNoDictionaryDim()) {
                    NoDictionaryDimOrdinals.add(dimension.getOrdinal());
                    continue;
                }
                if (dimension.isColumnar()) {
                    dimensionStoreType[dimension.getOrdinal()] = dimension.isColumnar();
                }
            }
            hybridStoreModel = CarbonUtil
                    .getHybridStoreMeta(findRequiredDimensionForStartAndEndKey(),
                            dimensionStoreType, NoDictionaryDimOrdinals);
            keyGenerator = KeyGeneratorFactory
                    .getKeyGenerator(hybridStoreModel.getHybridCardinality(),
                            hybridStoreModel.getDimensionPartitioner());
            int startAndEndKeySizeWithPrimitives = keyGenerator.getKeySizeInBytes();
            keyGenerator.setStartAndEndKeySizeWithOnlyPrimitives(startAndEndKeySizeWithPrimitives);
        }
        // Process fact and aggregate data cache
        if (!loadOnlyLevelFiles) {
            for (String table : metaCube.getTablesList()) {
                if (!table.equals(tableName)) {
                    continue;
                }
                TableDataStore dataCache =
                        new TableDataStore(table, metaCube, rsStore.getSliceMetaCache(table),
                                keyGenerator, dimensionCardinality, hybridStoreModel);
                //add start and end key size with only primitives
                if (dataCache.loadDataFromFile(fileStore,
                        keyGenerator.getStartAndEndKeySizeWithOnlyPrimitives())) {
                    dataCacheMap.put(table, dataCache);
                }
            }
        }
        String loadFolderName =
                fileStore.substring(fileStore.lastIndexOf(CarbonCommonConstants.LOAD_FOLDER));
        CarbonDef.CubeDimension dimension = null;
        // Process dimension and hierarchies cache
        for (int i = 0; i < carbonCube.dimensions.length; i++) {
            dimension = carbonCube.dimensions[i];
            if (dimension.visible && !dimension.noDictionary) {
                DimensionHierarichyStore cache =
                        new DimensionHierarichyStore(dimension, membersCache, cubeUniqueName,
                                factTableName, schema);
                String levelActualName = CacheUtil.getLevelActualName(schema, dimension);
                String fileName = fileStore + File.separator + factTableName + '_' + levelActualName
                        + CarbonCommonConstants.LEVEL_FILE_EXTENSION;
                if (InMemoryTableStore.getInstance().isLevelCacheEnabled() && tableName
                        .equals(factTableName) && CacheUtil.isFileExists(fileName)) {
                    String levelCacheKey =
                            cubeUniqueName + '_' + loadFolderName + '_' + levelActualName;
                    if (null == levelCache.get(levelCacheKey)) {
                        long memberFileSize = CacheUtil.getMemberFileSize(fileName);
                        LevelInfo levelInfo =
                                new LevelInfo(memberFileSize, dimension.name, levelActualName,
                                        factTableName, fileStore, loadFolderName);
                        levelInfo.setLoaded(false);
                        levelCache.put(levelCacheKey, levelInfo);
                    }
                } else {
                    cache.processCacheFromFileStore(fileStore, executorService);
                }
                dimesionCache.put(dimension.name, cache);
            }
        }
        tableSlicePathInfo = new TableSlicePathInfo(fileStore);

        try {
            executorService.shutdown();
            executorService.awaitTermination(2, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e);
        }
    }

    private int[] findRequiredDimensionForStartAndEndKey() {
        List<Integer> dimCardinalities = new ArrayList<Integer>();
        for (int dimCard : dimensionCardinality) {
            if (dimCard != 0) dimCardinalities.add(dimCard);
            else break;
        }
        int[] primitiveDimsForKey = new int[dimCardinalities.size()];
        for (int i = 0; i < dimCardinalities.size(); i++) {
            primitiveDimsForKey[i] = dimCardinalities.get(i);
        }
        return primitiveDimsForKey;
    }

    /**
     * @param file
     */
    private int[] getDimensionCardinality(CarbonFile file, final String tableName) {
        dimensionCardinality = new int[0];
        CarbonFile[] files = file.listFiles(new CarbonFileFilter() {
            public boolean accept(CarbonFile pathname) {
                return (!pathname.isDirectory()) && pathname.getName()
                        .startsWith(CarbonCommonConstants.LEVEL_METADATA_FILE) && pathname.getName()
                        .endsWith(tableName + ".metadata");
            }

        });

        if (files.length <= 0) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                    "Level Cardinality file not found in path : " + file.getAbsolutePath());
            return dimensionCardinality;
        }

        try {
            dimensionCardinality =
                    CarbonUtil.getCardinalityFromLevelMetadataFile(files[0].getAbsolutePath());
        } catch (CarbonUtilException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e);
        }
        return dimensionCardinality;
    }

    /**
     * Access the dimension members through levelName
     */
    public MemberStore getMemberCache(String levelName) {
        return membersCache.get(levelName);
    }

    /**
     * Returns
     */
    public long getID() {
        return id;
    }

    /**
     * Gives the current cube status is active or not.
     */
    public boolean isActive() {
        if (cubeStatus == ACTIVE) {
            return true;
        }
        return false;
    }

    /**
     * Marks this slice dirty so that it can be cleaned once all the dependent
     * queries are finished their execution.
     */
    public void setCubeMerged() {
        cubeStatus = READY_TO_CLEAN;
    }

    /**
     * Try clearing the resources
     */
    public void clean() {
        dimesionCache.clear();
        membersCache.clear();
        for (TableDataStore store : dataCacheMap.values()) {
            store.clear();
        }
        dataCacheMap.clear();
    }

    /**
     * @return
     */
    public long getSize() {
        // TODO current data size is only fact table size. Need to consider
        // hierarchies and dimensions also.
        return dataCacheMap.get(this.factTableName).getSize();
    }

    /**
     * @return the rsStore
     */
    public RestructureStore getRsStore() {
        return rsStore;
    }

    /**
     * @param rsStore the rsStore to set
     */
    public void setRsStore(RestructureStore rsStore) {
        this.rsStore = rsStore;
    }

    /**
     * getSchemaName
     *
     * @return String
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * getCubeUniqueName
     *
     * @return String
     */
    public String getCubeUniqueName() {
        return cubeUniqueName;
    }

    /**
     * @return Returns the factTableName.
     */
    public String getFactTableName() {
        return factTableName;
    }

    /**
     * @param factTableName The factTableName to set.
     */
    public void setFactTableName(String factTableName) {
        this.factTableName = factTableName;
    }

    public CarbonDef.Schema getSchema() {
        return schema;
    }

    public void setSchema(CarbonDef.Schema schema) {
        this.schema = schema;
    }

    public String getLoadName() {
        return loadName;
    }

    public void setLoadName(String loadName) {
        this.loadName = loadName;

    }

    /**
     * @return Returns the tableName.
     */
    public String getTableName() {
        return tableName;
    }

    public int getLoadId() {
        int lastIndexOf = loadName.lastIndexOf(CarbonCommonConstants.LOAD_FOLDER);
        int loadNumber = -1;
        try {
            if (isLoadMerged(loadName)) {
                String loadNum = loadName.substring(loadName.indexOf('_') + 1,
                        loadName.indexOf(CarbonCommonConstants.MERGERD_EXTENSION));
                loadNumber = Integer.parseInt(loadNum);
            } else {
                loadNumber = Integer.parseInt(loadName.substring(lastIndexOf + 5));
            }
        } catch (NumberFormatException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
                    "Problem while getting the load number");
        }
        return loadNumber;
    }

    @Override
    public int compareTo(InMemoryTable memCubeInstance) {
        String loadNameOfCurrntObj = this.getLoadName();
        String loadNameCurrntObj;

        if (isLoadMerged(loadNameOfCurrntObj)) {
            loadNameCurrntObj = loadNameOfCurrntObj.substring(loadNameOfCurrntObj.indexOf('_') + 1,
                    loadNameOfCurrntObj.indexOf(CarbonCommonConstants.MERGERD_EXTENSION));
        } else {
            loadNameCurrntObj = loadNameOfCurrntObj
                    .substring(loadNameOfCurrntObj.lastIndexOf('_') + 1,
                            loadNameOfCurrntObj.length());
        }
        int idOfCurrentObj = Integer.parseInt(loadNameCurrntObj);
        String loadNameOfComparableObj = memCubeInstance.getLoadName();

        if (isLoadMerged(loadNameOfComparableObj)) {
            loadNameOfComparableObj = loadNameOfComparableObj
                    .substring(loadNameOfComparableObj.indexOf('_') + 1, loadNameOfComparableObj
                            .indexOf(CarbonCommonConstants.MERGERD_EXTENSION));
        } else {
            loadNameOfComparableObj = loadNameOfComparableObj
                    .substring(loadNameOfComparableObj.lastIndexOf('_') + 1,
                            loadNameOfComparableObj.length());
        }
        int idOfCompObj = Integer.parseInt(loadNameOfComparableObj);
        return idOfCurrentObj - idOfCompObj;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof InMemoryTable)) {
            return false;
        }
        InMemoryTable memCubeInstance = (InMemoryTable) obj;
        return compareTo(memCubeInstance) == 0;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((loadName == null) ? 0 : loadName.hashCode());
        result = prime * result + ((loadName == null) ? 0 : loadName.hashCode());
        return result;
    }

    /**
     * @return Returns the dimensionCardinality.
     */
    public int[] getDimensionCardinality() {
        return dimensionCardinality;
    }

    /**
     * To check if this is a merged load or not.
     *
     * @param loadName
     * @return
     */
    private boolean isLoadMerged(String loadName) {
        if (loadName.contains(CarbonCommonConstants.MERGERD_EXTENSION)) {
            return true;
        }
        return false;
    }

    public HybridStoreModel getHybridStoreModel() {
        return this.hybridStoreModel;
    }

    /**
     * return the segment modification time
     * @return
     */
    public long getModificationTime() {
        return modificationTime;
    }
}
