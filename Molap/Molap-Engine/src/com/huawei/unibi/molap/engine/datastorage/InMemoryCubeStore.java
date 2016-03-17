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

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdh/HjOjN0Brs7b7TRorj6S6iAIeaqK90lj7BAM
GSGxBvQ+nH6KDW9rKk8VMGGhEP4VVbZTHy2TcNLB0ErjDZcMLSsskr8VUmlc6HM+z9SeYg9o
6DN1utx92ZWHDUIfOEaiSmetXdaHyAlv7wIrawv6BSipTmcwS5k8yJaBZLPFEQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.datastorage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFileFilter;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.engine.datastorage.cache.LevelInfo;
import com.huawei.unibi.molap.engine.datastorage.cache.MolapLRULevelCache;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.metadata.MolapMetadata;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.metadata.SliceMetaData;
import com.huawei.unibi.molap.olap.MolapDef;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * @author K00900207
 * 
 */
public final class InMemoryCubeStore
{

    /**
     * 
     */
    private static InMemoryCubeStore instance = new InMemoryCubeStore();

    /**
     * 
     */
    private Map<String,MolapDef.Cube> cubeNameAndCubeMap = new ConcurrentHashMap<String, MolapDef.Cube>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    /**
     * Unique key for cube from rolap schema maps to its data cache
     */
    private Map<String, List<RestructureStore>> cubeSliceMap = new ConcurrentHashMap<String, List<RestructureStore>>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    
    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(InMemoryCubeStore.class.getName());
    
    /**
     * Attribute for QUERY_AVAILABLE 
     */
    public static final byte QUERY_AVAILABLE = 0;

    /**
     * Attribute for QUERY_BLOCK
     */
    private static final byte QUERY_BLOCK = 1;

    /**
     * Attribute for QUERY_WAITING
     */
    public static final byte QUERY_WAITING = 2;

    /**
     * 
     */
    private static final byte QUERY_FINISHED_FOR_RELOAD = 3;
    
    /**
     * folder name where molap data writer will write 
     */
    private static final String FOLDER_NAME = "Load_";
    
    /**
     * restructure folder name where molap data writer will write 
     */
    private static final String RS_FOLDER_NAME = "RS_";

    // private static final byte QUERY_FINISHED_WAIT_PUBLISH = 4;

    /**
     * Map<cubeName, QUERY_EXECUTE_STATUS> the map about waiting type of cube
     */
    private Map<String, Byte> queryExecuteStatusMap = new ConcurrentHashMap<String, Byte>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

    /**
     * 
     */
    private static final boolean SLICE_LIST_CONCURRENT = false;

    /**
     * contains the mapping of cubeName to Schema Map<cubeName, schema>
     * 
     * @author Sojer z00218041
     */
    private Map<String, MolapDef.Schema> mapCubeToSchema = new ConcurrentHashMap<String, MolapDef.Schema>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    private static ConcurrentHashMap<String,CubeLockInstance> mapOfCubeInstance = new ConcurrentHashMap<String,CubeLockInstance>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

    private Map<String, Integer> tableAndCurrentRSMap = new ConcurrentHashMap<String, Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    
    private Map<String, Long> cubeNameAndCreationTime = new ConcurrentHashMap<String, Long>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    
    /**
     * Dummy constructor
     */
    private InMemoryCubeStore()
    {

    }

    /**
     * @return
     */
    public static InMemoryCubeStore getInstance()
    {
        return instance;
    }

    /**
     * @param name
     * @return
     */
    public MolapDef.Cube getRolapCube(String name)
    {
        return cubeNameAndCubeMap.get(name);
    }

    /**
     * @param cubeKey
     * @return
     */
    public boolean findCache(String cubeKey)
    {
        List<RestructureStore> slices = cubeSliceMap.get(cubeKey);
        if(slices != null && slices.size() > 0)
        {
            return slices.get(0).isSlicesAvailable();
        }

        return false;
    }

    /**
     * @param cubeKey
     */
    public void clearCache(String cubeKey)
    {
        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Removed cube from InMemory : " + cubeKey);
        cubeSliceMap.remove(cubeKey);
        queryExecuteStatusMap.remove(cubeKey);
        cubeNameAndCubeMap.remove(cubeKey);
        clearLevelLRUCache(cubeKey);
    }
    
    /**
     * @param key
     */
    public void clearTableAndCurrentRSMap(String key)
    {
        tableAndCurrentRSMap.remove(key);
    }
    
    /**
     * 
     * @param cubeKey
     * 
     */
    public void clearLevelLRUCache(String cubeKey)
    {
        if(isLevelCacheEnabled())
        {
            MolapLRULevelCache levelCache = MolapLRULevelCache.getInstance();
            levelCache.removeAllKeysForGivenCube(cubeKey);
        }
    }
    
    /**
     * Clears the cache 
     * @throws Exception 
     */
    public void flushCache()
    {
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Removed all cubes from cache : ");
            cubeSliceMap.clear();
            queryExecuteStatusMap.clear();
            cubeNameAndCubeMap.clear();
            mapCubeToSchema.clear();
            MolapUtil.flushSEQGenLruCache();
    }

    /**
     * @param sliceUpdatedLoadPaths 
     * @param rolapCube cube to be loaded
     * 
     *Example basepath Content structure
* RS_0
     */
    public void loadCube(MolapDef.Schema schema, Cube metadataCube, String partitionId, List<String> listLoadFolders,
            List<String> sliceUpdatedLoadPaths, final String factTableName, String basePath, final int currentRestructNumber, long cubeCreationTime)
    {
        MolapDef.Cube cube = schema.cubes[0];
        String cubeName = cube.name;
        String schemaName = schema.name;
        String cubeUniqueName = schemaName + '_' + cubeName;
        synchronized(mapOfCubeInstance.get(cubeUniqueName))
        {
            validateCubeCreationTime(cubeCreationTime, cubeUniqueName, metadataCube);
        List<RestructureStore> slices = cubeSliceMap.get(cubeUniqueName);
        listLoadFolders = removeAlreadyLoadedFoldersFromList(listLoadFolders, sliceUpdatedLoadPaths, slices,
                factTableName);
//        String basePath = MolapProperties.getInstance().getProperty(MolapCommonConstants.STORE_LOCATION,
//                MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL);
        basePath = basePath + File.separator + schema.name + File.separator + cubeName;

        FileType fileType = FileFactory.getFileType(basePath);
        MolapFile file = null;
        MolapFile[] list = null;
        try
        {
            if(FileFactory.isFileExist(basePath, fileType))
            {
                file = FileFactory.getMolapFile(basePath, fileType);
                list = file.listFiles(new MolapFileFilter()
                {
                    @Override
                    public boolean accept(MolapFile pathname)
                    {
                        String name = pathname.getName();
                        String[] splits = name.split(RS_FOLDER_NAME);
                        if (2 == splits.length)
                        {
                            try
                            {
                                if(Integer.parseInt(splits[1]) <= currentRestructNumber || -1 == currentRestructNumber)
                                {
                                    return (pathname.isDirectory()) && name.startsWith(RS_FOLDER_NAME)
                                            && !(name.indexOf(MolapCommonConstants.FILE_INPROGRESS_STATUS) > -1);
                                }
                            }
                            catch(NumberFormatException e)
                            {
                                return false;
                            }
                        }
                        
                        return false;
                    }
                });
            }
        }
        catch(IOException e)
        {
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "File does not exist :: " + e.getMessage());
        }
        if(null != file && file.exists() && null != list && list.length != 0)
        {
            if(null == slices)
            {
                slices = new ArrayList<RestructureStore>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            }
            slices = loadSliceFromFile(cube, basePath, schema, listLoadFolders, sliceUpdatedLoadPaths, slices, currentRestructNumber, factTableName, metadataCube);
            if(null!=slices)
            {
                Collections.sort(slices, new Comparator<RestructureStore>()
                {
                    public int compare(RestructureStore o1, RestructureStore o2)
                    {
                        String firstFileName = o1.getFolderName();
                        String secondFileName = o2.getFolderName();
                        int lastIndexOffile1 = firstFileName.lastIndexOf(MolapCommonConstants.RESTRUCTRE_FOLDER);
                        int lastIndexOffile2 = secondFileName.lastIndexOf(MolapCommonConstants.RESTRUCTRE_FOLDER);
                        int f1 = 0;
                        int f2 = 0;
                        try
                        {
                            f1 = Integer.parseInt(firstFileName.substring(lastIndexOffile1 + 3));
                            f2 = Integer.parseInt(secondFileName.substring(lastIndexOffile2 + 3));
                            if(f1-f2==0)
                            {
                                int lsize =(null == o1.getSlices(factTableName))?0:o1.getSlices(factTableName).size();
                                int rsize =(null == o2.getSlices(factTableName))?0:o2.getSlices(factTableName).size();
                                if(lsize == 0 || rsize == 0)
                                {
                                    return lsize - rsize;
                                }
                                else
                                {
                                    return o1.getSlices(factTableName).get(lsize - 1)
                                            .getLoadId()
                                            - o2.getSlices(factTableName).get(rsize - 1)
                                                    .getLoadId();
                                }
                            }
                        }
                        catch(NumberFormatException e)
                        {
                            return -1;
                        }
                        return f1-f2;
                    }
                });
            }
        }
        cubeNameAndCubeMap.put(cubeUniqueName, cube);
        if(null != slices)
        {
            cubeSliceMap.put(cubeUniqueName, slices);
        }
        queryExecuteStatusMap.put(cubeUniqueName, QUERY_AVAILABLE);
        mapCubeToSchema.put(cubeUniqueName, schema);
        }
    }
    
    /**
     * This method will load the cube metadata (dimensions and measures) if
     * required
     * 
     * @param schema
     * @param cube
     * @param partitionId
     * @param schemaLastUpdatedTime
     * @return
     * 
     */
    public Cube loadCubeMetadataIfRequired(MolapDef.Schema schema, MolapDef.Cube cube, String partitionId,
            long schemaLastUpdatedTime)
    {
        if(null != partitionId)
        {
            schema.name = schema.name + '_' + partitionId;
            cube.name = cube.name + '_' + partitionId;
        }
        String cubeUniqueName = schema.name + '_' + cube.name;
        Cube loadedCube = null;
        CubeLockInstance cubeLockInstance = new CubeLockInstance();
        CubeLockInstance lockReference = mapOfCubeInstance.putIfAbsent(cubeUniqueName, cubeLockInstance);
        if(null == lockReference)
        {
            lockReference = cubeLockInstance;
        }
        loadedCube = MolapMetadata.getInstance().getCube(cubeUniqueName);
        if(null == loadedCube || schemaLastUpdatedTime != loadedCube.getSchemaLastUpdatedTime())
        {
            synchronized(lockReference)
            {
                loadedCube = MolapMetadata.getInstance().getCube(cubeUniqueName);
                if(null == loadedCube || schemaLastUpdatedTime != loadedCube.getSchemaLastUpdatedTime())
                {
                    MolapMetadata.getInstance().loadCube(schema, schema.name, cube.name, cube);
                    loadedCube = MolapMetadata.getInstance().getCube(cubeUniqueName);
                }
                loadedCube.setSchemaLastUpdatedTime(schemaLastUpdatedTime);
            }
        }
        return loadedCube;
    }

    /**
     * This method will sleep for 500 ms and recheck to acquire cube for loading
     * cube
     * 
     */
    private void waitToAcquireCube(long milliSeconds)
    {
        try
        {
            Thread.sleep(milliSeconds);
        }
        catch(InterruptedException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Interruped exception occurred :: " + e.getMessage());
        }
    }

    /**
     * 
     * @param cubeCreationTime
     * @param cubeUniqueName
     * 
     */
    private void validateCubeCreationTime(long cubeCreationTime, String cubeUniqueName, Cube metadataCube)
    {
        Long cubeCreationTimeInMap = cubeNameAndCreationTime.get(cubeUniqueName);
        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "cube creation time in map :: " + cubeCreationTimeInMap);
        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "cube creation time sent from driver :: " + cubeCreationTime);
        if(null == cubeCreationTimeInMap)
        {
            cubeNameAndCreationTime.put(cubeUniqueName, cubeCreationTime);
        }
        else if(cubeCreationTimeInMap != cubeCreationTime)
        {
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "*******clearing cache as time are different*******");
            performCubeCacheCleanUp(cubeUniqueName, metadataCube);
            cubeNameAndCreationTime.put(cubeUniqueName, cubeCreationTime);
        }
    }

    /**
     * 
     * @param cubeUniqueName
     * 
     */
    private void performCubeCacheCleanUp(String cubeUniqueName, Cube metadataCube)
    {
        clearCache(cubeUniqueName);
        Set<String> metaTables = metadataCube.getMetaTableNames();
        Iterator<String> tblItr = metaTables.iterator();
        while(tblItr.hasNext())
        {
            clearTableAndCurrentRSMap(cubeUniqueName+'_'+tblItr.next());
        }
    }
    
    /**
     * This method will update the level access count in level LRU cache
     * 
     * @param cubeUniqueName
     * 
     */
    public void updateLevelAccessCountInLRUCache(final String levelCacheUniqueId)
    {
        MolapLRULevelCache instance = MolapLRULevelCache.getInstance();
        LevelInfo levelInfo = instance.get(levelCacheUniqueId);
        if(null != levelInfo)
        {
            if(levelInfo.getAccessCount() > 0)
            {
                levelInfo.decrementAccessCount();
            }
            LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "*****level access count updated for level "
                    + levelCacheUniqueId + " in level LRU cache to :: " + levelInfo.getAccessCount());
        }
    }
    
    /**
     * This method will return true if level cache feature is enabled
     * 
     * @return
     * 
     */
    public boolean isLevelCacheEnabled()
    {
        MolapLRULevelCache levelCacheInstance = MolapLRULevelCache.getInstance();
        if(levelCacheInstance.getLRUCacheSize() >= 0)
        {
            return true;
        }
        return false;
    }
    
    /**
     * This method will check and load all the required levels in memory if they
     * are not loaded and fail in case the levels cannot be loaded
     * 
     * @param cubeUniqueName
     * @param columns
     * @param listLoadFolders
     * @return
     * @throws Exception
     * 
     */
    public List<String> loadRequiredLevels(final String cubeUniqueName, Set<String> columns,
            List<String> listLoadFolders) throws RuntimeException
    {
        List<LevelInfo> notLoadedLevels = new ArrayList<LevelInfo>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        List<String> levelCacheKeys = new ArrayList<String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        for(String colName : columns)
        {
            for(String load : listLoadFolders)
            {
                String key = cubeUniqueName + '_' + load + '_' + colName;
                checkAndAddToUnloadedLevelList(levelCacheKeys, notLoadedLevels, key);
            }
        }
        if(notLoadedLevels.size() > 0)
        {
            int retryCount = 0;
            long retryTimeInterval = MolapUtil.getRetryIntervalForLoadingLevelFile();
            while(!removeAndLoadLevelsIfRequired(levelCacheKeys, cubeUniqueName, notLoadedLevels))
            {
                waitToAcquireCube(retryTimeInterval);
                checkLevelLoadedStatus(cubeUniqueName, levelCacheKeys, notLoadedLevels);
                if(notLoadedLevels.isEmpty())
                {
                    break;
                }
                retryCount++;
                if(MolapCommonConstants.MAX_RETRY_COUNT == retryCount)
                {
                    for(String key : levelCacheKeys)
                    {
                        updateLevelAccessCountInLRUCache(key);
                    }
                    throw new RuntimeException("Required level files cannot be loaded in memory as size limit exceeded");
                }
            }
        }
        // return the level cache keys so that after completion there access
        // count can be decremented
        return levelCacheKeys;
    }
    
    /**
     * 
     * @param levelCacheKeys
     * @param notLoadedLevels
     * @param key
     * 
     */
    private void checkAndAddToUnloadedLevelList(List<String> levelCacheKeys, List<LevelInfo> notLoadedLevels, String key)
    {
        LevelInfo levelInfo = MolapLRULevelCache.getInstance().get(key);
        if(null != levelInfo)
        {
            if(!levelInfo.isLoaded())
            {
                notLoadedLevels.add(levelInfo);
            }
            else
            {
                levelInfo.incrementAccessCount();
                levelCacheKeys.add(key);
            }
        }
    }

    /**
     * 
     * @param cubeUniqueName
     * @param levelCacheKeys
     * @param notLoadedLevels
     * @param levelInfo
     * 
     */
    private void checkLevelLoadedStatus(String cubeUniqueName, List<String> levelCacheKeys,
            List<LevelInfo> notLoadedLevels)
    {
        Iterator<LevelInfo> iterator = notLoadedLevels.iterator();
        LevelInfo levelInfo = null;
        while(iterator.hasNext())
        {
            levelInfo = iterator.next();
            if(levelInfo.isLoaded())
            {
                levelInfo.incrementAccessCount();
                levelCacheKeys.add(cubeUniqueName + '_' + levelInfo.getLoadName() + '_' + levelInfo.getColumn());
                iterator.remove();
            }
        }
    }

    /**
     * 
     * @param cubeUniqueName
     * @param notLoadedLevels
     * @return
     * 
     */
    private boolean removeAndLoadLevelsIfRequired(List<String> levelCacheKey, String cubeUniqueName,
            List<LevelInfo> notLoadedLevels)
    {
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        List<InMemoryCube> activeSlices = getInstance().getActiveSlices(cubeUniqueName);
        DimensionHierarichyStore dimensionCache = null;
        for(LevelInfo info : notLoadedLevels)
        {
            for(InMemoryCube slice : activeSlices)
            {
                // check slice load number level info load name
                if(!slice.getLoadName().equals(info.getLoadName()) || !slice.getTableName().equals(info.getTableName()))
                {
                    continue;
                }
                dimensionCache = slice.getDimensionAndHierarchyCache(info.getName());
                if(null != dimensionCache)
                {
                    // add size check here and set loaded false in case
                    // level
                    // file is removed from cache
                    if(!checkAndRemoveFromLevelLRUCache(info))
                    {
                        return false;
                    }
                    // in case 2 queries come here for loading the same level
                    // then only one should go ahead and load that level
                    synchronized(info)
                    {
                        dimensionCache.processCacheFromFileStore(info.getFilePath(), executorService);
                    }
                    MolapLRULevelCache levelCacheInstance = MolapLRULevelCache.getInstance();
                    String key = cubeUniqueName + '_' + info.getLoadName() + '_' + info.getColumn();
                    levelCacheInstance.loadLevelInCache(key);
                    info.incrementAccessCount();
                    levelCacheKey.add(key);
                    break;
                }
            }
        }
        shoutDownExecutor(executorService);
        return true;
    }

    /**
     * 
     * @param executorService
     * 
     */
    private void shoutDownExecutor(ExecutorService executorService)
    {
        try
        {
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.DAYS);
        }
        catch(InterruptedException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
        }
    }

    /**
     * 
     * @param info
     * @return
     * 
     */
    private boolean checkAndRemoveFromLevelLRUCache(LevelInfo info)
    {
        MolapLRULevelCache levelCache = MolapLRULevelCache.getInstance();
        // check if required size is greater than total LRU cache size
        if(canRequiredLevelBeLoaded(info, levelCache))
        {
            // check if available size is LRU cache is sufficient to load the
            // required level
            if(isLevelDeletionFromCacheRequired(info, levelCache))
            {
                List<String> keysToBeRemoved = levelCache.getKeysToBeremoved(info.getFileSize());
                // this scenario will come when all the levels loaded in memory
                // are getting used or levels that can be unloaded from memory
                // does not free up the sufficient space
                if(keysToBeRemoved.isEmpty())
                {
                    return false;
                }
                for(String key : keysToBeRemoved)
                {
                    String cubeUniqueName = key.substring(0, (key.indexOf(MolapCommonConstants.LOAD_FOLDER) - 1));
                    LevelInfo levelInfo = levelCache.get(key);
                    unloadLevelFile(cubeUniqueName, levelInfo);
                    levelCache.unloadLevelInCache(key);
                }
            }
            return true;
        }
        return false;
    }
    
    /**
     * 
     * @param info
     * @return
     * 
     */
    private boolean isLevelDeletionFromCacheRequired(LevelInfo info, MolapLRULevelCache levelCache)
    {
        long requiredSize = info.getFileSize() + levelCache.getCurrentSize();
        if(requiredSize <= levelCache.getLRUCacheSize())
        {
            return false;
        }
        return true;
    }

    /**
     * 
     * @param info
     * @return
     * 
     */
    private boolean canRequiredLevelBeLoaded(LevelInfo info, MolapLRULevelCache levelCache)
    {
        if(info.getFileSize() > levelCache.getLRUCacheSize())
        {
            return false;
        }
        return true;
    }

    /**
     * 
     * @param cubeUniqueName
     * @param levelInfo
     * 
     */
    public void unloadLevelFile(String cubeUniqueName, LevelInfo levelInfo)
    {
        DimensionHierarichyStore dimensionCache = null;
        List<InMemoryCube> activeSlices = getInstance().getActiveSlices(cubeUniqueName);
        for(InMemoryCube slice : activeSlices)
        {
            // check slice load number with load id
            if(!slice.getLoadName().equals(levelInfo.getLoadName())
                    || !slice.getTableName().equals(levelInfo.getTableName()))
            {
                continue;
            }
            dimensionCache = slice.getDimensionAndHierarchyCache(levelInfo.getName());
            if(null != dimensionCache)
            {
                dimensionCache.unloadLevelFile(levelInfo.getTableName(), levelInfo.getName(), levelInfo.getName(),
                        levelInfo.getColumn());
                LOGGER.info(
                        MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                        "*****unloaded level file for cube " + cubeUniqueName + " with level name as "
                                + levelInfo.getLoadName() + '_' + levelInfo.getColumn());
                break;
            }
        }
    }

    /**
     * 
     * @param listLoadFolders
     * @param sliceUpdatedLoadPaths
     * @param slices
     * @param factTableName
     * @return
     */
    private List<String> removeAlreadyLoadedFoldersFromList(List<String> listLoadFolders,
            List<String> sliceUpdatedLoadPaths, List<RestructureStore> slices, String factTableName)
    {
        if(null != slices && null != factTableName)
        {
            Iterator<RestructureStore> itr = slices.iterator();
            RestructureStore rs = null;
            while(itr.hasNext())
            {
                rs = itr.next();
                List<InMemoryCube> listOfSlice = rs.getSlices(factTableName);
                if(null != listOfSlice)
                {
                    Iterator<InMemoryCube> itrForSliceList = listOfSlice.iterator();
                    while(itrForSliceList.hasNext())
                    {
                        InMemoryCube slice = itrForSliceList.next();
                        String sliceName = slice.getLoadName().substring(slice.getLoadName().indexOf('_') + 1,
                                slice.getLoadName().length());
                        if(listLoadFolders.contains(slice.getLoadName()) && null != sliceUpdatedLoadPaths
                                && !sliceUpdatedLoadPaths.contains(sliceName))
                        {
                            listLoadFolders.remove(slice.getLoadName());
                        }
                        else
                        {
                            itrForSliceList.remove();
                        }

                    }
                }

            }
        }
        return listLoadFolders;
    }

    /**
     * @param rolapCube
     * @param basePath
     */
//    public void updateCube(MolapDef.Cube rolapCube,String basePath, MolapDef.Schema schema)
//    {
//        String cubeUniqueName = schema.name+'_'+rolapCube.name;
//        List<RestructureStore> inmemoryCubeList = cubeSliceMap.get(cubeUniqueName);
//        //
//        MolapFile[] sortedFolderListList = getSortedFolderListList(basePath,RS_FOLDER_NAME);
//        //
//        if(null == sortedFolderListList)
//        {
//            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"sortedFolderListList is null so returned");
//            return;
//        }
//        for(MolapFile rsFolder : sortedFolderListList)
//        {
//            
//            MolapFile[] tableFiles = rsFolder.listFiles(new MolapFileFilter()
//            {
//                public boolean accept(MolapFile pathname)
//                {
//                    return (pathname.isDirectory());
//                }
//            });
//            RestructureStore rsStore = findRestructureStore(inmemoryCubeList,rsFolder.getName());
//            //Coverity Fix added null check
//            if(null != rsStore)
//            {
//                for(MolapFile tableFolder : tableFiles)
//                {
//                    SliceMetaData smd = readSliceMetaDataFile(tableFolder.getAbsolutePath() + File.separator
//                            + MolapCommonConstants.SLICE_METADATA_FILENAME);
//                    rsStore.setSliceMetaCache(smd, tableFolder.getName());
//                    List<InMemoryCube> slices = rsStore.getSlices(tableFolder.getName());
//                    for(InMemoryCube slice : slices)
//                    {
//                        slice.setRolapCube(rolapCube);
//                    }
//                    
//                }
//            }
//        }             
//    }
    
//    private RestructureStore findRestructureStore(List<RestructureStore> inmemoryCubeList,String folderName)
//    {
//        for(RestructureStore restructureStore : inmemoryCubeList)
//        {
//            if(restructureStore.getFolderName().equals(folderName))
//            {
//                return restructureStore;
//            }
//        }
//        return null;
//    }
    
    private SliceMetaData readSliceMetaDataFile(String path)
    {
        SliceMetaData readObject=null;
        InputStream stream = null;
        ObjectInputStream objectInputStream = null;
        //
        try
        {
            stream = FileFactory.getDataInputStream(path, FileFactory.getFileType(path));//new FileInputStream(path);
            objectInputStream = new ObjectInputStream(stream);
            readObject = (SliceMetaData)objectInputStream.readObject();
        }
        catch(ClassNotFoundException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
        }
        catch (FileNotFoundException e) 
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "@@@@@ SliceMetaData File is missing @@@@@ :"+path);
        }
        catch(IOException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "@@@@@ Error while reading SliceMetaData File @@@@@ :"+path);
        }
        finally
        {
          MolapUtil.closeStreams(objectInputStream,stream);
        }
        return readObject;
    }
    
    /**
     * @param rolapCube
     * @param basePath
     * @return
     */
//    private List<RestructureStore> loadSliceFromFile(MolapDef.Cube rolapCube,String basePath, MolapDef.Schema schema)
//    {
//        List<RestructureStore> slices = null;
//        //
//        MolapFile[] files = getSortedFolderListList(basePath,RS_FOLDER_NAME);
//
//        slices = new ArrayList<RestructureStore>();
//        //Coverity Fix
//        if( null == files)
//        {
//            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG," files are null so return empty array");
//            return slices; 
//        }
//        
//        int restructureId=0;
//        for(MolapFile rsFolder : files)
//        {
//            //
//            MolapFile[] tableFiles = rsFolder.listFiles(new MolapFileFilter()
//            {
//                public boolean accept(MolapFile pathname)
//                {
//                    return (pathname.isDirectory());
//                }
//            });
//            RestructureStore rsStore = new RestructureStore(rsFolder.getName(),restructureId++);
////            rsStore.setFolderName(rsFolder.getName());
//            for(MolapFile tableFolder : tableFiles)
//            {
//                //
//                SliceMetaData smd = readSliceMetaDataFile(tableFolder.getAbsolutePath() + File.separator
//                        + MolapCommonConstants.SLICE_METADATA_FILENAME);
//                String tableName = tableFolder.getName();
//                rsStore.setSliceMetaCache(smd, tableName);
//                MolapFile[] loadFiles = getSortedFolderListList(tableFolder.getAbsolutePath(),FOLDER_NAME);
//                for(MolapFile loadFolder : loadFiles)
//                {
//                    InMemoryCube cubeCache = new InMemoryCube(schema,rolapCube);
//                    cubeCache.setRsStore(rsStore);
//                    cubeCache.loadCacheFromFile(loadFolder.getAbsolutePath(),tableName);
//                    rsStore.setSlice(cubeCache, tableName);
//                }
//
//            }
//            slices.add(rsStore);
//            
//        }
//        return slices;
//    }
    
    
    
    /**
     * @param rolapCube 
     * @param basePath
     * @param sliceUpdatedLoadPaths 
     * @return
     */
    public List<RestructureStore> loadSliceFromFile(final MolapDef.Cube rolapCube, String basePath, final MolapDef.Schema schema, 
            final List<String> loadFolderNames, final List<String> sliceUpdatedLoadPaths,  final List<RestructureStore> slices, 
            int currentRestructNumber, String factTableName, final Cube metadataCube)
    {

        MolapFile[] files = getSortedFolderListList(basePath, RS_FOLDER_NAME, currentRestructNumber, true);
      
        // Coverity Fix
        if(null == files)
        {
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, " files are null so return empty array");
            return slices;
        }

        int restructureId = 0;
        Set<String> tableNames=new HashSet<String>(20);
        for(final MolapFile rsFolder : files)
        {
           
            //
            MolapFile[] tableFiles = rsFolder.listFiles(new MolapFileFilter()
            {
                public boolean accept(MolapFile pathname)
                {
                    return (pathname.isDirectory());
                }
            });

            final RestructureStore rsStore = new RestructureStore(rsFolder.getName(), restructureId++);
//            boolean updateReq = false;
            final List<Byte> flagList = new CopyOnWriteArrayList<Byte>();
            // rsStore.setFolderName(rsFolder.getName());
            for(MolapFile tableFolder : tableFiles)
            {
                ExecutorService executorService = Executors.newFixedThreadPool(5);
                SliceMetaData smd = readSliceMetaDataFile(tableFolder.getAbsolutePath() + '/' 
                        + MolapUtil.getSliceMetaDataFileName(currentRestructNumber));
                if(null == smd)
                {
                    continue;
                }
                final String tableName = tableFolder.getName();
                if(!tableName.equals(factTableName) && !tableName.equals(metadataCube.getFactTableName()))
                {
                    continue;
                }
                rsStore.setSliceMetaCache(smd, tableName);
                rsStore.setSliceMetaPathCache(tableFolder.getAbsolutePath(), tableName);
                addTableRestructuringNumber(schema.name+'_'+rolapCube.name+'_'+tableName,currentRestructNumber);
                MolapFile[] loadFiles = getSortedFolderListList(tableFolder.getAbsolutePath(), FOLDER_NAME, -1, false);
                if(null!=loadFiles){
                for(final MolapFile loadFolder : loadFiles)
                {
                  MolapFile[] listFiles = loadFolder.listFiles();
                    if(null==listFiles || listFiles.length==0)
                    {
                        continue; 
                    }
                    executorService.submit(new Runnable()
                    {
                            public void run()
                            {
                                if(!isLoadFolderAlreadyPresent(slices, sliceUpdatedLoadPaths, rsFolder, tableName,
                                        loadFolder.getName()))
                                {
                                    if(null != loadFolderNames)
                                    {
                                        if(!loadFolderNames.contains(loadFolder.getName()))
                                        {
                                            loadMemberCacheForEachLoad(rolapCube, schema, rsStore, tableName,
                                                    loadFolder,metadataCube);
                                        }
                                        else
                                        {
                                            InMemoryCube cubeCache = new InMemoryCube(schema, rolapCube, metadataCube);
                                            cubeCache.setLoadName(loadFolder.getName());
                                            cubeCache.setRsStore(rsStore);
                                            cubeCache.loadCacheFromFile(loadFolder.getAbsolutePath(), tableName, false);
                                            rsStore.setSlice(cubeCache, tableName);
                                        }
                                    }
                                    else
                                    {
                                        InMemoryCube cubeCache = new InMemoryCube(schema, rolapCube, metadataCube);
                                        cubeCache.setLoadName(loadFolder.getName());
                                        cubeCache.setRsStore(rsStore);
                                        cubeCache.loadCacheFromFile(loadFolder.getAbsolutePath(), tableName, false);
                                        rsStore.setSlice(cubeCache, tableName);
                                    }
                                    flagList.add((byte)1);
                                }
                            }
                    });
//                    updateReq=true;
                 }
                try
                {
                    executorService.shutdown();
                    executorService.awaitTermination(2, TimeUnit.DAYS);
                }
                catch(InterruptedException e)
                {
                    LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
                }
              }
                //sort the loads based on the load id since multiple threads are handling addition of slices in rsStore instance.
                tableNames.add(tableName);
            }
            if(flagList.size()>0)
            {
                slices.add(rsStore);
                sortSlicesBasedOnLoadName(slices,tableNames);
            }
            
           
           
        }
        
       
        return slices;
    }

    private void sortSlicesBasedOnLoadName(List<RestructureStore> slices, Set<String> tableNames)
    {
        for(RestructureStore slice : slices)
        {
            for(String tableName : tableNames)
            {
                List<InMemoryCube> inMemCube = slice.getSlices(tableName);
                if(null != inMemCube)
                {
                    Collections.sort(inMemCube);
                }
            }

        }

    }

    private boolean loadMemberCacheForEachLoad(MolapDef.Cube rolapCube, MolapDef.Schema schema,
            RestructureStore rsStore, String tableName, MolapFile loadFolder, Cube metadataCube)
    {
        boolean updateReq;
        InMemoryCube cubeCache = new InMemoryCube(schema, rolapCube, metadataCube);
        cubeCache.setLoadName(loadFolder.getName());
        cubeCache.setRsStore(rsStore);
        cubeCache.loadCacheFromFile(loadFolder.getAbsolutePath(), tableName, true);
        rsStore.setSlice(cubeCache, tableName);
        updateReq = true;
        return updateReq;
    }

    private boolean isLoadFolderAlreadyPresent(List<RestructureStore> slices, List<String> sliceUpdatedLoadPaths, MolapFile rsFolder, String tableName,String loadName)
    {

        if(null != slices)
        {
            for(RestructureStore resFolderExisting : slices)
            {
                List<InMemoryCube> listOfCubes = resFolderExisting.getSlices(tableName);
                if(null != listOfCubes)
                {

                    Iterator<InMemoryCube> itr = listOfCubes.iterator();
                    while(itr.hasNext())
                    {
                        InMemoryCube inMemCube = itr.next();
                        if(inMemCube.getLoadName().equals(loadName)
                                && !sliceUpdatedLoadPaths.contains(inMemCube.getLoadName()))
                        {
                            return true;
                        }
                    }

                }
            }
        }
        return false;
    }

    /**
     * @param path
     * @param folderStsWith
     * @return
     */
    private MolapFile[] getSortedFolderListList(String path,final String folderStsWith, final int currentRestructNumber, final boolean isRSFolder)
    {
        MolapFile file = FileFactory.getMolapFile(path,FileFactory.getFileType(path));
        MolapFile[] files = null;
        if(file.isDirectory())
        {
            files = file.listFiles(new MolapFileFilter()
            {
                @Override
                public boolean accept(MolapFile pathname)
                {
                    String name = pathname.getName();
                    if(pathname.isDirectory() && name.startsWith(folderStsWith)
                            && !(name.indexOf(MolapCommonConstants.FILE_INPROGRESS_STATUS) > -1))
                    {
                        if (isRSFolder)
                        {
                            String[] splits = name.split(folderStsWith);
                            if(2 == splits.length)
                            {
                                try
                                {
                                    if(Integer.parseInt(splits[1]) <= currentRestructNumber || -1 == currentRestructNumber)
                                    {
                                        return true;
                                    }
                                }
                                catch(NumberFormatException e)
                                {
                                    return false;
                                }
                            }
                        }
                        return true;
                    }
                    return false;
                }
            });
            Arrays.sort(files, new Comparator<MolapFile>()
            {
                /**
                 * @param o1
                 * @param o2
                 * @return
                 */
                public int compare(MolapFile o1, MolapFile o2)
                {
                    try
                    {
                        //
                        int firstFolderIndex = o1.getAbsolutePath().lastIndexOf("/");
                        if(firstFolderIndex == -1)
                        {
                            firstFolderIndex = o1.getAbsolutePath().lastIndexOf("\\");
                        }
                        int secondFolderIndex = o2.getAbsolutePath().lastIndexOf("/");
                        if(secondFolderIndex == -1)
                        {
                            secondFolderIndex = o2.getAbsolutePath().lastIndexOf("\\");
                        }
                        //
                        String firstFolder = o1.getAbsolutePath().substring(firstFolderIndex);
                        String secondFolder = o2.getAbsolutePath().substring(secondFolderIndex);
                        //
                        int f1 = -1;
                        int f2 = -1;
                        try
                        {
                         f1 = Integer.parseInt(firstFolder.split("_")[1]);
                        }
                        catch(NumberFormatException e)
                        {
                            String loadName = (firstFolder.split("_")[1]);
                          f1 =  Integer.parseInt(loadName.substring(0, loadName.indexOf(MolapCommonConstants.MERGERD_EXTENSION)));
                            
                        }
                        try
                        {
                         f2 = Integer.parseInt(secondFolder.split("_")[1]);
                        }
                        catch(NumberFormatException e)
                        {
                            String loadName = (secondFolder.split("_")[1]);
                          f2 =  Integer.parseInt(loadName.substring(0, loadName.indexOf(MolapCommonConstants.MERGERD_EXTENSION)));
                            
                        }
//                        int f2 = Integer.parseInt(secondFolder.split("_")[1]);
                        return (f1 < f2) ? -1 : (f1 == f2 ? 0 : 1);
                    }
                    catch(Exception e)
                    {
                        LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
                        return o1.getName().compareTo(o2.getName());
                    }
                }
            });
        }
        return files;
    }

    /**
     * Add the slice to cube.
     * 
     * @param cubeName
     * @param deltaCube
     */
    public void registerSlice(InMemoryCube deltaCube,RestructureStore rsStore)
    {
        String cubeUniqueName = deltaCube.getCubeUniqueName();
        LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Adding new slice " 
                    + deltaCube.getID() + "For cube " + cubeUniqueName);
        if(null==cubeSliceMap.get(cubeUniqueName))
        {
            List<RestructureStore> inMemoryCube = new ArrayList<RestructureStore>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            deltaCube.setRsStore(rsStore);
            inMemoryCube.add(rsStore);
            cubeSliceMap.put(cubeUniqueName, inMemoryCube);
        }
        else
        {
            cubeSliceMap.get(cubeUniqueName).add(rsStore);
        }
    }
    
    /**
     * @param cubeName
     * @param rsFolder
     * @return
     */
    public RestructureStore findRestructureStore(String cubeUniqueName,String rsFolder)
    {
        List<RestructureStore> rsStores = cubeSliceMap.get(cubeUniqueName);
        
        if(rsStores == null)
        {
            return null;
        }
        
        for(RestructureStore rsStore : rsStores)
        {
            if(rsStore.getFolderName().equals(rsFolder))
            {
                return rsStore;
            }
        }
        return null;
    }

    /**
     * Add the slice to cube.
     * 
     * @param cubeName
     * @param deltaCube
     */
    public void unRegisterSlice(String cubeUniqueName, InMemoryCube deltaCube)
    {
        if(cubeUniqueName != null && deltaCube != null)
        {
            cubeSliceMap.get(cubeUniqueName).remove(deltaCube);
            LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Removed slice " 
                        + deltaCube.getID() + "For cube " + cubeUniqueName);
        }
    }

    /**
     * Gives the slices available for the cube
     * 
     * @param cubeName
     * @return
     */
    public List<InMemoryCube> getActiveSlices(String cubeUniqueName)
    {
            List<InMemoryCube> slices = new ArrayList<InMemoryCube>(MolapCommonConstants.CONSTANT_SIZE_TEN);

            if(cubeSliceMap.get(cubeUniqueName) == null)
            {
                return new ArrayList<InMemoryCube>(10);
            }

            for(RestructureStore rsStore : cubeSliceMap.get(cubeUniqueName))
            {
                rsStore.getActiveSlices(slices);
            }
            return slices;
    }

    /**
     * Gives the slices available for the cube.
     * 
     * @param cubeName
     * @return
     */
    public synchronized List<Long> getActiveSliceIds(String cubeUniqueName)
    {
        List<Long> slices = new ArrayList<Long>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        for(RestructureStore rsStore : cubeSliceMap.get(cubeUniqueName))
        {
            rsStore.getActiveSliceIds(slices);
        }
        return slices;
    }

    /**
     * Give the Slice references for given list of id values.
     * 
     * @param cubeName
     * @param ids
     * @return
     */
    public List<InMemoryCube> getSllicesbyIds(String cubeUniqueName, List<Long> ids)
    {
        List<InMemoryCube> slices = new ArrayList<InMemoryCube>(MolapCommonConstants.CONSTANT_SIZE_TEN);

        for(RestructureStore rsStore : cubeSliceMap.get(cubeUniqueName))
        {
            rsStore.getSlicesByIds(ids, slices);
        }
        return slices;
    }
    
    /**
     * add listener to end executing queries and clean the cube.
     * 
     * @param cubeName
     * @return
     * @author Sojer z00218041
     */
    public void clearExecutingQueryAndCube(InMemoryCube inMemoryCube, List<InMemoryCube> toRemove)
    {
        List<Long> queries = QueryMapper.getQueriesPerSlice(inMemoryCube);
        if(queries.size() > 0)
        {
            SliceListener listener = new SliceListener(inMemoryCube);
            for(Long query : queries)
            {
                listener.registerQuery(query);
                QueryMapper.registerSliceListener(listener, query);
            }
        }
        else
        {
            toRemove.add(inMemoryCube);
            // TODO if it is sync,we can use flow code
            /*
             * //slice is ready to clean
             * InMemoryCubeStore.getInstance().unRegisterSlice
             * (inMemoryCube.getCubeName(), inMemoryCube); inMemoryCube.clean();
             * afterClearAllQueriesAndCubes();
             */
        }

        // Mark the slices inactive
        inMemoryCube.setCubeMerged();
    }

    /**
     * run this method to do data switch after Restructuring It will add
     * listeners to end executing queries and clean the cubes.
     * 
     * @author Sojer z00218041 2012-7-27
     */
    public void clearQueriesAndSlices(String cubeUniqueName)
    {
//        List<RestructureStore> sliceList = cubeSliceMap.get(cubeName);
//        if(sliceList != null && sliceList.size() > 0)
//        {
//            List<InMemoryCube> toRemove = new ArrayList<InMemoryCube>();
//            SLICE_LIST_CONCURRENT = true;
//            for(InMemoryCube inMemoryCube : sliceList)
//            {
//                clearExecutingQueryAndCube(inMemoryCube, toRemove);
//            }
//
//            // remove the slices without query directly
//            for(InMemoryCube imcube : toRemove)
//            {
//                imcube.clean();
//            }
//            sliceList.removeAll(toRemove);
//
//            SLICE_LIST_CONCURRENT = false;
//
//            afterClearQueriesAndCubes(cubeName);
//        }

    }

    /**
     * if cleanQueriesAndCubes finished, change the QUERY_EXECUTE_STATUS and
     * reload or flush cache now called by clearQueriesAndCubes(cubeName) and
     * QueryMapper.invokeListeners(Long,cubeName)
     * 
     * @author Sojer z00218041 2012-7-26
     */
    public void afterClearQueriesAndCubes(String cubeUniqueName)
    {
        if(getQueryExecuteStatus(cubeUniqueName) == QUERY_WAITING)
        {
            if(isAllSlicesCleared(cubeUniqueName))
            {
                clearCache(cubeUniqueName);
                // flush schema model in RolapSchema.Pool
                MolapDef.Schema rolapSchema = mapCubeToSchema.get(cubeUniqueName);
                if(rolapSchema != null)
                {
//                    new CacheControlImpl().flushSchema(rolapSchema);
                    mapCubeToSchema.remove(cubeUniqueName);
                    // re create connection (load in-memory)
//                    recreateConnAfterFlushSchema(rolapSchema);
                }

                // receive new query
                setQueryExecuteStatus(cubeUniqueName, QUERY_AVAILABLE);
            }

        }
        else if(getQueryExecuteStatus(cubeUniqueName) == QUERY_BLOCK)
        {
            if(isAllSlicesCleared(cubeUniqueName))
            {
                // TODO if there are several cubes in the same schema, it is
                // better to wait all cubes done

                // wait for reload
                setQueryExecuteStatus(cubeUniqueName, QUERY_FINISHED_FOR_RELOAD);
            }
        }
    }

    /**
     * judge if all slices for cubeName have been cleared
     * 
     * @param cubeName
     * @return boolean
     * @author Sojer z00218041 2012-8-9
     */
    private boolean isAllSlicesCleared(String cubeName)
    {
        List<RestructureStore> sliceList = cubeSliceMap.get(cubeName);
        /*if(sliceList != null && sliceList.size() > 0)
        {
            return false;
        }
        else
        {
            return true;
        }*/
        
        return (sliceList != null && sliceList.size() > 0);
    }

    /**
     * judge if the query can be executed or waiting While switch for
     * Restructuring of data is in progess, the query will be wait
     * 
     * @return
     * @author Sojer z00218041 2012-7-31
     */
    public boolean isQueryWaiting(String cubeUniqueName)
    {
        return getQueryExecuteStatus(cubeUniqueName) == InMemoryCubeStore.QUERY_WAITING;
    }

    /**
     * judge if the query can be executed or blocked. While Restructuring of
     * schema is in progess, the query will be blocked
     * 
     * @author Sojer z00218041 2012-7-26
     */
    public boolean isQueryBlock(String cubeUniqueName)
    {
        return getQueryExecuteStatus(cubeUniqueName) == QUERY_BLOCK
                || getQueryExecuteStatus(cubeUniqueName) == QUERY_FINISHED_FOR_RELOAD;
    }

    /**
     * judge if the clean task of switch for Restructuring of schema is done,
     * then ready for ETL publish schema XML file
     * 
     * @author Sojer z00218041 2012-7-26
     */
//    public boolean isReadyToReloadSchema(List<String> cubeUniqueNames)
//    {
//        for(String cubeUniqueName : cubeUniqueNames)
//        {
//            if(getQueryExecuteStatus(cubeUniqueName) != QUERY_FINISHED_FOR_RELOAD
//                    && getQueryExecuteStatus(cubeUniqueName) != QUERY_BLOCK)
//            {
//                return false;
//            }
//        }
//        return true;
//    }

    /**
     * ETL inform mondrian the schema XML file has published then flush Schema
     * in RolapSchema.Pool And Reload it, in order to reload the in-memory cache
     * that has been clear
     * 
     * @author Sojer z00218041 2012-7-26
     */
    public void informSchemaPublished(String schemaName)
    {
        Set<Entry<String, MolapDef.Schema>> entrySet = mapCubeToSchema.entrySet();
        boolean hasReCreate = false;
        for(Iterator<Entry<String,  MolapDef.Schema>> iter = entrySet.iterator();iter.hasNext();)
        {
            Entry<String,  MolapDef.Schema> entry = iter.next();

            if(entry.getValue().getName().equals(schemaName))
            {
                // flush schema model in RolapSchema.Pool
//                new CacheControlImpl().flushSchema(entry.getValue());
                // remove entry
                iter.remove();
                // re create connection (load in-memory).Because there are 1
                // more cubes map to the schema, so use a flag to control only
                // re create one time
                if(!hasReCreate)
                {
//                    recreateConnAfterFlushSchema(entry.getValue());
                    hasReCreate = true;
                }
                // break;
            }
        }
    }

//    /**
//     * create a new MolapConnection here ,so the users not use to wait for the
//     * loading of in-memory when they query
//     * 
//     * @author Sojer z00218041 2012-9-14
//     */
//    private void recreateConnAfterFlushSchema(RolapSchema rolapSchema)
//    {
//        rolapSchema.getInternalConnection().getCacheControl(null).flushSchema(rolapSchema);
//        MolapMetadata.getInstance().removeCube(rolapSchema.getName());
//        RolapConnection rc = rolapSchema.getInternalConnection();
//        DriverManager.getConnection(rc.getConnectInfo(), null, rc.getDataSource());
//    }

    /**
     * get waiting type of cube. if not exist,then init as QUERY_AVAILABLE
     * 
     * @param cubeName
     * @return
     * @author Sojer z00218041 2012-8-7
     */
    public byte getQueryExecuteStatus(String cubeUniqueName)
    {
        if(!queryExecuteStatusMap.containsKey(cubeUniqueName))
        {
            setQueryExecuteStatus(cubeUniqueName, QUERY_AVAILABLE);
        }
        return queryExecuteStatusMap.get(cubeUniqueName);
    }

    /**
     * while Restructuring of data and reload cache to Mondrian, set query on
     * waiting set QUERY_EXECUTE_STATUS to specified cube
     * 
     * @author Sojer z00218041 2012-7-27
     */
    public void setQueryExecuteStatus(String cubeUniqueName, byte queryExecuteStatus) 
    {
        queryExecuteStatusMap.put(cubeUniqueName, queryExecuteStatus);
    }

    /**
     * get the status if sliceList is in iterating
     * 
     * @return
     * @author Sojer z00218041 2012-8-7
     */
    public boolean isSliceListConcurrent()
    {
        return SLICE_LIST_CONCURRENT;
    }

    /**
     * switch After Restructure of Schema, make the query block and then clean
     * queries and InMemory cache,then flush the RolapSchema corresponding the
     * cube
     * 
     * @author Sojer z00218041 2012-7-27
     */
//    public void switchAfterRestructureSchema(List<String> cubeUniqueNames)
//    {
//        for(String cubeUniqueName : cubeUniqueNames)
//        {
//            setQueryExecuteStatus(cubeUniqueName, QUERY_BLOCK);
//            clearQueriesAndSlices(cubeUniqueName);
//        }
//    }

    /**
     * switch After Restructure of Data, make the query wait and then clean
     * queries and InMemory cache, then reload to InMemory cache.
     * 
     * @author Sojer z00218041 2012-7-27
     */
    public void switchAfterRestructureData(List<String> cubeUniqueNames)
    {
        for(String cubeUniqueName : cubeUniqueNames)
        {
            setQueryExecuteStatus(cubeUniqueName, QUERY_WAITING);
            clearQueriesAndSlices(cubeUniqueName);
        }
    }

    /**
     * Get all the name of cubes in Memory.
     * 
     * @return added by liupeng 00204190
     */
    public String[] getCubeNames()
    {
        String[] s = new String[0];
        String[] cubeNames = cubeSliceMap.keySet().toArray(s);
        return cubeNames;
    }

    private void addTableRestructuringNumber(String tableName,int currentRSNumber)
    {
        Integer rsNumber = tableAndCurrentRSMap.get(tableName);
        if(null == rsNumber)
        {
            tableAndCurrentRSMap.put(tableName,currentRSNumber);
        }
        else
        {
            if (rsNumber < currentRSNumber)
            {
                tableAndCurrentRSMap.put(tableName,currentRSNumber);
            }
        }
    }

    public int getTableRSNumber(String tableName)
    {
        Integer rsNumber=tableAndCurrentRSMap.get(tableName);
        if(null==rsNumber)
        {
            return -1;
        }
        return rsNumber;
    }
}
