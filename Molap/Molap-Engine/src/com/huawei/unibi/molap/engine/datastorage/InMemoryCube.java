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

package com.huawei.unibi.molap.engine.datastorage;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFileFilter;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.engine.datastorage.cache.LevelInfo;
import com.huawei.unibi.molap.engine.datastorage.cache.MolapLRULevelCache;
import com.huawei.unibi.molap.engine.util.CacheUtil;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.keygenerator.factory.KeyGeneratorFactory;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.metadata.MolapSchemaReader;
import com.huawei.unibi.molap.olap.MolapDef;
import com.huawei.unibi.molap.util.MolapUtil;
import com.huawei.unibi.molap.util.MolapUtilException;
import com.huawei.unibi.molap.vo.HybridStoreModel;

public class InMemoryCube implements Comparable<InMemoryCube>
{
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
    private Map<String, CubeDataStore> dataCacheMap = new HashMap<String, CubeDataStore>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

    /**
     * All dimensions and its cache
     */
    private Map<String, DimensionHierarichyStore> dimesionCache = new HashMap<String, DimensionHierarichyStore>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

    /**
     * Can hold members, each level
     * 
     * column name and the cache
     */
    private Map<String, MemberStore> membersCache = new HashMap<String, MemberStore>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

    /**
     * rolap cube
     */
    private MolapDef.Cube rolapCube;

    private MolapDef.Schema schema;

    /**
     * 
     */
    private RestructureStore rsStore;

    /**
     * 
     */
    private CubeSlicePathInfo cubeSlicePathInfo;

    /**
     * All the cubes loaded from file or Slice are slices. Only cube loaded
     * initially from persistent store is main cube as of now.
     */
    private boolean isSlice = true;

    /**
     * 
     *//*
    private static long counter = 0;*/
    
    private static AtomicLong counter = new AtomicLong(0);

    /**
     * 
     */
    private long id;

    /**
     * 
     */
    private byte cubeStatus;

    /**
     * 
     */
    private static final byte ACTIVE = 0;

    /**
     * 
     */
    private static final  byte READY_TO_CLEAN = 1;

    /**
     * factTableName
     */
    private String factTableName;

    private String loadName;
    
    private int[] dimensionCardinality;
    
    private KeyGenerator keyGenerator;
    
    private String tableName;
    
    private MolapLRULevelCache levelCache;
    
    private Cube metaCube;

    private HybridStoreModel hybridStoreModel;   
    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(InMemoryCube.class.getName());

    public InMemoryCube(MolapDef.Schema schema, MolapDef.Cube cube, Cube metaCube)
    {
        this.cubeName = cube.name;
        this.rolapCube = cube;
        this.schema = schema;
        this.metaCube = metaCube;
        this.schemaName = schema.name;
        this.cubeUniqueName = this.schemaName + '_' + this.cubeName;
        this.id = counter.incrementAndGet();
        this.factTableName = MolapSchemaReader.getFactTableName(cube);
        this.levelCache = MolapLRULevelCache.getInstance();
    }

    /**
     * RolapCube
     */
    public MolapDef.Cube getRolapCube()
    {
        return rolapCube;
    }

    /**
     * RolapCube
     */
    public void setRolapCube(MolapDef.Cube rolapCube)
    {
        this.rolapCube = rolapCube;
    }

    /**
     * @return
     */
    public boolean isSlice()
    {
        return isSlice;
    }

    /**
     * @return
     */
    public CubeSlicePathInfo getCubeSlicePathInfo()
    {
        return cubeSlicePathInfo;
    }

    /**
     * Getter for data cache reference. CubeDataCache provide the details
     * access/process methods for fact table/aggregate tables.
     */
    public CubeDataStore getDataCache(String tableName)
    {
        return dataCacheMap.get(tableName);
    }

    /**
     * @return the cubeName
     */
    public String getCubeName()
    {
        return cubeName;
    }

    /**
     * @param dimension
     * @return
     */
    public DimensionHierarichyStore getDimensionAndHierarchyCache(String dimension)
    {
        return dimesionCache.get(dimension);
    }

    /**
     * getStartKey
     * 
     * @param tableName
     * @return byte[]
     */
    public byte[] getStartKey(String tableName)
    {
        return dataCacheMap.get(tableName).getStartKey();
    }

    /**
     * getKeyGenerator
     * 
     * @param tableName
     * @return KeyGenerator
     */
    public KeyGenerator getKeyGenerator(String tableName)
    {
        return keyGenerator;
    }

    /**
     * Load the cube cache from a file storage.
     * 
     * @param filesLocaton
     */
    public void loadCacheFromFile(String fileStore, String tableName, boolean loadOnlyLevelFiles)
    {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        String cubeUniqueName = schemaName + '_' + cubeName;
        MolapFile file = FileFactory.getMolapFile(fileStore, FileFactory.getFileType(fileStore));
        if(file.isDirectory())
        {
            getDimensionCardinality(file, tableName);
            List<Dimension> dimensions=metaCube.getDimensions(tableName);
            
            boolean[] dimensionStoreType=new boolean[dimensionCardinality.length];
            List<Integer> highCardDimOrdinals=new ArrayList<Integer>();
            for(Dimension dimension:dimensions)
            {
                if(dimension.isHighCardinalityDim())
                {
                    highCardDimOrdinals.add(dimension.getOrdinal());
                    continue;
                }
                if(dimension.isColumnar())
                {
                    dimensionStoreType[dimension.getOrdinal()]=dimension.isColumnar();    
                }
            }
            hybridStoreModel= MolapUtil.getHybridStoreMeta(dimensionCardinality, dimensionStoreType,highCardDimOrdinals);
            //TO -DO  : need finalise on keygenerator
            keyGenerator = KeyGeneratorFactory.getKeyGenerator(hybridStoreModel.getHybridCardinality(),hybridStoreModel.getDimensionPartitioner());
//            keyGenerator = KeyGeneratorFactory.getKeyGenerator(dimensionCardinality);
            keyGenerator = KeyGeneratorFactory.getKeyGenerator(findRequiredDimensionForStartAndEndKey());
            int startAndEndKeySizeWithPrimitives = KeyGeneratorFactory.getKeyGenerator(findRequiredDimensionForStartAndEndKey()).getKeySizeInBytes();
            keyGenerator.setStartAndEndKeySizeWithOnlyPrimitives(startAndEndKeySizeWithPrimitives);
        }
        // Process fact and aggregate data cache
        if(!loadOnlyLevelFiles)
        {
            for(String table : metaCube.getTablesList())
            {
                if(!table.equals(tableName))
                {
                    continue;
                }
                CubeDataStore dataCache = new CubeDataStore(table, metaCube, rsStore.getSliceMetaCache(table),keyGenerator, dimensionCardinality,hybridStoreModel);
                //add start and end key size with only primitives
                if(dataCache.loadDataFromFile(fileStore, keyGenerator.getStartAndEndKeySizeWithOnlyPrimitives()))
                {
                    dataCacheMap.put(table, dataCache);
                }
            }
        }
        String loadFolderName = fileStore.substring(fileStore.lastIndexOf(MolapCommonConstants.LOAD_FOLDER));
        MolapDef.CubeDimension dimension = null;
        // Process dimension and hierarchies cache
        for(int i = 0;i < rolapCube.dimensions.length;i++)
        {
            dimension = rolapCube.dimensions[i];
            if(dimension.visible && !dimension.highCardinality)
            {
                DimensionHierarichyStore cache = new DimensionHierarichyStore(dimension, membersCache, cubeUniqueName,
                        factTableName, schema);
                String levelActualName = CacheUtil.getLevelActualName(schema, dimension);
                String fileName = fileStore + File.separator + factTableName + '_' + levelActualName
                        + MolapCommonConstants.LEVEL_FILE_EXTENSION;
                if(InMemoryCubeStore.getInstance().isLevelCacheEnabled() && tableName.equals(factTableName)
                        && CacheUtil.isFileExists(fileName))
                {
                    long memberFileSize = CacheUtil.getMemberFileSize(fileName);
                    LevelInfo levelInfo = new LevelInfo(memberFileSize, dimension.name, levelActualName, factTableName,
                            fileStore, loadFolderName);
                    levelInfo.setLoaded(false);
                    levelCache.put(cubeUniqueName + '_' + loadFolderName + '_' + levelActualName, levelInfo);
                }
                else
                {
                    cache.processCacheFromFileStore(fileStore, executorService);
                }
                dimesionCache.put(dimension.name, cache);
            }
        }
        cubeSlicePathInfo = new CubeSlicePathInfo(fileStore);

        try
        {
            executorService.shutdown();
            executorService.awaitTermination(2, TimeUnit.DAYS);
        }
        catch(InterruptedException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
        }
        this.tableName=tableName;
    }
    
    
    private int[] findRequiredDimensionForStartAndEndKey()
    {
        List<Integer> dimCardinalities = new ArrayList<Integer>();
        for(int dimCard : dimensionCardinality)
        {
            if(dimCard != 0)
                dimCardinalities.add(dimCard);
            else
                break;
        }
        int[] primitiveDimsForKey = new int[dimCardinalities.size()];
        for(int i=0;i<dimCardinalities.size();i++)
        {
            primitiveDimsForKey[i] = dimCardinalities.get(i);
        }
        return primitiveDimsForKey;
    }
    /**
     * @param file
     */
    private int[] getDimensionCardinality(MolapFile file, final String tableName)
    {
        dimensionCardinality =new int[0]; 
        MolapFile[] files = file.listFiles(new MolapFileFilter()
        {
            public boolean accept(MolapFile pathname)
            {
                return (!pathname.isDirectory()) && pathname.getName().startsWith(MolapCommonConstants.LEVEL_METADATA_FILE)
                        && pathname.getName().endsWith(tableName+".metadata");
            }

        });
        
        
        if(files.length <= 0)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Level Cardinality file not found in path : " + file.getAbsolutePath());
            return dimensionCardinality;
        }
        
        try
        {
            dimensionCardinality=MolapUtil.getCardinalityFromLevelMetadataFile(files[0].getAbsolutePath());
        }
        catch(MolapUtilException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
        }
        return dimensionCardinality;
    }

    /**
     * Access the dimension members through levelName
     */
    public MemberStore getMemberCache(String levelName)
    {
        return membersCache.get(levelName);
    }

    /**
     * Returns
     */
    public long getID()
    {
        return id;
    }

    /**
     * Gives the current cube status is active or not.
     */
    public boolean isActive()
    {
        if(cubeStatus == ACTIVE)
        {
            return true;
        }
        return false;
    }

    /**
     * Marks this slice dirty so that it can be cleaned once all the dependent
     * queries are finished their execution.
     */
    public void setCubeMerged()
    {
        cubeStatus = READY_TO_CLEAN;
    }

    /**
     * Try clearing the resources
     */
    public void clean()
    {
        dimesionCache.clear();
        membersCache.clear();
        for(CubeDataStore store : dataCacheMap.values())
        {
            store.clear();
        }
        dataCacheMap.clear();
    }

    /**
     * @return
     */
    public long getSize()
    {
        // TODO current data size is only fact table size. Need to consider
        // hierarchies and dimensions also.
        return dataCacheMap.get(this.factTableName).getSize();
    }

    /**
     * @return the rsStore
     */
    public RestructureStore getRsStore()
    {
        return rsStore;
    }

    /**
     * @param rsStore
     *            the rsStore to set
     */
    public void setRsStore(RestructureStore rsStore)
    {
        this.rsStore = rsStore;
    }

    /**
     * getSchemaName
     * 
     * @return String
     */
    public String getSchemaName()
    {
        return schemaName;
    }

    /**
     * getCubeUniqueName
     * 
     * @return String
     */
    public String getCubeUniqueName()
    {
        return cubeUniqueName;
    }

    /**
     * 
     * @return Returns the factTableName.
     * 
     */
    public String getFactTableName()
    {
        return factTableName;
    }

    /**
     * 
     * @param factTableName
     *            The factTableName to set.
     * 
     */
    public void setFactTableName(String factTableName)
    {
        this.factTableName = factTableName;
    }

    public MolapDef.Schema getSchema()
    {
        return schema;
    }

    public void setSchema(MolapDef.Schema schema)
    {
        this.schema = schema;
    }

    public void setLoadName(String loadName)
    {
        this.loadName = loadName;

    }

    public String getLoadName()
    {
        return loadName;
    }

    /**
     * 
     * @return Returns the tableName.
     * 
     */
    public String getTableName()
    {
        return tableName;
    }
    
    public int getLoadId()
    {
        int lastIndexOf = loadName.lastIndexOf(MolapCommonConstants.LOAD_FOLDER);
        int loadNumber = -1;
        try
        {
            if(isLoadMerged(loadName))
            {
                String loadNum = loadName.substring(loadName.indexOf('_') + 1,
                        loadName.indexOf(MolapCommonConstants.MERGERD_EXTENSION));
                loadNumber = Integer.parseInt(loadNum);
            }
            else
            {
                loadNumber = Integer.parseInt(loadName.substring(lastIndexOf + 5));
            }
        }
        catch(NumberFormatException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e, "Problem while getting the load number");
        }
        return loadNumber;
    }



    @Override
    public int compareTo(InMemoryCube memCubeInstance)
    {
        String loadNameOfCurrntObj = this.getLoadName();
        String loadNameCurrntObj;
        
        if(isLoadMerged(loadNameOfCurrntObj))
        {
             loadNameCurrntObj = loadNameOfCurrntObj.substring(loadNameOfCurrntObj.indexOf('_')+1,loadNameOfCurrntObj.indexOf(MolapCommonConstants.MERGERD_EXTENSION));
        }
        else
        {
         loadNameCurrntObj = loadNameOfCurrntObj.substring(loadNameOfCurrntObj.lastIndexOf('_')+1,
                loadNameOfCurrntObj.length());
        }
        int idOfCurrentObj = Integer.parseInt(loadNameCurrntObj);
        String loadNameOfComparableObj = memCubeInstance.getLoadName();
        
        if(isLoadMerged(loadNameOfComparableObj))
        {
            loadNameOfComparableObj = loadNameOfComparableObj.substring(loadNameOfComparableObj.indexOf('_')+1,loadNameOfComparableObj.indexOf(MolapCommonConstants.MERGERD_EXTENSION));
        }
        else
        {
        loadNameOfComparableObj = loadNameOfComparableObj.substring(loadNameOfComparableObj.lastIndexOf('_')+1,
                loadNameOfComparableObj.length());
        }
        int idOfCompObj = Integer.parseInt(loadNameOfComparableObj);
        return idOfCurrentObj - idOfCompObj;
    }
    
    @Override
    public boolean equals(Object obj) 
    {
        if(obj == this)
        {
            return true;
        }
        if(!(obj instanceof InMemoryCube))
        {
            return false;
        }
        InMemoryCube memCubeInstance=(InMemoryCube)obj;
        return compareTo(memCubeInstance) == 0; 
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((loadName == null) ? 0 : loadName.hashCode());
        result = prime * result
                + ((loadName == null) ? 0 : loadName.hashCode());
        return result;
    }

    /**
     * 
     * @return Returns the dimensionCardinality.
     * 
     */
    public int[] getDimensionCardinality()
    {
        return dimensionCardinality;
    }
    
    /**
     * To check if this is a merged load or not.
     * @param loadName
     * @return
     */
    private boolean isLoadMerged(String loadName)
    {
         if(loadName.contains(MolapCommonConstants.MERGERD_EXTENSION))
         {
             return true;
         }
        return false;
    }

    public HybridStoreModel getHybridStoreModel()
    {
        return this.hybridStoreModel;
    }
}
