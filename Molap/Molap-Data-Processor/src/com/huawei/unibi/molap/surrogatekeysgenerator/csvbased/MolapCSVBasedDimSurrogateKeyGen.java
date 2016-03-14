/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcfJtSMNYgnOYiEQwbS13nxM8hk/dmbY4B4u+tG
aRAl/qLgLF01nUH8aF6Rax4CHrvEIhCMdT1KiUPPMd1YNMR4ur6tRwA0vI5xyzYgYsCUv5nO
Flac5demsxQf6t9Ss3d1mmPcULenmxSejK1Bo3vtdHiLlYovoQYDeJPQy6TSbQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
*/

package com.huawei.unibi.molap.surrogatekeysgenerator.csvbased;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.File;
import java.io.OutputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.pentaho.di.core.exception.KettleException;

import com.huawei.datasight.molap.datatypes.GenericDataType;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.file.manager.composite.FileData;
import com.huawei.unibi.molap.file.manager.composite.IFileManagerComposite;
import com.huawei.unibi.molap.keygenerator.KeyGenException;
import com.huawei.unibi.molap.schema.metadata.ArrayWrapper;
import com.huawei.unibi.molap.schema.metadata.MolapInfo;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapUtil;
import com.huawei.unibi.molap.writer.HierarchyValueWriterForCSV;
import com.huawei.unibi.molap.writer.LevelValueWriter;



/**
 * @author R00900208
 * 
 */
public abstract class MolapCSVBasedDimSurrogateKeyGen
{
    
    private static final LogService LOGGER = LogServiceFactory.getLogService(MolapCSVBasedDimSurrogateKeyGen.class.getName());
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
     * max
     */
    protected int[] max;
    
    /**
     * timeDimMax
     */
    private int[] timDimMax;

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
     * hierCache
     */
    private Map<String, Int2ObjectMap<int[]>> hierCache = new HashMap<String, Int2ObjectMap<int[]>>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    
    /**
     * 
     */
    private  Map<String, Map<ArrayWrapper,Integer>> hierCacheReverse = new HashMap<String, Map<ArrayWrapper,Integer>>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

    /**
     * molapInfo
     */
    protected MolapInfo molapInfo;
    
    /**
     * measureValWriter
     */
    protected Map<String , LevelValueWriter> measureValWriterMap;
    
    protected IFileManagerComposite measureFilemanager;

    /**
     * rwLock
     */
//    private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
//    
//    /**
//     * wLock
//     */
//    private Lock wLock = rwLock.writeLock();
//    
//    private Lock readLock = rwLock.readLock();
    
    
//    private ReentrantReadWriteLock rwLockForMember = new ReentrantReadWriteLock();
    
//    private Lock wLockForMember = rwLockForMember.writeLock();
//    
//    private Lock rLockForMember = rwLockForMember.readLock();
    

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
     * primary key max surrogate key map
     */
    protected Map<String ,Integer> primaryKeysMaxSurroagetMap;
    
    /**
     * Measure max surrogate key map
     */
    protected Map<String ,Integer> measureMaxSurroagetMap;
    
    
    /**
     * File manager
     */
    protected IFileManagerComposite fileManager;
    
    /**
     * dimensionWriter
     */
    protected LevelValueWriter[] dimensionWriter;
    
    /**
     * @param molapInfo
     *            MolapInfo With all the required details for surrogate key generation and
     *            hierarchy entries.
     */
    public MolapCSVBasedDimSurrogateKeyGen(MolapInfo molapInfo)
    {
        this.molapInfo = molapInfo;

        //setConnection(molapInfo.getConnectionString());
        setDimensionTables(molapInfo.getDimColNames());
        setHierFileNames(molapInfo.getHierTables());
    }

    /**
     * @param timeTuples
     * @param out
     * @param columnIndex
     * @param timeOrdinalColValues
     * @return
     * @throws KettleException
     */
//    public int[] generateSurrogateKeys(String[] timeTuples, int [] out, int []columnIndex,
//            List<Integer> timeOrdinalColValues) throws KettleException
//    {
//        Integer key = null;
//        for(int i =0;i<columnIndex.length;i++)
//        {
//            Map<String, Integer> cache = memberCache.get(molapInfo.getDimColNames()[columnIndex[i]]);
//
//            key = cache.get(timeTuples[i]);
//            if(key == null)
//            {
//                // Validate the key against cardinality bits
//                if(max[i] >= molapInfo.getMaxKeys()[i])
//                {
//                    throw new KettleException(new KeyGenException(
//                            "Invalid cardinality. Key size exceeded cardinality for: " + molapInfo.getDimColNames()[i]));
//                }
//                // Extract properties from tuple
//                Object[] props = getProperties(new Object[0], timeOrdinalColValues, columnIndex[i]);
//                
//                // Need to create a new surrogate key.
//                key = getSurrogateFromStore(timeTuples[i], columnIndex[i], props);
//                cache.put(timeTuples[i], key);
//            }
//            out[i] = key;
//        }
//        return out;
//    }
    
    /**
     * @param tuples
     * @param columnNames
     * @param index
     * @param props
     * @return
     * @throws KettleException
     */
    public Integer generateSurrogateKeys(String tuples, String columnNames,
            int index, Object[] props) throws KettleException
    {
//      boolean locked = false;
//      if(tuples== null || tuples.length() == 0)
//      {
//          tuples = MolapCommonConstants.MEMBER_DEFAULT_VAL;
//      }
            
        
        Integer key = null;
        Map<String, Integer> cache = memberCache.get(columnNames);

        key = cache.get(tuples);
        if(key == null)
        {
             synchronized (cache) 
             {
                 key = cache.get(tuples);
                 if(null == key)
                 {
                	 // Commented for Dynamic Cardinality requirement - Suprith
//                     if(max[index] >= molapInfo.getMaxKeys()[index])
//                     {
//                         if(MolapCommonConstants.MEMBER_DEFAULT_VAL.equals(tuples))
//                         {
//                             tuples = null;
//                         }
//                         LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Invalid cardinality. Key size exceeded cardinality for: "
//                                         + molapInfo.getDimColNames()[index]+ ": MemberValue: "+tuples) ;
//                         return -1;
//                     }
                     key = getSurrogateFromStore(tuples, index, props);
                     cache.put(tuples, key);
                 }
            }   
            
        }
        return key;
    }
    
    /**
     * 
     * 
     */
    public void closeMeasureLevelValWriter()
    {

        if(null == measureFilemanager || null == measureValWriterMap)
        {
            return;
        }
        int fileMangerSize = measureFilemanager.size();

        for(int i = 0;i < fileMangerSize;i++)
        {
            FileData memberFile = (FileData)measureFilemanager.get(i);
            String msrLvlInProgressFileName = memberFile.getFileName();
            LevelValueWriter measureValueWriter = measureValWriterMap
                    .get(msrLvlInProgressFileName);
            if(null == measureValueWriter)
            {
                continue;
            }

            // now write the byte array in the file.
            OutputStream bufferedOutputStream = measureValueWriter.getBufferedOutputStream();
            if(null == bufferedOutputStream)
            {
                continue;
                
            }
            MolapUtil.closeStreams(bufferedOutputStream);
            measureValueWriter.clearOutputStream();
            String storePath = memberFile.getStorePath();
            String levelFileName = measureValueWriter.getMemberFileName();
            int counter = measureValueWriter.getCounter();
            
            String changedFileName = levelFileName + (counter -1);
            
            String inProgFileName = changedFileName + MolapCommonConstants.FILE_INPROGRESS_STATUS;
            
            File currentFile = new File(storePath + File.separator
                    + inProgFileName);
            File destFile = new File(storePath + File.separator
                    + changedFileName);

            if(!currentFile.exists())
            {
                continue;
            }
            if(currentFile.length() == 0)
            {
               boolean isDeleted= currentFile.delete();
               if(!isDeleted)
               {
            	   LOGGER.debug(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Failed to delete file "+currentFile.getName()); 
               }
            }

            if(!currentFile.renameTo(destFile))
            {
                LOGGER.debug(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Failed to rename from "+currentFile.getName()+" to "+destFile.getName());
            }
        }
        
    }
    
    
    /**
     * @param tuples
     * @param columnName
     * @param index
     * @param props
     * @return
     * @throws KettleException
     */
    public Integer generateSurrogateKeysForTimeDims(String tuples, String columnName,
            int index, Object[] props) throws KettleException
    {
        Integer key = null;
        Map<String, Integer> cache = memberCache.get(columnName);

        key = cache.get(tuples);
        if(key == null)
        {
            if(timDimMax[index] >= molapInfo.getMaxKeys()[index])
            {
                if(MolapCommonConstants.MEMBER_DEFAULT_VAL.equals(tuples))
                {
                    tuples = null;
                }
                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Invalid cardinality. Key size exceeded cardinality for: "
                                + molapInfo.getDimColNames()[index]+ ": MemberValue: "+tuples);
                return -1;
            }
            timDimMax[index]++;
            Map<String, Integer> timeCache = timeDimCache.get(columnName);
            // Extract properties from tuple
            // Need to create a new surrogate key.
            key = getSurrogateFromStore(tuples, index, props);
            cache.put(tuples, key);
            if(null != timeCache)
            {
                timeCache.put(tuples, key);
            }
        }
        else
        {

            return updateSurrogateToStore(tuples,columnName, index,key,props);
        }
        return key;
    }
    

    /**
     * @param tuple
     * @param out
     * @param timeOrdinalColValues
     * @throws KettleException
     */
//    public void generateSurrogateKeys(Object[] tuple, Object[] out, List<Integer> timeOrdinalColValues) throws KettleException
//    {
//      boolean locked = false;
//        try
//        {
//            for (int i = 0; i < molapInfo.getDims().length; i++)
//            {
//                Integer key = null;
//                Object value = tuple[molapInfo.getDims()[i]];
//                if (value == null)
//                {
//                    value = "null";
//                }
//                String dimS = value.toString();
//                // getting values from local cache
//                Map<String, Integer> cache = memberCache.get(molapInfo
//                        .getDimColNames()[i]);
//
//                key = cache.get(dimS);
//                // here we added this null check
//                if (key == null)
//                {
//                    // Validate the key against cardinality bits
//                    // Commenting for testing if this required will be enabled
//                    /*
//                     * if(max[i] >= molapInfo.getMaxKeys()[i]) { throw new
//                     * KettleException(new KeyGenException(
//                     * "Invalid cardinality. Key size exceeded cardinality for: "
//                     * + molapInfo.getDimColNames()[i])); }
//                     */
//                    // Extract properties from tuple
//                  
//                  readLock.lock();
//                    wLock.lock();
//                  locked = true;
//                    // try{
//                    key = cache.get(dimS);
//                    if (null == key)
//                    {
//                        Object[] props = getProperties(tuple,
//                                timeOrdinalColValues, i);
//                        key = getSurrogateFromStore(dimS, i, props);
//                        cache.put(dimS, key);
//                    }
//                    // }finally{
//                    // wLock.unlock();
//                    // }
//                }
//                // Update the generated key in output.
//                out[i] = key;
//            }
//        }
//        finally
//        {
//          if (locked)
//          {
//              wLock.unlock();
//              readLock.unlock();
//          }
//        }
//    }

    /**
     * @param tuple
     * @param timeOrdinalColValues
     * @param i
     * @return
     * 
     */
    /*private Object[] getProperties(Object[] tuple, List<Integer> timeOrdinalColValues, int i)
    {
        Object[] props = new Object[0];
        if(molapInfo.getTimDimIndex() != -1 && i >= molapInfo.getTimDimIndex() && i < molapInfo.getTimDimIndexEnd())
        {
            //For time dimensions only ordinal columns is considered.
            int ordinalIndx = molapInfo.getTimeOrdinalIndices()[i-molapInfo.getTimDimIndexEnd()];
            if(ordinalIndx != -1)
            {
                props =  new Object[1];
                props[0] = timeOrdinalColValues.get(ordinalIndx);
            }
        }
        else
        {
            if(molapInfo.getPropIndx() != null)
            {
                int[] pIndices = molapInfo.getPropIndx()[i];
                props= new Object[pIndices.length];
                for(int j = 0;j < props.length;j++)
                {//CHECKSTYLE:OFF    Approval No:Approval-310
                    props[j] = tuple[pIndices[j]];
                }//CHECKSTYLE:ON
            }
        }
        return props;
    }*/

    /**
     * @param val
     * @param hier
     * @throws KeyGenException
     * @throws KettleException
     */
    public void checkHierExists(int[] val, String hier, int primaryKey)
            throws KettleException
    {
        Int2ObjectMap<int[]> cache = hierCache.get(hier);

        int[] hCache = cache.get(primaryKey);
        if(hCache != null && Arrays.equals(hCache, val))
        {
            return;
        }
        else
        {
            wLock2.lock();
            try
            {
//                 getHierFromStore(val, hier,primaryKey);
                // Store in cache
                cache.put(primaryKey, val);
            }
            finally
            {
                wLock2.unlock();
            }
        }
    }

    /**
     * @param val
     * @param hier
     * @throws KeyGenException
     * @throws KettleException
     */
    public void checkNormalizedHierExists(int[] val, String hier,HierarchyValueWriterForCSV hierWriter)
            throws KettleException
    {
        Map<ArrayWrapper, Integer> cache = hierCacheReverse.get(hier);

        ArrayWrapper wrapper = new ArrayWrapper(val);
        Integer hCache = cache.get(wrapper);
        if(hCache != null)
        {
            return;
        }
        else
        {
            wLock2.lock();
            try
            {
                getNormalizedHierFromStore(val, hier,1,hierWriter);
                // Store in cache
                cache.put(wrapper, 1);
            }
            finally
            {
                wLock2.unlock();
            }
        }
    }

    
    /**
     * @throws Exception
     */
    public void close() throws Exception
    {
        if(null != connection)
        {
            connection.close();
        }
    }
    
    /**
     * @throws KettleException
     * @throws KeyGenException 
     *///CHECKSTYLE:OFF    Approval No:Approval-311
    public abstract void writeHeirDataToFileAndCloseStreams() throws KettleException, KeyGenException;
//CHECKSTYLE:ON
    
    /**
     * @throws KettleException
     * @throws KeyGenException 
     *///CHECKSTYLE:OFF    Approval No:Approval-311
        public abstract void writeDataToFileAndCloseStreams() throws KettleException, KeyGenException;
    //CHECKSTYLE:ON
    /**
     * Search entry and insert if not found in store.
     *  
     * @param val
     * @param hier
     * @return
     * @throws KeyGenException
     * @throws KettleException 
     * 
     */
    protected abstract byte[] getHierFromStore(int[] val, String hier,int primaryKey)
            throws KettleException;
    
    /**
     * Search entry and insert if not found in store.
     *  
     * @param val
     * @param hier
     * @return
     * @throws KeyGenException
     * @throws KettleException 
     * 
     */
    protected abstract byte[] getNormalizedHierFromStore(int[] val, String hier,int primaryKey, HierarchyValueWriterForCSV hierWriter)
            throws KettleException;

    /**
     * Search entry and insert if not found in store.
     * 
     * @param value
     * @param index
     * @param properties - Ordinal column, name column and all other properties
     * 
     * @return
     * @throws KettleException 
     * 
     */
    protected abstract int getSurrogateFromStore(String value, int index, Object[] properties) throws KettleException;
   
    /**
     * Search entry and insert if not found in store.
     * 
     * @param value
     * @param columnName 
     * @param index
     * @param properties - Ordinal column, name column and all other properties
     * 
     * @return
     * @throws KettleException 
     * 
     */
    protected abstract int updateSurrogateToStore(String value, String columnName, int index, int key,Object[] properties) throws KettleException;
   
    
    /**
     * generate the surroagate key for the primary keys.
     * 
     * @return
     * @throws KettleException 
     * 
     */
    public abstract int getSurrogateKeyForPrimaryKey(String value,String fileName,LevelValueWriter levelValueWriter) throws KettleException;
    
    /**
     * generate the surroagate key for the measure values.
     * 
     * @return
     * @throws KettleException 
     * 
     */
    public abstract int getSurrogateForMeasure(String tuple, String columnName,int index) throws KettleException;
    
    
    private Int2ObjectMap<int[]> getHCache(String hName)
    {
        Int2ObjectMap<int[]> hCache = hierCache.get(hName);
        if(hCache == null)
        {
            hCache = new Int2ObjectOpenHashMap<int[]>();
            hierCache.put(hName, hCache);
        }

        return hCache;
    }
    
    private Map<ArrayWrapper, Integer> getHCacheReverse(String hName)
    {
        Map<ArrayWrapper, Integer> hCache = hierCacheReverse.get(hName);
        if(hCache == null)
        {
            hCache = new HashMap<ArrayWrapper, Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
            hierCacheReverse.put(hName, hCache);
        }

        return hCache;
    }
    private void setHierFileNames(Set<String> set)
    {
        hierInsertFileNames = new HashMap<String, String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        for(String s : set)
        {
            hierInsertFileNames.put(s, s + MolapCommonConstants.HIERARCHY_FILE_EXTENSION);
            
            // fix hierStream is null issue
            getHCache(s);
            getHCacheReverse(s);
        }
    }
    
    private void setDimensionTables(String[] dimeFileNames)
    {
//        this.dimsFiles = dimeFileNames;
//        max = new int[dimeFileNames.length];
        int noOfPrimitiveDims = 0;
        List<String> dimFilesForPrimitives = new ArrayList<String>();
        memberCache = new ConcurrentHashMap<String, Map<String, Integer>>();
        for(int i = 0;i < dimeFileNames.length;i++)
        {
        	GenericDataType complexType = molapInfo.getComplexTypesMap().get(dimeFileNames[i].substring(molapInfo.getTableName().length() + 1));
        	if(complexType != null)
        	{
        		List<GenericDataType> primitiveChild = new ArrayList<GenericDataType>();
        		complexType.getAllPrimitiveChildren(primitiveChild);
        		for(GenericDataType eachPrimitive: primitiveChild)
        		{
        			memberCache.put(molapInfo.getTableName()+"_"+eachPrimitive.getName(), new ConcurrentHashMap<String, Integer>());
        			dimFilesForPrimitives.add(molapInfo.getTableName()+"_"+eachPrimitive.getName());
        			eachPrimitive.setSurrogateIndex(noOfPrimitiveDims);
        			noOfPrimitiveDims++;
        		}
        	}
        	else
        	{
        		memberCache.put(dimeFileNames[i], new ConcurrentHashMap<String, Integer>());
        		dimFilesForPrimitives.add(dimeFileNames[i]);
        		noOfPrimitiveDims++;
        	}
        }
        max = new int[noOfPrimitiveDims];
        this.dimsFiles = dimFilesForPrimitives.toArray(new String[dimFilesForPrimitives.size()]);

       // checkDimTableCreated();
        createRespectiveDimFilesForDimTables();
    }

    private void createRespectiveDimFilesForDimTables()
    {
        int dimCount = this.dimsFiles.length;
        dimInsertFileNames = new String[dimCount];
        System.arraycopy(dimsFiles, 0, dimInsertFileNames, 0, dimCount);
        // Checkstyle fix
        /*
         * for(int i=0 ; i < dimCount ; i++) { dimInsertFileNames[i] =
         * dimsFiles[i]; }
         */
    }
    
    /**
     * isCacheFilled
     * @param columnNames
     * @return boolean
     */
    public abstract boolean isCacheFilled(String []columnNames);

    /**
     * 
     * @return Returns the storeFolderWithLoadNumber.
     * 
     */
    public String getStoreFolderWithLoadNumber()
    {
        return storeFolderWithLoadNumber;
    }

    /**
     * 
     * @param storeFolderWithLoadNumber The storeFolderWithLoadNumber to set.
     * 
     */
    public void setStoreFolderWithLoadNumber(String storeFolderWithLoadNumber)
    {
        this.storeFolderWithLoadNumber = storeFolderWithLoadNumber;
    }

    /**
     * 
     * @return Returns the memberCache.
     * 
     */
    public Map<String, Map<String, Integer>> getMemberCache()
    {
        return memberCache;
    }

    /**
     * 
     * @param memberCache The memberCache to set.
     * 
     */
    public void setMemberCache(Map<String, Map<String, Integer>> memberCache)
    {
        this.memberCache = memberCache;
    }

    /**
     * 
     * @return Returns the timeDimCache.
     * 
     */
    public Map<String, Map<String, Integer>> getTimeDimCache()
    {
        return timeDimCache;
    }

    /**
     * 
     * @param timeDimCache The timeDimCache to set.
     * 
     */
    public void setTimeDimCache(Map<String, Map<String, Integer>> timeDimCache)
    {
        this.timeDimCache = timeDimCache;
    }

    /**
     * 
     * @return Returns the dimsFiles.
     * 
     */
    public String[] getDimsFiles()
    {
        return dimsFiles;
    }

    /**
     * 
     * @param dimsFiles The dimsFiles to set.
     * 
     */
    public void setDimsFiles(String[] dimsFiles)
    {
        this.dimsFiles = dimsFiles;
    }
    
    /**
     * 
     * @return Returns the hierCache.
     * 
     */
    public Map<String, Int2ObjectMap<int[]>> getHierCache()
    {
        return hierCache;
    }

    /**
     * 
     * @param hierCache The hierCache to set.
     * 
     */
    public void setHierCache(Map<String, Int2ObjectMap<int[]>> hierCache)
    {
        this.hierCache = hierCache;
    }


    /**
     * 
     * @return Returns the timDimMax.
     * 
     */
    public int[] getTimDimMax()
    {
        return timDimMax;
    }

    /**
     * 
     * @param timDimMax The timDimMax to set.
     * 
     */
    public void setTimDimMax(int[] timDimMax)
    {
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
    public void setHierCacheReverse(
            Map<String, Map<ArrayWrapper, Integer>> hierCacheReverse) {
        this.hierCacheReverse = hierCacheReverse;
    }

    public int[] getMax()
    {
        return max;
    }

    public void setMax(int[] max)
    {
        this.max = max;
    }

    /**
     * @return the measureMaxSurroagetMap
     */
    public Map<String, Integer> getMeasureMaxSurroagetMap()
    {
        return measureMaxSurroagetMap;
    }

    /**
     * @param measureMaxSurroagetMap the measureMaxSurroagetMap to set
     */
    public void setMeasureMaxSurroagetMap(
            Map<String, Integer> measureMaxSurroagetMap)
    {
        this.measureMaxSurroagetMap = measureMaxSurroagetMap;
    }

}
