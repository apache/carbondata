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


package com.huawei.unibi.molap.surrogatekeysgenerator.csvbased;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.pentaho.di.core.exception.KettleException;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.csvreader.checkpoint.CheckPointHanlder;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFileFilter;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.file.manager.composite.FileData;
import com.huawei.unibi.molap.file.manager.composite.IFileManagerComposite;
import com.huawei.unibi.molap.file.manager.composite.LoadFolderData;
import com.huawei.unibi.molap.keygenerator.KeyGenException;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.schema.metadata.ArrayWrapper;
import com.huawei.unibi.molap.schema.metadata.MolapInfo;
import com.huawei.unibi.molap.surrogatekeysgenerator.dbbased.FileStoreSurrogateKeyGen;
import com.huawei.unibi.molap.surrogatekeysgenerator.lru.LRUCache;
import com.huawei.unibi.molap.surrogatekeysgenerator.lru.MolapSeqGenCacheHolder;
import com.huawei.unibi.molap.util.ByteUtil;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapUtil;
import com.huawei.unibi.molap.writer.ByteArrayHolder;
import com.huawei.unibi.molap.writer.HierarchyValueWriterForCSV;
import com.huawei.unibi.molap.writer.LevelValueWriter;

public class FileStoreSurrogateKeyGenForCSV extends MolapCSVBasedDimSurrogateKeyGen
{
    
    /**
     * 
     * Comment for <code>LOGGER</code>
     * 
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(FileStoreSurrogateKeyGen.class.getName());
    
    /**
     * syncObject
     */
    private final Object syncObject =new Object();

    /**
     * hierValueWriter
     */
    private Map<String, HierarchyValueWriterForCSV> hierValueWriter;
    
    /**
     * keyGenerator
     */
    private Map<String, KeyGenerator> keyGenerator;
    
    /**
     * baseStorePath
     */
    private String baseStorePath;
    
    /**
     * LOAD_FOLDER
     */
    private String loadFolderName;
    
    /**
     * folderList
     */
    private List<MolapFile> folderList = new ArrayList<MolapFile>(5);
    
    /**
     * primaryKeyStringArray
     */
    private String[] primaryKeyStringArray;
    
    /**
     * 
     */
    private Object lock = new Object();
    
    private int currentRestructNumber;
    
    /**
     * 
     * @param molapInfo
     * @throws KettleException 
     * 
     */
    public FileStoreSurrogateKeyGenForCSV(MolapInfo molapInfo, int currentRestructNum) throws KettleException
    {
        super(molapInfo);
        currentRestructNumber = currentRestructNum;
        populatePrimaryKeyarray(dimInsertFileNames, molapInfo.getPrimaryKeyMap());
        
        keyGenerator = new HashMap<String, KeyGenerator>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        
        baseStorePath = molapInfo.getBaseStoreLocation();
        setStoreFolderWithLoadNumber(checkAndCreateLoadFolderNumber(baseStorePath,molapInfo.getTableName()));
        fileManager = new LoadFolderData();
        fileManager.setName(loadFolderName
                + MolapCommonConstants.FILE_INPROGRESS_STATUS);
        
        dimensionWriter = new LevelValueWriter[dimInsertFileNames.length];
        for(int i = 0;i < dimensionWriter.length;i++)
        {
            String dimFileName = dimInsertFileNames[i]
                    + MolapCommonConstants.LEVEL_FILE_EXTENSION;
            dimensionWriter[i] = new LevelValueWriter(dimFileName,
                    getStoreFolderWithLoadNumber());
            FileData fileData = new FileData(dimFileName,
                    getStoreFolderWithLoadNumber());
            fileData.setLevelValueWriter(dimensionWriter[i]);
            fileManager.add(fileData);
        }

        hierValueWriter = new HashMap<String, HierarchyValueWriterForCSV>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        for(Entry<String, String> entry : hierInsertFileNames.entrySet())
        {
            String hierFileName = entry.getValue().trim();
            hierValueWriter.put(entry.getKey(), new HierarchyValueWriterForCSV(hierFileName, getStoreFolderWithLoadNumber()));
            Map<String, KeyGenerator> keyGenerators = molapInfo.getKeyGenerators();
            keyGenerator.put(entry.getKey(), keyGenerators.get(entry.getKey()));
            FileData fileData = new FileData(hierFileName,getStoreFolderWithLoadNumber());
            fileData.setHierarchyValueWriter(hierValueWriter.get(entry.getKey()));
            fileManager.add(fileData);
        }
        boolean isCacheEnabled=Boolean.parseBoolean(MolapProperties
                .getInstance()
                .getProperty(
                        MolapCommonConstants.MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_ENABLED,
                        MolapCommonConstants.MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_ENABLED_DEFAULT_VALUE));
        if(isCacheEnabled)
        {
            String cacheKey = molapInfo.getSchemaName()
                    + '_'
                    + molapInfo.getCubeName();
            MolapSeqGenCacheHolder molapSeqGenCacheHolder = LRUCache.getIntance().get(cacheKey);
            if(null!=molapSeqGenCacheHolder)
            {
                LOGGER.info(
                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "********************************************** Loading from LRU cache");
                setMax(molapSeqGenCacheHolder.getMax());
                setHierCache(molapSeqGenCacheHolder.getHierCache());
                setHierCacheReverse(molapSeqGenCacheHolder.getHierCacheReverse());
                setMemberCache(molapSeqGenCacheHolder.getMemberCache());
                setTimDimMax(molapSeqGenCacheHolder.getTimDimMax());
                setTimeDimCache(molapSeqGenCacheHolder.getTimeDimCache());
                setMeasureMaxSurroagetMap(molapSeqGenCacheHolder.getMeasureMaxSurroagetMap());
            }
            else 
            {
                populateCache();
            }
        }
        else
        {
            populateCache();
        }

        
        //Update the primary key surroagate key map
        
        updatePrimaryKeyMaxSurrogateMap();
        
        
    }
    
    private void populatePrimaryKeyarray(String[] dimInsertFileNames, Map<String, Boolean> map)
    {
        List<String> primaryKeyList = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        
        for(String columnName : dimInsertFileNames)
        {
            if(null != map.get(columnName))
            {
                map.put(columnName, false);
            }
        }
        
        Set<Entry<String, Boolean>> entrySet = map.entrySet();
        
       
        
        for(Entry<String, Boolean> entry : entrySet)
        {
            if(entry.getValue())
            {
               primaryKeyList.add(entry.getKey().trim());
            }
        }
        
        primaryKeyStringArray = primaryKeyList.toArray(new String[primaryKeyList.size()]);
    }
    
    /**
     * update the 
     * 
     *
     */
    private void updatePrimaryKeyMaxSurrogateMap()
    {
        Map<String, Boolean> primaryKeyMap = molapInfo.getPrimaryKeyMap();
       
        for(Entry<String, Boolean> entry : primaryKeyMap.entrySet())
        {
            if(!primaryKeyMap.get(entry.getKey()))
            {
                int repeatedPrimaryFromLevels = getRepeatedPrimaryFromLevels(dimInsertFileNames,entry.getKey());
                
                if(null == primaryKeysMaxSurroagetMap)
                {
                    primaryKeysMaxSurroagetMap = new HashMap<String, Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
                }
                primaryKeysMaxSurroagetMap.put(entry.getKey(), max[repeatedPrimaryFromLevels]);
            }
        }
       
    }
    
    private int getRepeatedPrimaryFromLevels(String[] columnNames,String primaryKey)
    {
       for(int j = 0;j < columnNames.length;j++)
       {
         if(primaryKey.equals(columnNames[j]))
         {
             return j;
         }
       }
    return -1;
    }

    private String checkAndCreateLoadFolderNumber(String baseStorePath, String tableName) throws KettleException
    {
        int restrctFolderCount = currentRestructNumber;
        //
        if(restrctFolderCount == -1)
        {
            restrctFolderCount = 0;
        }
        //
        String baseStorePathWithTableName = baseStorePath + File.separator
                + MolapCommonConstants.RESTRUCTRE_FOLDER + restrctFolderCount + File.separator + tableName;
        int counter = MolapUtil
                .checkAndReturnCurrentLoadFolderNumber(baseStorePathWithTableName);
        if(!CheckPointHanlder.IS_CHECK_POINT_NEEDED)
        {
            counter++;
        }
        else if(counter == -1)
        {
            counter++;
        }
        String basePath = baseStorePathWithTableName + File.separator
                + MolapCommonConstants.LOAD_FOLDER + counter;
        // Incase of normalized data we will load dinemnsion data first and will rename the files, level files
        // extension from inprogress to normal , so in that case we need to start create new folder with 
        // next available folder.
        if(new File(basePath).exists())
        {
            counter++;
        }
        
        basePath = baseStorePathWithTableName + File.separator
                + MolapCommonConstants.LOAD_FOLDER + counter + MolapCommonConstants.FILE_INPROGRESS_STATUS;
                
        loadFolderName = MolapCommonConstants.LOAD_FOLDER + counter;
        
        if(new File(basePath).exists())
        {
            return basePath;
        }
        //
        boolean isDirCreated= new File(basePath).mkdirs();
        if(!isDirCreated)
        {
            throw new KettleException("Unable to create dataload directory" + basePath);
        }
        return basePath;
    }
    
    private MolapFile[] getFilesArray(String baseStorePath, final String fileNameSearchPattern)
    {
        FileType fileType = FileFactory.getFileType(baseStorePath);
        MolapFile storeFolder = FileFactory.getMolapFile(baseStorePath, fileType);
        
        MolapFile[] listFiles = storeFolder.listFiles(new MolapFileFilter()
        {
            
            @Override
            public boolean accept(MolapFile pathname)
            {
                if(pathname.getName().indexOf(fileNameSearchPattern) > -1 && !pathname.getName().endsWith(MolapCommonConstants.FILE_INPROGRESS_STATUS))
                {
                    return true;
                }
                return false;
            }
        });
        
        return listFiles;
    }
    
    
    private void populateCache() throws KettleException
    {
        //TODO temporary changes to load cache from HDFS store. If base store path itself changed to HDFS this path is not required 
        boolean exists = false;
        String baselocation = null;
        try
        {
            baselocation = MolapProperties.getInstance().getProperty(MolapCommonConstants.STORE_LOCATION_HDFS);
            if(baselocation!=null)
            {
                exists = FileFactory.isFileExist(baselocation, FileFactory.getFileType(baselocation));
            }
        }
        catch(Exception e)
        {
        	LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e, e.getMessage());
        }
        
        baselocation=baselocation+'/'+molapInfo.getSchemaName()+'/'+molapInfo.getCubeName();
        checkAndUpdateFolderList(exists ? baselocation : baseStorePath);
        
        // Fixing Check style
        String exceptionMsg = "";
        try
        {
            for(MolapFile folder : folderList)
            {
                exceptionMsg = "Not able to read level mapping File.";
                // update the member cache
                for(int i = 0;i < dimInsertFileNames.length;i++)
                {
                    MolapFile[] levelFilesArray = getFilesArray(folder.getAbsolutePath(), dimInsertFileNames[i] + MolapCommonConstants.LEVEL_FILE_EXTENSION);
                    
                    for(MolapFile file : levelFilesArray)
                    {
                        if(file.exists())
                        {
                            //
                                readLevelFileAndUpdateCache(file,
                                        dimInsertFileNames[i],false,false);
                        }
                    }

                }

                // Update the hierarchy cache
                exceptionMsg = "Not able to read hierarchy mapping File.";
                for(Entry<String, String> entry : hierInsertFileNames
                        .entrySet())
                {

                    MolapFile[] hierarchyFilesArray = getFilesArray(
                            folder.getAbsolutePath(),
                            entry.getKey()
                                    + MolapCommonConstants.HIERARCHY_FILE_EXTENSION);

                    for(MolapFile hierarchyFile : hierarchyFilesArray)
                    {
                        if(hierarchyFile.exists())
                        {

                            readHierarchyAndUpdateCache(hierarchyFile,
                                    entry.getKey());

                        }
                    }

                }
                
                exceptionMsg = "Not able to read primary value mapping File.";
                // update the member cache
                for(int i = 0;i < primaryKeyStringArray.length;i++)
                {
                    MolapFile[] primaryKeyFilesArray = getFilesArray(
                            folder.getAbsolutePath(), primaryKeyStringArray[i]
                                    + MolapCommonConstants.LEVEL_FILE_EXTENSION);

                    for(MolapFile primaryKey : primaryKeyFilesArray)
                    {

                        if(primaryKey.exists())
                        {
                            //
                            readLevelFileAndUpdateCache(primaryKey,
                                    primaryKeyStringArray[i], true, false);
                        }
                    }

                }
                
                // update the member cache for measure 
                for(int i = 0;i < molapInfo.getMeasureColumns().length;i++)
                {
                    String path = folder.getAbsolutePath()+ File.separator + molapInfo.getTableName()+ '_' + molapInfo.getMeasureColumns()[i]  + MolapCommonConstants.LEVEL_FILE_EXTENSION;
                    FileType fileType = FileFactory.getFileType(path);

                    if(FileFactory.isFileExist(path, fileType, true))
                    {
                        MolapFile file = FileFactory.getMolapFile(path, FileFactory.getFileType(path));
                        readLevelFileAndUpdateCache(file,
                                molapInfo.getTableName()+ '_' + molapInfo.getMeasureColumns()[i],false,true);
                    }

                }

            }
        }
        catch(IOException e)
        {
            throw new KettleException(exceptionMsg, e);
        }
        folderList.clear();

    }
    
    /**
     * This method recursively checks the folder with Load_ inside each and every RS_x/TableName/Load_x
     * and add in the folder list the load folders.
     * 
     * @param baseStorePath
     * @return
     * @throws KettleException 
     *
     */
    private MolapFile[] checkAndUpdateFolderList(String baseStorePath)
    {
        FileType fileType = FileFactory.getFileType(baseStorePath);
        try {
			if(!FileFactory.isFileExist(baseStorePath, fileType))
			{
				return new MolapFile[0];
			}
		} 
        catch (IOException e) 
        {
        	LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e, e.getMessage());
		}
        MolapFile folders = FileFactory.getMolapFile(baseStorePath, fileType);
        MolapFile[] rsFolders = folders.listFiles(new MolapFileFilter()
        {
            @Override
            public boolean accept(MolapFile pathname)
            {
                boolean check = false;
                if(CheckPointHanlder.IS_CHECK_POINT_NEEDED)
                {
                    check = pathname.isDirectory()
                            && pathname.getAbsolutePath().indexOf(
                                    MolapCommonConstants.LOAD_FOLDER) > -1;
                }
                else
                {
                    check = pathname.isDirectory()
                            && pathname.getAbsolutePath().indexOf(
                                    MolapCommonConstants.LOAD_FOLDER) > -1
                            && pathname.getName().indexOf(
                                    MolapCommonConstants.FILE_INPROGRESS_STATUS) == -1;
                    
                }
                if(check)
                {
                    return true;
                }
                else
                {
                    //
                    MolapFile[] checkFolder = checkAndUpdateFolderList(pathname.getAbsolutePath());
                    if(null != checkFolder)
                    {
                        for(MolapFile f: checkFolder)
                        {
                            folderList.add(f);
                        }
                    }
                }
                return false;
            }
        });
        
        return rsFolders;
        
    }
    
    @Override
    protected byte[] getHierFromStore(int[] val, String hier,int primaryKey)
            throws KettleException

    {

        byte[] bytes;
        try
        {
            bytes = molapInfo.getKeyGenerators().get(hier).generateKey(val);
            hierValueWriter.get(hier).getByteArrayList().add(new ByteArrayHolder(bytes, primaryKey));
        }
        catch(KeyGenException e)
        {
            throw new KettleException(e);
        }
        return bytes;

    }

    @Override
    protected int getSurrogateFromStore(String value, int index,
            Object[] properties) throws KettleException
    {
        max[index]++;
        int key = max[index];

            dimensionWriter[index].writeIntoLevelFile(value, key, properties);

        return key;
    }
    
    @Override
    protected int updateSurrogateToStore(String tuple, String columnName, int index,int key,
            Object[] properties) throws KettleException
    {
        Integer count = null;
        Map<String, Integer> cache = getTimeDimCache().get(columnName);
        
        if(cache == null)
        {
            return key;
        }

        count = cache.get(tuple);
        if(count == null)
        {
            if(getTimDimMax()[index] >= molapInfo.getMaxKeys()[index])
            {
                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Invalid cardinality. Key size exceeded cardinality for: "
                                + molapInfo.getDimColNames()[index]+ ": MemberValue: "+tuple);
                return -1;
            }
            getTimDimMax()[index]++;
            max[index]++;
            cache.put(tuple, key);
            synchronized(syncObject)
            {
                dimensionWriter[index].writeIntoLevelFile(tuple, key,
                        properties);
            }
        }
        else
        {
            return count;
        }

        return key;
    }
    
    
    public void writeHeirDataToFileAndCloseStreams() throws KettleException
    {
        // For closing Level value writer bufferred streams
        for(int i=0; i< dimensionWriter.length; i++)
        {
            String memberFileName = dimensionWriter[i].getMemberFileName();
            OutputStream bufferedOutputStream = dimensionWriter[i].getBufferedOutputStream();
            if(null == bufferedOutputStream)
            {
                continue;
            }
            try
            {
                dimensionWriter[i].writeMaxValue();
            }
            catch(IOException e)
            {
                LOGGER.info(
                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "********************************************** Problem writing max value :: "
                                + e.getMessage());
                throw new KettleException("Unable to write max value for level file :: " + memberFileName);
            }
            MolapUtil.closeStreams(bufferedOutputStream);
            dimensionWriter[i].clearOutputStream();
            int size = fileManager.size();
            for(int j = 0; j < size; j++)
            {
                FileData fileData = (FileData)fileManager.get(j);
                String fileName = fileData.getFileName();
                if(memberFileName.equals(fileName))
                {
                    String storePath = fileData.getStorePath();
                    LevelValueWriter levelValueWriter = fileData.getLevelValueWriter();
                    String levelFileName = levelValueWriter.getMemberFileName();
                    int counter = levelValueWriter.getCounter();
                    
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
                       if(!currentFile.delete())
                       {
                           LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Not Able to delete current file : " + currentFile.getName()); 
                       }
                    }
                    else
                    {
                        if(!currentFile.renameTo(destFile))
                        {
                            LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Not Able to rename " + currentFile.getName() + " to " + destFile.getName());
                        }
                    }
                    
                  break;
                }
                
            }
            
            
        }
        
    }

    private void readHierarchyAndUpdateCache(MolapFile hierarchyFile, String hierarchy) throws IOException
    {
        KeyGenerator generator = keyGenerator.get(hierarchy);
        int keySizeInBytes = generator.getKeySizeInBytes();
        int rowLength = keySizeInBytes+4;
        DataInputStream inputStream = null;

        inputStream = FileFactory.getDataInputStream(hierarchyFile.getAbsolutePath(), FileFactory.getFileType(hierarchyFile.getAbsolutePath()));
        
        long size = hierarchyFile.getSize();
        long position =0;
        Int2ObjectMap<int[]> hCache = getHierCache().get(hierarchy);
        Map<ArrayWrapper, Integer> hierCacheReverse = getHierCacheReverse().get(hierarchy);
        ByteBuffer rowlengthToRead = ByteBuffer.allocate(rowLength);
        byte[] rowlengthToReadBytes = new byte[rowLength];
        try
        {
            while(position < size)
            {
                inputStream.readFully(rowlengthToReadBytes);
                position+=rowLength;
                rowlengthToRead = ByteBuffer.wrap(rowlengthToReadBytes);
                rowlengthToRead.rewind();

                byte[] mdKey = new byte[keySizeInBytes];
                rowlengthToRead.get(mdKey);
                int primaryKey = rowlengthToRead.getInt();
                int[] keyArray = ByteUtil.convertToIntArray(generator.getKeyArray(mdKey));
                // Change long to int
                // update the cache
                hCache.put(primaryKey, keyArray);
                hierCacheReverse.put(new ArrayWrapper(keyArray), primaryKey);
                rowlengthToRead.clear();
            }
        }
        finally
        {
           MolapUtil.closeStreams(inputStream);
        }

    }


    private void readLevelFileAndUpdateCache(MolapFile memberFile, String fileName,boolean isPrimary,boolean isMeasure) throws IOException, KettleException
    {
    	DataInputStream inputStream = null;
        Map<String, Integer> memberMap = getMemberCache().get(fileName);
        
        if (null == memberMap)
        {
            memberMap = new HashMap<String, Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
            getMemberCache().put(fileName, memberMap);
        }
        
        Integer maxValFromMap = primaryKeysMaxSurroagetMap == null ?  Integer.valueOf(0)
                : primaryKeysMaxSurroagetMap.get(fileName);
        
        int maxKey = maxValFromMap == null ? 0 : maxValFromMap.intValue();
        
        Map<String, Integer> localMemberMap = new HashMap<String, Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        
        try
        {
        	 inputStream = FileFactory.getDataInputStream(memberFile.getPath(), FileFactory.getFileType(memberFile.getPath()));

             long currPositionIndx = 0;
       
             int minValue = inputStream.readInt();
             int surrogateValue = minValue;
             // ByteBuffer toltalLength, memberLength, surrogateKey, bf3;
             // subtracted 4 as last 4 bytes will have the max value for no of records
             long size = memberFile.getSize() - 4;
             
             boolean enableEncoding = Boolean.valueOf(MolapProperties.getInstance().getProperty(
                     MolapCommonConstants.ENABLE_BASE64_ENCODING,
                     MolapCommonConstants.ENABLE_BASE64_ENCODING_DEFAULT));
            // incremented by 4 as integer value as read for minimum no of surrogates
            currPositionIndx += 4;
            while (currPositionIndx < size)
            {
                int len = inputStream.readInt();
                currPositionIndx+=4; 
                byte[] rowBytes = new byte[len];
                inputStream.readFully(rowBytes);
                currPositionIndx+=len;
                String decodedValue = null;
                
                if(enableEncoding)
                {
                    decodedValue = new String(Base64.decodeBase64(rowBytes), Charset.defaultCharset());
                }
                else
                {
                    decodedValue = new String(rowBytes, Charset.defaultCharset());
                }
                memberMap.put(decodedValue, surrogateValue);
                maxKey = surrogateValue;
                surrogateValue++;
            }

        }
        catch(Exception e)
        {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e, "Not able to read level file for Populating Cache : " + fileName);
            MolapUtil.closeStreams(inputStream);
            if(!memberFile.delete())
            {
                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Not able to delete level File after exception.");
            }
            return;
        }
        finally
        {
            MolapUtil.closeStreams(inputStream);
        }
        
        if(isPrimary)
        {
            if(null == primaryKeysMaxSurroagetMap)
            {
                primaryKeysMaxSurroagetMap = new HashMap<String, Integer>();
            }

            primaryKeysMaxSurroagetMap.put(fileName, maxKey);
        }
        else if(isMeasure)
        {
            if(null == measureMaxSurroagetMap)
            {
                measureMaxSurroagetMap = new HashMap<String, Integer>();
            }
            measureMaxSurroagetMap.put(fileName, maxKey);
        }
        else
        {
            checkAndUpdateMap(maxKey, fileName);
        }
        memberMap.putAll(localMemberMap);
        
    }

    private void checkAndUpdateMap(int maxKey,
            String dimInsertFileNames)
    {
        String[] dimsFiles2 = getDimsFiles();
        for(int i=0; i < dimsFiles2.length; i++)
        {
            if(dimInsertFileNames.equalsIgnoreCase(dimsFiles2[i]))
            {
               if(max[i] < maxKey)
                {
                    max[i] = maxKey;
                    break;
                }
            }
        }
        
    }

    @Override
    public boolean isCacheFilled(String []columns)
    {
        for(String column : columns)
        {
            Map<String, Integer> memberMap = getMemberCache().get(column);
            if(null != memberMap && memberMap.isEmpty())
            {
                continue;
            }
            else
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public int getSurrogateKeyForPrimaryKey(String tuples, String columnName, LevelValueWriter levelValueWriter)
            throws KettleException
    {

        Integer primSurrogate = null;
        Map<String, Integer> cache = getMemberCache().get(columnName);
        
        if(null == primaryKeysMaxSurroagetMap)
        {
            primaryKeysMaxSurroagetMap = new HashMap<String, Integer>();
        }
        
        if(cache == null)
        {
            getMemberCache().put(columnName , new HashMap<String, Integer>());
        }
        
        cache = getMemberCache().get(columnName);

        primSurrogate = cache.get(tuples);
        if(primSurrogate == null)
        {
            // get the key from the map 
            primSurrogate = primaryKeysMaxSurroagetMap.get(columnName);
            if(null == primSurrogate || 0 == primSurrogate)
            {
                updatePrimaryKeyMaxSurrogateMap();
                primSurrogate = primaryKeysMaxSurroagetMap.get(columnName);
            }
            // Need to create a new surrogate key.

            cache.put(tuples, ++primSurrogate);
            levelValueWriter.writeIntoLevelFile(tuples, primSurrogate, new Object[0]);
            primaryKeysMaxSurroagetMap.put(columnName, primSurrogate);
        }
        return primSurrogate;
    
    }
    
    public IFileManagerComposite getFileManager()
    {
        return fileManager;
    }

    @Override
    protected byte[] getNormalizedHierFromStore(int[] val, String hier,
            int primaryKey, HierarchyValueWriterForCSV hierWriter)
            throws KettleException
    {
        byte[] bytes;
        try
        {
            bytes = molapInfo.getKeyGenerators().get(hier).generateKey(val);
            hierWriter.getByteArrayList().add(new ByteArrayHolder(bytes, primaryKey));
        }
        catch(KeyGenException e)
        {
            throw new KettleException(e);
        }
        return bytes;
    }

    @Override
    public int getSurrogateForMeasure(String tuple, String columnName,int index) throws KettleException
    {

        Integer measureSurrogate = null;
        
        Map<String, Map<String, Integer>> memberCache = getMemberCache();
        
        Map<String, Integer> cache = memberCache.get(columnName);
        
        if(index == -1)
        {
            
            if(null == measureValWriterMap)
            {
               measureValWriterMap = new HashMap<String, LevelValueWriter>();
            }
            
            if(null == cache)
            {
                cache = new HashMap<String, Integer>();
                memberCache.put(columnName, cache);
            }
            
            if(null == measureMaxSurroagetMap)
            {
                measureMaxSurroagetMap = new HashMap<String, Integer>();
            }
            
            measureSurrogate = cache.get(tuple);
            if(null == measureSurrogate)
            {
                String dimFileName = columnName
                        + MolapCommonConstants.LEVEL_FILE_EXTENSION;
                LevelValueWriter levelValueWriter = measureValWriterMap.get(dimFileName);
                if(null == levelValueWriter)
                {
                    synchronized(lock)
                    {
                        levelValueWriter = measureValWriterMap.get(dimFileName);
                        if(null == levelValueWriter)
                        {
                            levelValueWriter = new LevelValueWriter(
                                    dimFileName, getStoreFolderWithLoadNumber());
                            if(null == measureFilemanager)
                            {
                                measureFilemanager = new LoadFolderData();
                                measureFilemanager.setName(getStoreFolderWithLoadNumber());
                            }
                            FileData fileData = new FileData(dimFileName,
                                    getStoreFolderWithLoadNumber());
                            measureFilemanager.add(fileData);
                            measureValWriterMap.put(dimFileName, levelValueWriter);
                        }
                        
                    }
                    
                }
                
                synchronized(cache)
                {
                    measureSurrogate = cache.get(tuple);
                    if(measureSurrogate == null)
                    {
                        // get the key from the map 
                        measureSurrogate = measureMaxSurroagetMap.get(columnName);
                        if(null == measureSurrogate)
                        {
                            measureSurrogate = 0;
                        }
                        // Need to create a new surrogate key.
                        cache.put(tuple, ++measureSurrogate);
                        levelValueWriter.writeIntoLevelFile(tuple, measureSurrogate, new Object[0]);
                        measureMaxSurroagetMap.put(columnName, measureSurrogate);
                    }
                }
                
            }

        }
        else
        {
            synchronized(cache)
            {
                measureSurrogate = cache.get(tuple);
                if(measureSurrogate == null)
                {
                    measureSurrogate = getSurrogateFromStore(tuple, index, new Object[0]);
                    cache.put(tuple, measureSurrogate);
                }
            }
        }

        return measureSurrogate;
    }

    @Override
    public void writeDataToFileAndCloseStreams() throws KettleException,
            KeyGenException
    {

        // For closing Level value writer bufferred streams
        for(int i=0; i< dimensionWriter.length; i++)
        {
            String memberFileName = dimensionWriter[i].getMemberFileName();
            int size = fileManager.size();
            for(int j = 0; j < size; j++)
            {
                FileData fileData = (FileData)fileManager.get(j);
                String fileName = fileData.getFileName();
                if(memberFileName.equals(fileName))
                {
                    LevelValueWriter levelValueWriter = fileData
                            .getLevelValueWriter();

                    levelValueWriter.performRequiredOperation();

                    break;
                }
                
            }
            
            
        }
        
        // For closing stream inside hierarchy writer 
        
        for(Entry<String, String> entry : hierInsertFileNames.entrySet())
        {
            
            String hierFileName = hierValueWriter.get(entry.getKey()).getHierarchyName();
            
            int size = fileManager.size();
            for(int j = 0; j < size; j++)
            {
                FileData fileData = (FileData)fileManager.get(j);
                String fileName = fileData.getFileName();
                if(hierFileName.equals(fileName))
                {
                    HierarchyValueWriterForCSV hierarchyValueWriter = fileData.getHierarchyValueWriter();
                    hierarchyValueWriter.performRequiredOperation();

                    break;
                }
                
            }
        }

    }

}

