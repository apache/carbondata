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

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.cache.Cache;
import org.carbondata.core.cache.CacheProvider;
import org.carbondata.core.cache.CacheType;
import org.carbondata.core.cache.dictionary.*;
import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.csvreader.checkpoint.CheckPointHanlder;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.file.manager.composite.FileData;
import org.carbondata.core.file.manager.composite.IFileManagerComposite;
import org.carbondata.core.file.manager.composite.LoadFolderData;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.reader.CarbonDictionaryColumnMetaChunk;
import org.carbondata.core.reader.CarbonDictionaryMetadataReaderImpl;
import org.carbondata.core.util.ByteUtil;
import org.carbondata.core.util.CarbonDictionaryUtil;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.writer.ByteArrayHolder;
import org.carbondata.core.writer.HierarchyValueWriterForCSV;
import org.carbondata.processing.schema.metadata.ArrayWrapper;
import org.carbondata.processing.schema.metadata.CarbonInfo;
import org.carbondata.processing.surrogatekeysgenerator.dbbased.FileStoreSurrogateKeyGen;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;
import org.pentaho.di.core.exception.KettleException;

public class FileStoreSurrogateKeyGenForCSV extends CarbonCSVBasedDimSurrogateKeyGen {

    /**
     * Comment for <code>LOGGER</code>
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(FileStoreSurrogateKeyGen.class.getName());

    /**
     * syncObject
     */
    private final Object syncObject = new Object();

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
    private List<CarbonFile> folderList = new ArrayList<CarbonFile>(5);

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
     * @param carbonInfo
     * @throws KettleException
     */
    public FileStoreSurrogateKeyGenForCSV(CarbonInfo carbonInfo, int currentRestructNum)
            throws KettleException {
        super(carbonInfo);
        currentRestructNumber = currentRestructNum;
        populatePrimaryKeyarray(dimInsertFileNames, carbonInfo.getPrimaryKeyMap());

        keyGenerator =
                new HashMap<String, KeyGenerator>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

        baseStorePath = carbonInfo.getBaseStoreLocation();
        setStoreFolderWithLoadNumber(
                checkAndCreateLoadFolderNumber(baseStorePath, carbonInfo.getTableName()));
        fileManager = new LoadFolderData();
        fileManager.setName(loadFolderName + CarbonCommonConstants.FILE_INPROGRESS_STATUS);

        hierValueWriter = new HashMap<String, HierarchyValueWriterForCSV>(
                CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

        for (Entry<String, String> entry : hierInsertFileNames.entrySet()) {
            String hierFileName = entry.getValue().trim();
            hierValueWriter.put(entry.getKey(),
                    new HierarchyValueWriterForCSV(hierFileName, getStoreFolderWithLoadNumber()));
            Map<String, KeyGenerator> keyGenerators = carbonInfo.getKeyGenerators();
            keyGenerator.put(entry.getKey(), keyGenerators.get(entry.getKey()));
            FileData fileData = new FileData(hierFileName, getStoreFolderWithLoadNumber());
            fileData.setHierarchyValueWriter(hierValueWriter.get(entry.getKey()));
            fileManager.add(fileData);
        }
        populateCache();
        //Update the primary key surroagate key map
        updatePrimaryKeyMaxSurrogateMap();
    }

    private void populatePrimaryKeyarray(String[] dimInsertFileNames, Map<String, Boolean> map) {
        List<String> primaryKeyList = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

        for (String columnName : dimInsertFileNames) {
            if (null != map.get(columnName)) {
                map.put(columnName, false);
            }
        }

        Set<Entry<String, Boolean>> entrySet = map.entrySet();

        for (Entry<String, Boolean> entry : entrySet) {
            if (entry.getValue()) {
                primaryKeyList.add(entry.getKey().trim());
            }
        }

        primaryKeyStringArray = primaryKeyList.toArray(new String[primaryKeyList.size()]);
    }

    /**
     * update the
     */
    private void updatePrimaryKeyMaxSurrogateMap() {
        Map<String, Boolean> primaryKeyMap = carbonInfo.getPrimaryKeyMap();

        for (Entry<String, Boolean> entry : primaryKeyMap.entrySet()) {
            if (!primaryKeyMap.get(entry.getKey())) {
                int repeatedPrimaryFromLevels =
                        getRepeatedPrimaryFromLevels(dimInsertFileNames, entry.getKey());

                if (null == primaryKeysMaxSurroagetMap) {
                    primaryKeysMaxSurroagetMap = new HashMap<String, Integer>(
                            CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
                }
                primaryKeysMaxSurroagetMap.put(entry.getKey(), max[repeatedPrimaryFromLevels]);
            }
        }

    }

    private int getRepeatedPrimaryFromLevels(String[] columnNames, String primaryKey) {
        for (int j = 0; j < columnNames.length; j++) {
            if (primaryKey.equals(columnNames[j])) {
                return j;
            }
        }
        return -1;
    }

    private String checkAndCreateLoadFolderNumber(String baseStorePath, String tableName)
            throws KettleException {
        int restrctFolderCount = currentRestructNumber;
        //
        if (restrctFolderCount == -1) {
            restrctFolderCount = 0;
        }
        //
        String baseStorePathWithTableName =
                baseStorePath + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER
                        + restrctFolderCount + File.separator + tableName;
        int counter = CarbonUtil.checkAndReturnCurrentLoadFolderNumber(baseStorePathWithTableName);
        if (!CheckPointHanlder.IS_CHECK_POINT_NEEDED) {
            counter++;
        } else if (counter == -1) {
            counter++;
        }
        String basePath =
                baseStorePathWithTableName + File.separator + CarbonCommonConstants.LOAD_FOLDER
                        + counter;
        // Incase of normalized data we will load dinemnsion data first and will rename the files, level files
        // extension from inprogress to normal , so in that case we need to start create new folder with 
        // next available folder.
        if (new File(basePath).exists()) {
            counter++;
        }

        basePath = baseStorePathWithTableName + File.separator + CarbonCommonConstants.LOAD_FOLDER
                + counter + CarbonCommonConstants.FILE_INPROGRESS_STATUS;

        loadFolderName = CarbonCommonConstants.LOAD_FOLDER + counter;

        if (new File(basePath).exists()) {
            return basePath;
        }
        //
        boolean isDirCreated = new File(basePath).mkdirs();
        if (!isDirCreated) {
            throw new KettleException("Unable to create dataload directory" + basePath);
        }
        return basePath;
    }

    private CarbonFile[] getFilesArray(String baseStorePath, final String fileNameSearchPattern) {
        FileType fileType = FileFactory.getFileType(baseStorePath);
        CarbonFile storeFolder = FileFactory.getCarbonFile(baseStorePath, fileType);

        CarbonFile[] listFiles = storeFolder.listFiles(new CarbonFileFilter() {

            @Override
            public boolean accept(CarbonFile pathname) {
                if (pathname.getName().indexOf(fileNameSearchPattern) > -1 && !pathname.getName()
                        .endsWith(CarbonCommonConstants.FILE_INPROGRESS_STATUS)) {
                    return true;
                }
                return false;
            }
        });

        return listFiles;
    }

    /**
     * This method will update the maxkey information.
     * @param carbonStorePath       location where Carbon will create the store and write the data in its own format.
     * @param carbonTableIdentifier table identifier which will give table name and database name
     * @param columnName            the name of column
     * @param tabColumnName         tablename + "_" + columnname, for example table_col
     * @param isMeasure whether this column is measure or not
     */
    private void updateMaxKeyInfo(String carbonStorePath, CarbonTableIdentifier carbonTableIdentifier, String columnName,
                                  String tabColumnName, boolean isMeasure) throws IOException {
        CarbonDictionaryMetadataReaderImpl columnMetadataReaderImpl =
                new CarbonDictionaryMetadataReaderImpl(carbonStorePath,
                        carbonTableIdentifier, columnName, false);
        CarbonDictionaryColumnMetaChunk lastEntryOfDictionaryMetaChunk = null;
        // read metadata file
        try {
            lastEntryOfDictionaryMetaChunk = columnMetadataReaderImpl.readLastEntryOfDictionaryMetaChunk();
        } catch (IOException e) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, "Can not find the dictionary meta" +
                    " file of this column: " + columnName);
            throw new IOException();
        } finally {
            columnMetadataReaderImpl.close();
        }
        int maxKey = lastEntryOfDictionaryMetaChunk.getMax_surrogate_key();
        if (isMeasure) {
            if (null == measureMaxSurroagetMap) {
                measureMaxSurroagetMap = new HashMap<String, Integer>();
            }
            measureMaxSurroagetMap.put(tabColumnName, maxKey);
        } else {
            checkAndUpdateMap(maxKey, tabColumnName);
        }
    }

    /**
     * This method will generate cache for all the global dictionaries during data loading.
     */
    private void populateCache() throws KettleException {
        String hdfsStorePath = CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS);
        String[] dimColumnNames = carbonInfo.getDimColNames();
        String[] mesColumnNames = carbonInfo.getMeasureColumns();
        boolean isSharedDimension = false;
        String databaseName = carbonInfo.getSchemaName().substring(0, carbonInfo.getSchemaName().lastIndexOf("_"));
        String tableName = carbonInfo.getTableName();
        CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier(databaseName, tableName);
        String directoryPath = CarbonDictionaryUtil.getDirectoryPath(carbonTableIdentifier, hdfsStorePath, isSharedDimension);
        //update the member cache for dimension
        for (int i = 0; i < dimColumnNames.length; i++) {
            String dimColName = dimColumnNames[i].substring(tableName.length() + 1);
            initDicCacheInfo(carbonTableIdentifier, dimColName, dimColumnNames[i], hdfsStorePath);
            try {
                updateMaxKeyInfo(hdfsStorePath, carbonTableIdentifier, dimColName, dimColumnNames[i], false);
            } catch (IOException e) {
                LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, "Can not read metadata file" +
                        "of this column: " + dimColName);
            }
        }
        //update the member cache for measure
        for (int i = 0; i < mesColumnNames.length; i++) {
            String mesDictionaryFilePath = CarbonDictionaryUtil.getDictionaryFilePath(carbonTableIdentifier,
                    directoryPath, mesColumnNames[i],isSharedDimension);
            if (CarbonUtil.isFileExists(mesDictionaryFilePath)) {
                String mesTabColumnName = carbonInfo.getTableName() + CarbonCommonConstants.UNDERSCORE + mesColumnNames[i];
                initDicCacheInfo(carbonTableIdentifier, mesColumnNames[i], mesTabColumnName, hdfsStorePath);
                try {
                    updateMaxKeyInfo(hdfsStorePath, carbonTableIdentifier, mesColumnNames[i], mesTabColumnName, true);
                } catch (IOException e) {
                    LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, "Can not read metadata file" +
                            "of this column: " + mesColumnNames[i]);
                }
            }
        }

    }

    /**
     * This method will initial the needed information for a dictionary of one column.
     *
     * @param carbonTableIdentifier table identifier which will give table name and database name
     * @param columnName            the name of column
     * @param tabColumnName         tablename + "_" + columnname, for example table_col
     * @param carbonStorePath       location where Carbon will create the store and write the data in its own format.
     */
    private void initDicCacheInfo(CarbonTableIdentifier carbonTableIdentifier, String columnName, String tabColumnName,
                                  String carbonStorePath ){
        DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
                new DictionaryColumnUniqueIdentifier(carbonTableIdentifier, columnName);
        CacheProvider cacheProvider = CacheProvider.getInstance();
        Cache reverseDictionaryCache = cacheProvider.createCache(CacheType.REVERSE_DICTIONARY, carbonStorePath );
        Dictionary reverseDictionary = (Dictionary) reverseDictionaryCache.get(dictionaryColumnUniqueIdentifier);
        if(null != reverseDictionary){
            getDictionaryCaches().put(tabColumnName, reverseDictionary);
        }
    }

    /**
     * This method recursively checks the folder with Load_ inside each and every RS_x/TableName/Load_x
     * and add in the folder list the load folders.
     *
     * @param baseStorePath
     * @return
     * @throws KettleException
     */
    private CarbonFile[] checkAndUpdateFolderList(String baseStorePath) {
        FileType fileType = FileFactory.getFileType(baseStorePath);
        try {
            if (!FileFactory.isFileExist(baseStorePath, fileType)) {
                return new CarbonFile[0];
            }
        } catch (IOException e) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e,
                    e.getMessage());
        }
        CarbonFile folders = FileFactory.getCarbonFile(baseStorePath, fileType);
        CarbonFile[] rsFolders = folders.listFiles(new CarbonFileFilter() {
            @Override
            public boolean accept(CarbonFile pathname) {
                boolean check = false;
                if (CheckPointHanlder.IS_CHECK_POINT_NEEDED) {
                    check = pathname.isDirectory()
                            && pathname.getAbsolutePath().indexOf(CarbonCommonConstants.LOAD_FOLDER)
                            > -1;
                } else {
                    check = pathname.isDirectory()
                            && pathname.getAbsolutePath().indexOf(CarbonCommonConstants.LOAD_FOLDER)
                            > -1 &&
                            pathname.getName().indexOf(CarbonCommonConstants.FILE_INPROGRESS_STATUS)
                                    == -1;

                }
                if (check) {
                    return true;
                } else {
                    //
                    CarbonFile[] checkFolder = checkAndUpdateFolderList(pathname.getAbsolutePath());
                    if (null != checkFolder) {
                        for (CarbonFile f : checkFolder) {
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
    protected byte[] getHierFromStore(int[] val, String hier, int primaryKey) throws KettleException

    {

        byte[] bytes;
        try {
            bytes = carbonInfo.getKeyGenerators().get(hier).generateKey(val);
            hierValueWriter.get(hier).getByteArrayList()
                    .add(new ByteArrayHolder(bytes, primaryKey));
        } catch (KeyGenException e) {
            throw new KettleException(e);
        }
        return bytes;

    }

    @Override
    protected int getSurrogateFromStore(String value, int index, Object[] properties)
            throws KettleException {
        max[index]++;
        int key = max[index];
        return key;
    }

    @Override
    protected int updateSurrogateToStore(String tuple, String columnName, int index, int key,
            Object[] properties) throws KettleException {
        Integer count = null;
        Map<String, Integer> cache = getTimeDimCache().get(columnName);

        if (cache == null) {
            return key;
        }
        count = cache.get(tuple);
        return key;
    }

    private void readHierarchyAndUpdateCache(CarbonFile hierarchyFile, String hierarchy)
            throws IOException {
        KeyGenerator generator = keyGenerator.get(hierarchy);
        int keySizeInBytes = generator.getKeySizeInBytes();
        int rowLength = keySizeInBytes + 4;
        DataInputStream inputStream = null;

        inputStream = FileFactory.getDataInputStream(hierarchyFile.getAbsolutePath(),
                FileFactory.getFileType(hierarchyFile.getAbsolutePath()));

        long size = hierarchyFile.getSize();
        long position = 0;
        Int2ObjectMap<int[]> hCache = getHierCache().get(hierarchy);
        Map<ArrayWrapper, Integer> hierCacheReverse = getHierCacheReverse().get(hierarchy);
        ByteBuffer rowlengthToRead = ByteBuffer.allocate(rowLength);
        byte[] rowlengthToReadBytes = new byte[rowLength];
        try {
            while (position < size) {
                inputStream.readFully(rowlengthToReadBytes);
                position += rowLength;
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
        } finally {
            CarbonUtil.closeStreams(inputStream);
        }

    }

    private void checkAndUpdateMap(int maxKey, String dimInsertFileNames) {
        String[] dimsFiles2 = getDimsFiles();
        for (int i = 0; i < dimsFiles2.length; i++) {
            if (dimInsertFileNames.equalsIgnoreCase(dimsFiles2[i])) {
                if (max[i] < maxKey) {
                    max[i] = maxKey;
                    break;
                }
            }
        }

    }

    @Override
    public boolean isCacheFilled(String[] columns) {
        for (String column : columns) {
            org.carbondata.core.cache.dictionary.Dictionary dicCache = getDictionaryCaches().get(column);
            if (null != dicCache) {
                continue;
            } else {
                return true;
            }
        }
        return false;
    }

    public IFileManagerComposite getFileManager() {
        return fileManager;
    }

    @Override
    protected byte[] getNormalizedHierFromStore(int[] val, String hier, int primaryKey,
            HierarchyValueWriterForCSV hierWriter) throws KettleException {
        byte[] bytes;
        try {
            bytes = carbonInfo.getKeyGenerators().get(hier).generateKey(val);
            hierWriter.getByteArrayList().add(new ByteArrayHolder(bytes, primaryKey));
        } catch (KeyGenException e) {
            throw new KettleException(e);
        }
        return bytes;
    }

    @Override
    public int getSurrogateForMeasure(String tuple, String columnName)
            throws KettleException {

        Integer measureSurrogate = null;

        Map<String, org.carbondata.core.cache.dictionary.Dictionary> dictionaryCaches = getDictionaryCaches();

        org.carbondata.core.cache.dictionary.Dictionary dicCache = dictionaryCaches.get(columnName);

        measureSurrogate = dicCache.getSurrogateKey(tuple);

        return measureSurrogate;
    }

    @Override
    public void writeDataToFileAndCloseStreams() throws KettleException, KeyGenException {

        // For closing stream inside hierarchy writer 

        for (Entry<String, String> entry : hierInsertFileNames.entrySet()) {

            String hierFileName = hierValueWriter.get(entry.getKey()).getHierarchyName();

            int size = fileManager.size();
            for (int j = 0; j < size; j++) {
                FileData fileData = (FileData) fileManager.get(j);
                String fileName = fileData.getFileName();
                if (hierFileName.equals(fileName)) {
                    HierarchyValueWriterForCSV hierarchyValueWriter =
                            fileData.getHierarchyValueWriter();
                    hierarchyValueWriter.performRequiredOperation();

                    break;
                }

            }
        }

    }

}

