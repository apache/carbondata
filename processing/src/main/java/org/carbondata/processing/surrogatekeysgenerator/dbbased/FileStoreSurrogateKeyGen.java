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

package org.carbondata.processing.surrogatekeysgenerator.dbbased;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.cache.Cache;
import org.carbondata.core.cache.CacheProvider;
import org.carbondata.core.cache.CacheType;
import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.file.manager.composite.FileData;
import org.carbondata.core.file.manager.composite.IFileManagerComposite;
import org.carbondata.core.file.manager.composite.LoadFolderData;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.reader.CarbonDictionaryColumnMetaChunk;
import org.carbondata.core.reader.CarbonDictionaryMetadataReaderImpl;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.processing.schema.metadata.ColumnsInfo;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;

import org.pentaho.di.core.exception.KettleException;

public class FileStoreSurrogateKeyGen extends CarbonDimSurrogateKeyGen {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(FileStoreSurrogateKeyGen.class.getName());
  /**
   * hierValueWriter
   */
  private Map<String, HierarchyValueWriter> hierValueWriter;

  /**
   * keyGenerator
   */
  private Map<String, KeyGenerator> keyGeneratorMap;

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
  private List<File> folderList = new ArrayList<File>(5);

  /**
   * File manager
   */
  private IFileManagerComposite fileManager;

  private int currentRestructNumber;

  /**
   * @param columnsInfo
   * @throws KettleException
   */
  public FileStoreSurrogateKeyGen(ColumnsInfo columnsInfo, int currentRestructNum)
      throws KettleException {
    super(columnsInfo);

    keyGeneratorMap =
        new HashMap<String, KeyGenerator>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    baseStorePath = columnsInfo.getBaseStoreLocation();

    String storeFolderWithLoadNumber =
        checkAndCreateLoadFolderNumber(baseStorePath, columnsInfo.getTableName());

    fileManager = new LoadFolderData();
    fileManager.setName(loadFolderName + CarbonCommonConstants.FILE_INPROGRESS_STATUS);
    hierValueWriter =
        new HashMap<String, HierarchyValueWriter>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    for (Entry<String, String> entry : hierInsertFileNames.entrySet()) {
      String hierFileName = entry.getValue().trim() + CarbonCommonConstants.FILE_INPROGRESS_STATUS;
      hierValueWriter
          .put(entry.getKey(), new HierarchyValueWriter(hierFileName, storeFolderWithLoadNumber));
      Map<String, KeyGenerator> keyGenerators = columnsInfo.getKeyGenerators();
      keyGeneratorMap.put(entry.getKey(), keyGenerators.get(entry.getKey()));

      FileData fileData = new FileData(hierFileName, storeFolderWithLoadNumber);
      fileManager.add(fileData);
    }

    populateCache();

    this.currentRestructNumber = currentRestructNum;
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
    counter++;
    String basePath =
        baseStorePathWithTableName + File.separator + CarbonCommonConstants.LOAD_FOLDER + counter;

    basePath = basePath + CarbonCommonConstants.FILE_INPROGRESS_STATUS;
    loadFolderName = CarbonCommonConstants.LOAD_FOLDER + counter;
    //
    boolean isDirCreated = new File(basePath).mkdirs();
    if (!isDirCreated) {
      throw new KettleException("Unable to create dataload directory" + basePath);
    }
    return basePath;
  }

  /**
   * This method will update the maxkey information.
   *
   * @param carbonStorePath       location where Carbon will create the store and write the data
   *                              in its own format.
   * @param carbonTableIdentifier table identifier which will give table name and database name
   * @param columnName            the name of column
   * @param tabColumnName         tablename + "_" + columnname, for example table_col
   */
  private void updateMaxKeyInfo(String carbonStorePath, CarbonTableIdentifier carbonTableIdentifier,
      String columnName, String tabColumnName) throws IOException {
    CarbonDictionaryMetadataReaderImpl columnMetadataReaderImpl =
        new CarbonDictionaryMetadataReaderImpl(carbonStorePath, carbonTableIdentifier, columnName,
            false);
    CarbonDictionaryColumnMetaChunk lastEntryOfDictionaryMetaChunk = null;
    // read metadata file
    try {
      lastEntryOfDictionaryMetaChunk =
          columnMetadataReaderImpl.readLastEntryOfDictionaryMetaChunk();
    } catch (IOException e) {
      LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
          "Can not find the dictionary meta" +
              " file of this column: " + columnName);
      throw new IOException();
    } finally {
      columnMetadataReaderImpl.close();
    }
    int maxKey = lastEntryOfDictionaryMetaChunk.getMax_surrogate_key();
    checkAndUpdateMap(maxKey, tabColumnName);
  }

  /**
   * This method will generate cache for all the global dictionaries during data loading.
   */
  private void populateCache() throws KettleException {
    String carbonStorePath =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS);
    String[] dimColumnNames = columnsInfo.getDimColNames();
    boolean isSharedDimension = false;
    String databaseName =
        columnsInfo.getSchemaName().substring(0, columnsInfo.getSchemaName().lastIndexOf("_"));
    String tableName = columnsInfo.getTableName();
    CarbonTableIdentifier carbonTableIdentifier =
        new CarbonTableIdentifier(databaseName, tableName);
    //update the member cache for dimension
    for (int i = 0; i < dimColumnNames.length; i++) {
      String dimColName = dimColumnNames[i].substring(tableName.length() + 1);
      initDicCacheInfo(carbonTableIdentifier, dimColName, dimColumnNames[i], carbonStorePath);
      try {
        updateMaxKeyInfo(carbonStorePath, carbonTableIdentifier, dimColName, dimColumnNames[i]);
      } catch (IOException e) {
        LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
            "Can not read metadata file" +
                "of this column: " + dimColName);
      }
    }
  }

  /**
   * This method will initial the needed information for a dictionary of one column.
   *
   * @param carbonTableIdentifier table identifier which will give table name and database name
   * @param columnName            the name of column
   * @param tabColumnName         tablename + "_" + columnname, for example table_col
   * @param carbonStorePath       location where Carbon will create the store and write the data
   *                              in its own format.
   */
  private void initDicCacheInfo(CarbonTableIdentifier carbonTableIdentifier, String columnName,
      String tabColumnName, String carbonStorePath) {
    DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
        new DictionaryColumnUniqueIdentifier(carbonTableIdentifier, columnName);
    CacheProvider cacheProvider = CacheProvider.getInstance();
    Cache reverseDictionaryCache =
        cacheProvider.createCache(CacheType.REVERSE_DICTIONARY, carbonStorePath);
    Dictionary reverseDictionary =
        (Dictionary) reverseDictionaryCache.get(dictionaryColumnUniqueIdentifier);
    if (null != reverseDictionary) {
      dictionaryCaches.put(tabColumnName, reverseDictionary);
    }
  }

  /**
   * This method recursively checks the folder with Load_ inside each and every
   * RS_x/TableName/Load_x
   * and add in the folder list the load folders.
   *
   * @param baseStorePath
   * @return
   */
  private File[] checkAndUpdateFolderList(String baseStorePath) {
    File folders = new File(baseStorePath);
    //
    File[] rsFolders = folders.listFiles(new FileFilter() {
      @Override public boolean accept(File pathname) {
        if (pathname.isDirectory()
            && pathname.getAbsolutePath().indexOf(CarbonCommonConstants.LOAD_FOLDER) > -1) {
          return true;
        } else {
          //
          File[] checkFolder = checkAndUpdateFolderList(pathname.getAbsolutePath());
          if (null != checkFolder) {
            for (File f : checkFolder) {
              folderList.add(f);
            }
          }
        }
        return false;
      }
    });

    return rsFolders;
  }

  @Override protected byte[] getHierFromStore(int[] val, String hier) throws KettleException

  {

    long[] value = new long[val.length];

    System.arraycopy(val, 0, value, 0, val.length);

    byte[] bytes;
    try {
      bytes = columnsInfo.getKeyGenerators().get(hier).generateKey(value);
      hierValueWriter.get(hier).getByteArrayList().add(bytes);
    } catch (KeyGenException ex) {
      throw new KettleException(ex);
    }
    return bytes;

  }

  @Override protected int getSurrogateFromStore(String value, int index, Object[] properties)
      throws KettleException {
    max[index]++;
    int key = max[index];
    return key;
  }

  public void writeHeirDataToFileAndCloseStreams() throws KettleException {
    // For closing stream inside hierarchy writer
    for (Entry<String, String> entry : hierInsertFileNames.entrySet()) {
      // First we need to sort the byte array
      List<byte[]> byteArrayList = hierValueWriter.get(entry.getKey()).getByteArrayList();
      String hierFileName = hierValueWriter.get(entry.getKey()).getHierarchyName();
      Collections.sort(byteArrayList, columnsInfo.getKeyGenerators().get(entry.getKey()));
      byte[] bytesTowrite = null;
      for (byte[] bytes : byteArrayList) {
        bytesTowrite = new byte[bytes.length + 4];
        System.arraycopy(bytes, 0, bytesTowrite, 0, bytes.length);
        hierValueWriter.get(entry.getKey()).writeIntoHierarchyFile(bytesTowrite);
      }

      // now write the byte array in the file.
      BufferedOutputStream bufferedOutStream =
          hierValueWriter.get(entry.getKey()).getBufferedOutStream();
      if (null == bufferedOutStream) {
        continue;
      }
      CarbonUtil.closeStreams(hierValueWriter.get(entry.getKey()).getBufferedOutStream());

      int size = fileManager.size();

      for (int j = 0; j < size; j++) {
        FileData fileData = (FileData) fileManager.get(j);
        String fileName = fileData.getFileName();
        if (hierFileName.equals(fileName)) {
          String storePath = fileData.getStorePath();
          String inProgFileName = fileData.getFileName();
          String changedFileName = fileName.substring(0, fileName.lastIndexOf('.'));
          File currentFile = new File(storePath + File.separator + inProgFileName);
          File destFile = new File(storePath + File.separator + changedFileName);

          currentFile.renameTo(destFile);

          fileData.setName(changedFileName);

          break;
        }

      }
    }
  }

  private void readHierarchyAndUpdateCache(File hierarchyFile, String hierarchy)
      throws IOException {
    KeyGenerator generator = keyGeneratorMap.get(hierarchy);
    int keySizeInBytes = generator.getKeySizeInBytes();

    FileInputStream inputStream = null;
    FileChannel fileChannel = null;
    ByteBuffer byteBuffer = ByteBuffer.allocate(keySizeInBytes + 4);
    try {
      inputStream = new FileInputStream(hierarchyFile);
      fileChannel = inputStream.getChannel();

      while (fileChannel.read(byteBuffer) != -1) {
        byte[] array = byteBuffer.array();
        byte[] keyByteArray = new byte[keySizeInBytes];
        System.arraycopy(array, 0, keyByteArray, 0, keySizeInBytes);
        long[] keyArray = generator.getKeyArray(keyByteArray);
        int[] hirerarchyValues = new int[keyArray.length];
        // Change long to int
        for (int i = 0; i < keyArray.length; i++) {
          //CHECKSTYLE:OFF
          hirerarchyValues[i] = (int) keyArray[i];
          //CHECKSTYLE:ON

        }
        // update the cache
        updateHierCache(hirerarchyValues, hierarchy);

        byteBuffer.clear();
      }
    } finally {
      CarbonUtil.closeStreams(fileChannel, inputStream);
    }

  }

  private void updateHierCache(int[] hirerarchyValues, String hierarchy) {
    //
    IntArrayWrapper wrapper = new IntArrayWrapper(hirerarchyValues, 0);
    Map<IntArrayWrapper, Boolean> hCache = hierCache.get(hierarchy);
    //
    Boolean b = hCache.get(wrapper);
    if (b != null) {
      return;
    }
    //
    wLock2.lock();
    if (null == hCache.get(wrapper)) {
      // Store in cache
      hCache.put(wrapper, true);
    }
    wLock2.unlock();

  }

  private void checkAndUpdateMap(int maxKey, String dimInsertFileNames) {
    for (int i = 0; i < dimsFiles.length; i++) {
      if (dimInsertFileNames.equalsIgnoreCase(dimsFiles[i])) {
        max[i] = maxKey;
        break;
      }
    }

  }

  @Override protected byte[] getNormalizedHierFromStore(int[] val, String hier,
      HierarchyValueWriter hierWriter) throws KettleException {
    byte[] bytes;
    try {
      bytes = columnsInfo.getKeyGenerators().get(hier).generateKey(val);
      hierWriter.getByteArrayList().add(bytes);
    } catch (KeyGenException e) {
      throw new KettleException(e);
    }
    return bytes;
  }

}

