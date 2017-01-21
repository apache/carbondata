/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.processing.surrogatekeysgenerator.csvbased;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.writer.ByteArrayHolder;
import org.apache.carbondata.core.writer.HierarchyValueWriterForCSV;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.mdkeygen.file.FileData;
import org.apache.carbondata.processing.mdkeygen.file.FileManager;
import org.apache.carbondata.processing.mdkeygen.file.IFileManagerComposite;
import org.apache.carbondata.processing.schema.metadata.ColumnSchemaDetails;
import org.apache.carbondata.processing.schema.metadata.ColumnSchemaDetailsWrapper;
import org.apache.carbondata.processing.schema.metadata.ColumnsInfo;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.pentaho.di.core.exception.KettleException;

public class FileStoreSurrogateKeyGenForCSV extends CarbonCSVBasedDimSurrogateKeyGen {

  /**
   * hierValueWriter
   */
  private Map<String, HierarchyValueWriterForCSV> hierValueWriter;

  /**
   * keyGenerator
   */
  private Map<String, KeyGenerator> keyGenerator;

  /**
   * LOAD_FOLDER
   */
  private String loadFolderName;

  /**
   * primaryKeyStringArray
   */
  private String[] primaryKeyStringArray;
  /**
   * partitionID
   */
  private String partitionID;
  /**
   * load Id
   */
  private String segmentId;
  /**
   * task id, each spark task has a unique id
   */
  private String taskNo;

  /**
   * @param columnsInfo
   * @throws IOException
   */
  public FileStoreSurrogateKeyGenForCSV(ColumnsInfo columnsInfo, String partitionID,
      String segmentId, String taskNo) throws IOException {
    super(columnsInfo);
    populatePrimaryKeyarray(dimInsertFileNames, columnsInfo.getPrimaryKeyMap());
    this.partitionID = partitionID;
    this.segmentId = segmentId;
    this.taskNo = taskNo;
    keyGenerator = new HashMap<String, KeyGenerator>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    setStoreFolderWithLoadNumber(
        checkAndCreateLoadFolderNumber(columnsInfo.getDatabaseName(),
            columnsInfo.getTableName()));
    fileManager = new FileManager();
    fileManager.setName(loadFolderName + CarbonCommonConstants.FILE_INPROGRESS_STATUS);

    hierValueWriter = new HashMap<String, HierarchyValueWriterForCSV>(
        CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    for (Entry<String, String> entry : hierInsertFileNames.entrySet()) {
      String hierFileName = entry.getValue().trim();
      hierValueWriter.put(entry.getKey(),
          new HierarchyValueWriterForCSV(hierFileName, getStoreFolderWithLoadNumber()));
      Map<String, KeyGenerator> keyGenerators = columnsInfo.getKeyGenerators();
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
    Map<String, Boolean> primaryKeyMap = columnsInfo.getPrimaryKeyMap();
    for (Entry<String, Boolean> entry : primaryKeyMap.entrySet()) {
      if (!primaryKeyMap.get(entry.getKey())) {
        int repeatedPrimaryFromLevels =
            getRepeatedPrimaryFromLevels(dimInsertFileNames, entry.getKey());

        if (null == primaryKeysMaxSurroagetMap) {
          primaryKeysMaxSurroagetMap =
              new HashMap<String, Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
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

  private String checkAndCreateLoadFolderNumber(String databaseName,
      String tableName) throws IOException {
    String carbonDataDirectoryPath = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(databaseName, tableName, taskNo, partitionID, segmentId + "",
            false);
    boolean isDirCreated = new File(carbonDataDirectoryPath).mkdirs();
    if (!isDirCreated) {
      throw new IOException("Unable to create data load directory" + carbonDataDirectoryPath);
    }
    return carbonDataDirectoryPath;
  }

  /**
   * This method will update the maxkey information.
   * @param tabColumnName
   * @param maxKey max cardinality of a column
   */
  private void updateMaxKeyInfo(String tabColumnName, int maxKey) {
    checkAndUpdateMap(maxKey, tabColumnName);
  }

  /**
   * This method will generate cache for all the global dictionaries during data loading.
   */
  private void populateCache() throws IOException {
    String carbonStorePath =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS);
    String[] dimColumnNames = columnsInfo.getDimColNames();
    String[] dimColumnIds = columnsInfo.getDimensionColumnIds();
    String databaseName = columnsInfo.getDatabaseName();
    String tableName = columnsInfo.getTableName();
    CarbonTable carbonTable = CarbonMetadata.getInstance()
        .getCarbonTable(databaseName + CarbonCommonConstants.UNDERSCORE + tableName);
    CarbonTableIdentifier carbonTableIdentifier = carbonTable.getCarbonTableIdentifier();
    CacheProvider cacheProvider = CacheProvider.getInstance();
    Cache reverseDictionaryCache =
        cacheProvider.createCache(CacheType.REVERSE_DICTIONARY, carbonStorePath);
    List<String> dictionaryKeys = new ArrayList<>(dimColumnNames.length);
    List<DictionaryColumnUniqueIdentifier> dictionaryColumnUniqueIdentifiers =
        new ArrayList<>(dimColumnNames.length);
    ColumnSchemaDetailsWrapper columnSchemaDetailsWrapper =
        columnsInfo.getColumnSchemaDetailsWrapper();
    // update the member cache for dimension
    for (int i = 0; i < dimColumnNames.length; i++) {
      String dimColName = dimColumnNames[i].substring(tableName.length() + 1);
      ColumnSchemaDetails details = columnSchemaDetailsWrapper.get(dimColumnIds[i]);
      if (details.isDirectDictionary()) {
        continue;
      }
      GenericDataType complexType = columnsInfo.getComplexTypesMap().get(dimColName);
      if (complexType != null) {
        List<GenericDataType> primitiveChild = new ArrayList<GenericDataType>();
        complexType.getAllPrimitiveChildren(primitiveChild);
        for (GenericDataType eachPrimitive : primitiveChild) {
          details = columnSchemaDetailsWrapper.get(eachPrimitive.getColumnId());
          if (details.isDirectDictionary()) {
            continue;
          }
          ColumnIdentifier columnIdentifier = new ColumnIdentifier(eachPrimitive.getColumnId(),
              columnsInfo.getColumnProperties(eachPrimitive.getName()), details.getColumnType());
          String dimColumnName =
              tableName + CarbonCommonConstants.UNDERSCORE + eachPrimitive.getName();
          DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
              new DictionaryColumnUniqueIdentifier(carbonTableIdentifier, columnIdentifier);
          dictionaryColumnUniqueIdentifiers.add(dictionaryColumnUniqueIdentifier);
          dictionaryKeys.add(dimColumnName);
        }
      } else {
        ColumnIdentifier columnIdentifier =
            new ColumnIdentifier(dimColumnIds[i], columnsInfo.getColumnProperties(dimColName),
                details.getColumnType());
        DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
            new DictionaryColumnUniqueIdentifier(carbonTableIdentifier, columnIdentifier);
        dictionaryColumnUniqueIdentifiers.add(dictionaryColumnUniqueIdentifier);
        dictionaryKeys.add(dimColumnNames[i]);
      }
    }
    initDictionaryCacheInfo(dictionaryKeys, dictionaryColumnUniqueIdentifiers,
        reverseDictionaryCache);
  }

  /**
   * This method will initial the needed information for a dictionary of one column.
   *
   * @param dictionaryKeys
   * @param dictionaryColumnUniqueIdentifiers
   * @param reverseDictionaryCache
   * @throws KettleException
   */
  private void initDictionaryCacheInfo(List<String> dictionaryKeys,
      List<DictionaryColumnUniqueIdentifier> dictionaryColumnUniqueIdentifiers,
      Cache reverseDictionaryCache) throws IOException {
    long lruCacheStartTime = System.currentTimeMillis();
    List reverseDictionaries = reverseDictionaryCache.getAll(dictionaryColumnUniqueIdentifiers);
    for (int i = 0; i < reverseDictionaries.size(); i++) {
      Dictionary reverseDictionary = (Dictionary) reverseDictionaries.get(i);
      getDictionaryCaches().put(dictionaryKeys.get(i), reverseDictionary);
      updateMaxKeyInfo(dictionaryKeys.get(i), reverseDictionary.getDictionaryChunks().getSize());
    }
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordLruCacheLoadTime(
        (System.currentTimeMillis() - lruCacheStartTime)/1000.0);
  }

  @Override protected int getSurrogateFromStore(String value, int index, Object[] properties)
      throws KettleException {
    max[index]++;
    int key = max[index];
    return key;
  }

  @Override
  protected int updateSurrogateToStore(String tuple, String columnName, int index, int key,
      Object[] properties) throws KettleException {
    Map<String, Integer> cache = getTimeDimCache().get(columnName);
    if (cache == null) {
      return key;
    }
    return key;
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

  @Override public boolean isCacheFilled(String[] columns) {
    for (String column : columns) {
      Dictionary dicCache = getDictionaryCaches().get(column);
      if (null == dicCache) {
        return true;
      }
    }
    return false;
  }

  public IFileManagerComposite getFileManager() {
    return fileManager;
  }

  @Override protected byte[] getNormalizedHierFromStore(int[] val, String hier, int primaryKey,
      HierarchyValueWriterForCSV hierWriter) throws KettleException {
    byte[] bytes;
    try {
      bytes = columnsInfo.getKeyGenerators().get(hier).generateKey(val);
      hierWriter.getByteArrayList().add(new ByteArrayHolder(bytes, primaryKey));
    } catch (KeyGenException e) {
      throw new KettleException(e);
    }
    return bytes;
  }

  @Override public int getSurrogateForMeasure(String tuple, String columnName)
      throws KettleException {
    Integer measureSurrogate = null;
    Map<String, Dictionary> dictionaryCaches = getDictionaryCaches();
    Dictionary dicCache = dictionaryCaches.get(columnName);
    measureSurrogate = dicCache.getSurrogateKey(tuple);
    return measureSurrogate;
  }

}
