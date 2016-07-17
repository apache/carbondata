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

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.file.manager.composite.IFileManagerComposite;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.carbondata.core.writer.HierarchyValueWriterForCSV;
import org.carbondata.processing.datatypes.GenericDataType;
import org.carbondata.processing.schema.metadata.ArrayWrapper;
import org.carbondata.processing.schema.metadata.ColumnSchemaDetails;
import org.carbondata.processing.schema.metadata.ColumnsInfo;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import org.pentaho.di.core.exception.KettleException;

public abstract class CarbonCSVBasedDimSurrogateKeyGen {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonCSVBasedDimSurrogateKeyGen.class.getName());
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
   * columnsInfo
   */
  protected ColumnsInfo columnsInfo;
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
   * Cache should be map only. because, multiple levels can map to same
   * database column. This case duplicate storage should be avoided.
   */
  private Map<String, Dictionary> dictionaryCaches;
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
      new HashMap<String, Int2ObjectMap<int[]>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  /**
   *
   */
  private Map<String, Map<ArrayWrapper, Integer>> hierCacheReverse =
      new HashMap<String, Map<ArrayWrapper, Integer>>(
          CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
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
   * @param columnsInfo ColumnsInfo With all the required details for surrogate key generation and
   *                    hierarchy entries.
   */
  public CarbonCSVBasedDimSurrogateKeyGen(ColumnsInfo columnsInfo) {
    this.columnsInfo = columnsInfo;

    setDimensionTables(columnsInfo.getDimColNames());
    setHierFileNames(columnsInfo.getHierTables());
  }

  /**
   * @param tuple         The string value whose surrogate key will be gennerated.
   * @param tabColumnName The K of dictionaryCaches Map, for example "tablename_columnname"
   */
  public Integer generateSurrogateKeys(String tuple, String tabColumnName) throws KettleException {
    Integer key = null;
    Dictionary dicCache = dictionaryCaches.get(tabColumnName);
    key = dicCache.getSurrogateKey(tuple);
    return key;
  }

  /**
   * @param tuple         The string value whose surrogate key will be gennerated.
   * @param tabColumnName The K of dictionaryCaches Map, for example "tablename_columnname"
   */
  public Integer generateSurrogateKeys(String tuple, String tabColumnName, String columnId)
      throws KettleException {
    Integer key = null;
    Dictionary dicCache = dictionaryCaches.get(tabColumnName);
    if (null == dicCache) {
      ColumnSchemaDetails columnSchemaDetails =
          this.columnsInfo.getColumnSchemaDetailsWrapper().get(columnId);
      if (columnSchemaDetails.isDirectDictionary()) {
        DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(columnSchemaDetails.getColumnType());
        key = directDictionaryGenerator.generateDirectSurrogateKey(tuple);
      }
    } else {
      key = dicCache.getSurrogateKey(tuple);
    }
    return key;
  }


  public Integer generateSurrogateKeysForTimeDims(String tuple, String columnName, int index,
      Object[] props) throws KettleException {
    Integer key = null;
    Dictionary dicCache = dictionaryCaches.get(columnName);
    key = dicCache.getSurrogateKey(tuple);
    if (key == null) {
      if (timDimMax[index] >= columnsInfo.getMaxKeys()[index]) {
        if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(tuple)) {
          tuple = null;
        }
        LOGGER.error("Invalid cardinality. Key size exceeded cardinality for: " + columnsInfo
                .getDimColNames()[index] + ": MemberValue: " + tuple);
        return -1;
      }
      timDimMax[index]++;
      Map<String, Integer> timeCache = timeDimCache.get(columnName);
      // Extract properties from tuple
      // Need to create a new surrogate key.
      key = getSurrogateFromStore(tuple, index, props);
      if (null != timeCache) {
        timeCache.put(tuple, key);
      }
    } else {
      return updateSurrogateToStore(tuple, columnName, index, key, props);
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
  protected abstract int updateSurrogateToStore(String value, String columnName, int index, int key,
      Object[] properties) throws KettleException;

  /**
   * generate the surroagate key for the measure values.
   *
   * @return
   * @throws KettleException
   */
  public abstract int getSurrogateForMeasure(String tuple, String columnName)
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
      hCache = new HashMap<ArrayWrapper, Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
      hierCacheReverse.put(hName, hCache);
    }

    return hCache;
  }

  private void setHierFileNames(Set<String> set) {
    hierInsertFileNames =
        new HashMap<String, String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (String s : set) {
      hierInsertFileNames.put(s, s + CarbonCommonConstants.HIERARCHY_FILE_EXTENSION);

      // fix hierStream is null issue
      getHCache(s);
      getHCacheReverse(s);
    }
  }

  private void setDimensionTables(String[] dimeFileNames) {
    int noOfPrimitiveDims = 0;
    List<String> dimFilesForPrimitives = new ArrayList<String>();
    List<Boolean> isDirectDictionary = new ArrayList<Boolean>();
    dictionaryCaches = new ConcurrentHashMap<String, Dictionary>();
    for (int i = 0; i < dimeFileNames.length; i++) {
      GenericDataType complexType = columnsInfo.getComplexTypesMap()
          .get(dimeFileNames[i].substring(columnsInfo.getTableName().length() + 1));
      if (complexType != null) {
        List<GenericDataType> primitiveChild = new ArrayList<GenericDataType>();
        complexType.getAllPrimitiveChildren(primitiveChild);
        for (GenericDataType eachPrimitive : primitiveChild) {
          dimFilesForPrimitives.add(
              columnsInfo.getTableName() + CarbonCommonConstants.UNDERSCORE + eachPrimitive
                  .getName());
          eachPrimitive.setSurrogateIndex(noOfPrimitiveDims);
          noOfPrimitiveDims++;
          ColumnSchemaDetails columnSchemaDetails =
              columnsInfo.getColumnSchemaDetailsWrapper().get(eachPrimitive.getColumnId());
          if (columnSchemaDetails.isDirectDictionary()) {
            isDirectDictionary.add(true);
          }
        }
      } else {
        dimFilesForPrimitives.add(dimeFileNames[i]);
        noOfPrimitiveDims++;
        isDirectDictionary.add(false);
      }
    }
    max = new int[noOfPrimitiveDims];
    for(int i = 0; i < isDirectDictionary.size(); i++) {
      if (isDirectDictionary.get(i)) {
        max[i] = Integer.MAX_VALUE;
      }
    }
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
   * @return Returns the dictionaryCaches.
   */
  public Map<String, Dictionary> getDictionaryCaches() {
    return dictionaryCaches;
  }

  /**
   * @param dictionaryCaches The dictionaryCaches to set.
   */
  public void setDictionaryCaches(Map<String, Dictionary> dictionaryCaches) {
    this.dictionaryCaches = dictionaryCaches;
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
