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

package org.apache.carbondata.processing.surrogatekeysgenerator.dbbased;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.processing.schema.metadata.ColumnsInfo;

import org.pentaho.di.core.exception.KettleException;

public abstract class CarbonDimSurrogateKeyGen {
  /**
   * HIERARCHY_FILE_EXTENSION
   */
  protected static final String HIERARCHY_FILE_EXTENSION = ".hierarchy";
  /**
   * Comment for <code>LOGGER</code>
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonDimSurrogateKeyGen.class.getName());
  /**
   * Cache should be map only. because, multiple levels can map to same
   * database column. This case duplicate storage should be avoided.
   */
  protected Map<String, Dictionary> dictionaryCaches;
  /**
   * dimsFiles
   */
  protected String[] dimsFiles;
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
   * hierCache
   */
  protected Map<String, Map<IntArrayWrapper, Boolean>> hierCache =
      new HashMap<String, Map<IntArrayWrapper, Boolean>>(
          CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  /**
   * columnsInfo
   */
  protected ColumnsInfo columnsInfo;
  /**
   * rwLock
   */
  private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  /**
   * wLock
   */
  private Lock wLock = rwLock.writeLock();
  /**
   * rwLock2
   */
  private ReentrantReadWriteLock rwLock2 = new ReentrantReadWriteLock();
  /**
   * wLock2
   */
  protected Lock wLock2 = rwLock2.writeLock();

  /**
   * @param columnsInfo ColumnsInfo With all the required details for surrogate key generation and
   *                    hierarchy entries.
   */
  public CarbonDimSurrogateKeyGen(ColumnsInfo columnsInfo) {
    this.columnsInfo = columnsInfo;

    setDimensionTables(columnsInfo.getDimColNames());
    setHierFileNames(columnsInfo.getHierTables());
  }

  public Object[] generateSurrogateKeys(Object[] tuple, Object[] out,
      List<Integer> timeOrdinalColValues) throws KettleException {
    boolean[] dimsPresent = columnsInfo.getDimsPresent();
    int[] dims = columnsInfo.getDims();

    String[] dimColNames = columnsInfo.getDimColNames();
    int k = 0;
    for (int i = 0; i < dims.length; i++) {
      Integer key = null;
      Object value = null;
      if (columnsInfo.isAggregateLoad()) {
        value = tuple[i];
      } else {
        if (dimsPresent[i]) {
          value = tuple[k];

        } else {
          continue;
        }
      }

      if (value == null) {
        value = "null";
      }
      String dimS = value.toString().trim();
      // getting values from local cache
      Dictionary dicCache = dictionaryCaches.get(dimColNames[i]);

      key = dicCache.getSurrogateKey(dimS);

      // Update the generated key in output.
      out[k] = key;
      k++;
    }
    return out;
  }

  private Object[] getProperties(Object[] tuple, List<Integer> timeOrdinalColValues, int i) {
    Object[] props = new Object[0];
    if (columnsInfo.getTimDimIndex() != -1 && i >= columnsInfo.getTimDimIndex() && i < columnsInfo
        .getTimDimIndexEnd()) {
      //For time dimensions only ordinal columns is considered.
      int ordinalIndx = columnsInfo.getTimeOrdinalIndices()[i - columnsInfo.getTimDimIndexEnd()];
      if (ordinalIndx != -1) {
        props = new Object[1];
        props[0] = timeOrdinalColValues.get(ordinalIndx);
      }
    } else {
      if (columnsInfo.getPropIndx() != null) {
        int[] pIndices = columnsInfo.getPropIndx()[i];
        props = new Object[pIndices.length];
        for (int j = 0; j < props.length; j++) {
          props[j] = tuple[pIndices[j]];
        }
      }
    }
    return props;
  }

  public void checkHierExists(int[] val, String hier) throws KettleException {
    IntArrayWrapper wrapper = new IntArrayWrapper(val, 0);
    Map<IntArrayWrapper, Boolean> hCache = hierCache.get(hier);
    Boolean b = hCache.get(wrapper);
    if (b != null) {
      return;
    }

    wLock2.lock();
    try {
      if (null == hCache.get(wrapper)) {
        getHierFromStore(val, hier);
        // Store in cache
        hCache.put(wrapper, true);
      }
    } finally {
      wLock2.unlock();
    }
  }

  public void close() throws Exception {
    if (null != connection) {
      connection.close();
    }
    hierCache.clear();
  }

  public abstract void writeHeirDataToFileAndCloseStreams() throws KettleException;

  /**
   * Search entry and insert if not found in store.
   *
   * @param val
   * @param hier
   * @return
   * @throws KeyGenException
   * @throws KettleException
   */
  protected abstract byte[] getHierFromStore(int[] val, String hier) throws KettleException;

  /**
   * Search entry and insert if not found in store.
   *
   * @param val
   * @param hier
   * @return
   * @throws KeyGenException
   * @throws KettleException
   */
  protected abstract byte[] getNormalizedHierFromStore(int[] val, String hier,
      HierarchyValueWriter hierWriter) throws KettleException;

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

  private Map<IntArrayWrapper, Boolean> getHCache(String hName) {
    Map<IntArrayWrapper, Boolean> hCache = hierCache.get(hName);
    if (hCache == null) {
      hCache = new HashMap<IntArrayWrapper, Boolean>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
      hierCache.put(hName, hCache);
    }

    return hCache;
  }

  private void setHierFileNames(Set<String> set) {
    hierInsertFileNames =
        new HashMap<String, String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (String s : set) {
      hierInsertFileNames.put(s, s + HIERARCHY_FILE_EXTENSION);

      // fix hierStream is null issue
      getHCache(s);
    }
  }

  private void setDimensionTables(String[] dimeFileNames) {
    this.dimsFiles = dimeFileNames;
    max = new int[dimeFileNames.length];
    createRespectiveDimFilesForDimTables();
  }

  private void createRespectiveDimFilesForDimTables() {
    int dimCount = this.dimsFiles.length;
    dimInsertFileNames = new String[dimCount];
    System.arraycopy(dimsFiles, 0, dimInsertFileNames, 0, dimCount);
  }

  public void checkNormalizedHierExists(int[] val, String hier, HierarchyValueWriter hierWriter)
      throws KettleException {
    IntArrayWrapper wrapper = new IntArrayWrapper(val, 0);
    Map<IntArrayWrapper, Boolean> hCache = hierCache.get(hier);

    Boolean b = hCache.get(wrapper);

    if (b != null) {
      return;
    } else {
      wLock2.lock();
      try {
        getNormalizedHierFromStore(val, hier, hierWriter);
      } finally {
        wLock2.unlock();
      }
    }
  }

}
