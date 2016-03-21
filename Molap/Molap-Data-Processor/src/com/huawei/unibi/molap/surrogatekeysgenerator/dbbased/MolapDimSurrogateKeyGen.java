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

package com.huawei.unibi.molap.surrogatekeysgenerator.dbbased;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.keygenerator.KeyGenException;
import com.huawei.unibi.molap.schema.metadata.MolapInfo;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import org.pentaho.di.core.exception.KettleException;

public abstract class MolapDimSurrogateKeyGen {
    /**
     * HIERARCHY_FILE_EXTENSION
     */
    protected static final String HIERARCHY_FILE_EXTENSION = ".hierarchy";
    /**
     * Comment for <code>LOGGER</code>
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(MolapDimSurrogateKeyGen.class.getName());
    /**
     * Cache should be map only. because, multiple levels can map to same
     * database column. This case duplicate storage should be avoided.
     */
    protected Map<String, Map<String, Integer>> memberCache;
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
                    MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    /**
     * molapInfo
     */
    protected MolapInfo molapInfo;
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
     * @param molapInfo MolapInfo With all the required details for surrogate key generation and
     *                  hierarchy entries.
     */
    public MolapDimSurrogateKeyGen(MolapInfo molapInfo) {
        this.molapInfo = molapInfo;

        setDimensionTables(molapInfo.getDimColNames());
        setHierFileNames(molapInfo.getHierTables());
    }

    public int[] generateSurrogateKeys(String[] timeTuples, int[] out, int[] columnIndex,
            List<Integer> timeOrdinalColValues) throws KettleException {
        Integer key = null;
        for (int i = 0; i < columnIndex.length; i++) {
            Map<String, Integer> cache =
                    memberCache.get(molapInfo.getDimColNames()[columnIndex[i]]);

            key = cache.get(timeTuples[i]);
            if (key == null) {
                // Validate the key against cardinality bits
                if (max[i] >= molapInfo.getMaxKeys()[i]) {
                    throw new KettleException(new KeyGenException(
                            "Invalid cardinality. Key size exceeded cardinality for: " + molapInfo
                                    .getDimColNames()[i]));
                }
                // Extract properties from tuple
                Object[] props = getProperties(new Object[0], timeOrdinalColValues, columnIndex[i]);

                // Need to create a new surrogate key.
                key = getSurrogateFromStore(timeTuples[i], columnIndex[i], props);
                cache.put(timeTuples[i], key);
            }
            out[i] = key;
        }
        return out;
    }

    public Integer generateSurrogateKeys(String tuples, String columnNames, int index,
            Object[] props) throws KettleException {
        Integer key = null;
        Map<String, Integer> cache = memberCache.get(columnNames);

        key = cache.get(tuples);
        if (key == null) {
            if (max[index] >= molapInfo.getMaxKeys()[index]) {
                LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Invalid cardinality. Key size exceeded cardinality for: " + molapInfo
                                .getDimColNames()[index]);
                return -1;
            }
            // Extract properties from tuple
            // Need to create a new surrogate key.
            key = getSurrogateFromStore(tuples, index, props);
            cache.put(tuples, key);
        }
        return key;
    }

    public Object[] generateSurrogateKeys(Object[] tuple, Object[] out,
            List<Integer> timeOrdinalColValues) throws KettleException {
        //Modified for Normalized hierarchy AR-UniBI-OLAP-003
        boolean[] dimsPresent = molapInfo.getDimsPresent();
        int[] dims = molapInfo.getDims();

        String[] dimColNames = molapInfo.getDimColNames();
        int k = 0;
        for (int i = 0; i < dims.length; i++) {
            Integer key = null;
            Object value = null;
            if (molapInfo.isAggregateLoad()) {
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
            Map<String, Integer> cache = memberCache.get(dimColNames[i]);

            key = cache.get(dimS);
            // here we added this null check
            if (key == null) {
                // Validate the key against cardinality bits
                // Commenting for testing if this required will be enabled
                if (max[i] >= molapInfo.getMaxKeys()[i]) {
                    out = new Object[0];
                    return out;
                }
                try {
                    // Extract properties from tuple
                    wLock.lock();
                    key = cache.get(dimS);
                    if (null == key) {
                        Object[] props = getProperties(tuple, timeOrdinalColValues, i);
                        key = getSurrogateFromStore(dimS, i, props);
                        cache.put(dimS, key);
                    }
                } finally {
                    wLock.unlock();
                }

            }
            // Update the generated key in output.
            out[k] = key;
            k++;
        }
        return out;
    }

    private Object[] getProperties(Object[] tuple, List<Integer> timeOrdinalColValues, int i) {
        Object[] props = new Object[0];
        if (molapInfo.getTimDimIndex() != -1 && i >= molapInfo.getTimDimIndex() && i < molapInfo
                .getTimDimIndexEnd()) {
            //For time dimensions only ordinal columns is considered.
            int ordinalIndx = molapInfo.getTimeOrdinalIndices()[i - molapInfo.getTimDimIndexEnd()];
            if (ordinalIndx != -1) {
                props = new Object[1];
                props[0] = timeOrdinalColValues.get(ordinalIndx);
            }
        } else {
            if (molapInfo.getPropIndx() != null) {
                int[] pIndices = molapInfo.getPropIndx()[i];
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
            hCache = new HashMap<IntArrayWrapper, Boolean>(
                    MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
            hierCache.put(hName, hCache);
        }

        return hCache;
    }

    private void setHierFileNames(Set<String> set) {
        hierInsertFileNames =
                new HashMap<String, String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (String s : set) {
            hierInsertFileNames.put(s, s + HIERARCHY_FILE_EXTENSION);

            // fix hierStream is null issue
            getHCache(s);
        }
    }

    private void setDimensionTables(String[] dimeFileNames) {
        this.dimsFiles = dimeFileNames;
        max = new int[dimeFileNames.length];

        memberCache = new HashMap<String, Map<String, Integer>>(
                MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (int i = 0; i < dimeFileNames.length; i++) {
            memberCache.put(dimeFileNames[i],
                    new HashMap<String, Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE));
        }

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
