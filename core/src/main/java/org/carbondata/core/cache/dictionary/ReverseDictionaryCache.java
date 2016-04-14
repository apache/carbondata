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

package org.carbondata.core.cache.dictionary;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.cache.CacheType;
import org.carbondata.core.cache.CarbonLRUCache;
import org.carbondata.core.util.CarbonCoreLogEvent;

/**
 * This class implements methods to create dictionary cache which will hold
 * dictionary chunks for look up of surrogate keys and values
 */
public class ReverseDictionaryCache<K extends DictionaryColumnUniqueIdentifier,
        V extends Dictionary>
        extends AbstractDictionaryCache<K, V> {

    /**
     * Attribute for Carbon LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(ForwardDictionaryCache.class.getName());

    /**
     * @param carbonStorePath
     * @param carbonLRUCache
     */
    public ReverseDictionaryCache(String carbonStorePath, CarbonLRUCache carbonLRUCache) {
        super(carbonStorePath, carbonLRUCache);
    }

    /**
     * This method will get the value for the given key. If value does not exist
     * for the given key, it will check and load the value.
     *
     * @param dictionaryColumnUniqueIdentifier unique identifier which contains dbName,
     *                                         tableName and columnIdentifier
     * @return
     */
    @Override public Dictionary get(
            DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) {
        return getDictionary(dictionaryColumnUniqueIdentifier);
    }

    /**
     * This method will return a list of values for the given list of keys.
     * For each key, this method will check and load the data if required.
     *
     * @param dictionaryColumnUniqueIdentifiers unique identifier which contains dbName,
     *                                          tableName and columnIdentifier
     * @return
     */
    @Override public List<Dictionary> getAll(
            List<DictionaryColumnUniqueIdentifier> dictionaryColumnUniqueIdentifiers) {
        final List<Dictionary> reverseDictionaryObjectList =
                new ArrayList<Dictionary>(dictionaryColumnUniqueIdentifiers.size());
        ExecutorService executorService = Executors.newFixedThreadPool(FIXED_THREAD_POOL_SIZE);
        for (final DictionaryColumnUniqueIdentifier oneUniqueIdentifier :
                dictionaryColumnUniqueIdentifiers) {
            executorService.submit(new Runnable() {
                @Override public void run() {
                    reverseDictionaryObjectList.add(getDictionary(oneUniqueIdentifier));
                }
            });
        }
        try {
            executorService.shutdown();
            executorService.awaitTermination(2, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                    "Error loading the dictionary: " + e.getMessage());
        }
        return reverseDictionaryObjectList;
    }

    /**
     * This method will return the value for the given key. It will not check and load
     * the data for the given key
     *
     * @param dictionaryColumnUniqueIdentifier unique identifier which contains dbName,
     *                                         tableName and columnIdentifier
     * @return
     */
    @Override public Dictionary getIfPresent(
            DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) {
        Dictionary reverseDictionary = null;
        ColumnReverseDictionaryInfo columnReverseDictionaryInfo =
                (ColumnReverseDictionaryInfo) carbonLRUCache
                        .get(getLruCacheKey(dictionaryColumnUniqueIdentifier.getColumnIdentifier(),
                                CacheType.REVERSE_DICTIONARY));
        if (null != columnReverseDictionaryInfo) {
            reverseDictionary = new ReverseDictionary(columnReverseDictionaryInfo);
            incrementDictionaryAccessCount(columnReverseDictionaryInfo);
        }
        return reverseDictionary;
    }

    /**
     * This method will remove the cache for a given key
     *
     * @param dictionaryColumnUniqueIdentifier unique identifier which contains dbName,
     *                                         tableName and columnIdentifier
     */
    @Override public void invalidate(
            DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) {
        carbonLRUCache.remove(getLruCacheKey(dictionaryColumnUniqueIdentifier.getColumnIdentifier(),
                CacheType.REVERSE_DICTIONARY));
    }

    /**
     * This method will get the value for the given key. If value does not exist
     * for the given key, it will check and load the value.
     *
     * @param dictionaryColumnUniqueIdentifier unique identifier which contains dbName,
     *                                         tableName and columnIdentifier
     * @return dictionary
     */
    private Dictionary getDictionary(
            DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) {
        Dictionary reverseDictionary = null;
        // create column dictionary info object only if dictionary and its
        // metadata file exists for a given column identifier
        if (isFileExistsForGivenColumn(dictionaryColumnUniqueIdentifier)) {
            String columnIdentifier = dictionaryColumnUniqueIdentifier.getColumnIdentifier();
            ColumnReverseDictionaryInfo columnReverseDictionaryInfo =
                    getColumnReverseDictionaryInfo(dictionaryColumnUniqueIdentifier,
                            columnIdentifier);
            if (checkAndLoadDictionaryData(dictionaryColumnUniqueIdentifier,
                    columnReverseDictionaryInfo,
                    getLruCacheKey(dictionaryColumnUniqueIdentifier.getColumnIdentifier(),
                            CacheType.REVERSE_DICTIONARY))) {
                reverseDictionary = new ReverseDictionary(columnReverseDictionaryInfo);
            }
        }
        return reverseDictionary;
    }

    /**
     * This method will check and create columnReverseDictionaryInfo object for the given column
     *
     * @param dictionaryColumnUniqueIdentifier
     * @param columnIdentifier
     * @return
     */
    private ColumnReverseDictionaryInfo getColumnReverseDictionaryInfo(
            DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier,
            String columnIdentifier) {
        ColumnReverseDictionaryInfo columnReverseDictionaryInfo =
                (ColumnReverseDictionaryInfo) carbonLRUCache
                        .get(getLruCacheKey(columnIdentifier, CacheType.REVERSE_DICTIONARY));
        if (null == columnReverseDictionaryInfo) {
            synchronized (dictionaryColumnUniqueIdentifier) {
                columnReverseDictionaryInfo = (ColumnReverseDictionaryInfo) carbonLRUCache
                        .get(getLruCacheKey(columnIdentifier, CacheType.REVERSE_DICTIONARY));
                if (null == columnReverseDictionaryInfo) {
                    columnReverseDictionaryInfo = new ColumnReverseDictionaryInfo();
                }
            }
        }
        return columnReverseDictionaryInfo;
    }
}
