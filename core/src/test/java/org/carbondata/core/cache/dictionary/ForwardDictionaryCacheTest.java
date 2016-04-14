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

import org.carbondata.core.cache.Cache;
import org.carbondata.core.cache.CacheProvider;
import org.carbondata.core.cache.CacheType;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Test class to test the functionality of forward dictionary cache
 */
public class ForwardDictionaryCacheTest extends AbstractDictionaryCacheTest {

    private Cache forwardDictionaryCache;

    @Before public void setUp() throws Exception {
        init();
        this.databaseName = props.getProperty("database", "testSchema");
        this.tableName = props.getProperty("tableName", "carbon");
        this.carbonStorePath = props.getProperty("storePath", "carbonStore");
        carbonTableIdentifier = new CarbonTableIdentifier(databaseName, tableName);
        columnIdentifiers = new String[] { "name", "place" };
        deleteStorePath();
        prepareDataSet();
        createDictionaryCacheObject();
    }

    @After public void tearDown() throws Exception {
        carbonTableIdentifier = null;
        forwardDictionaryCache = null;
        deleteStorePath();
    }

    private void createDictionaryCacheObject() {
        // enable lru cache by setting cache size
        CarbonProperties.getInstance()
                .addProperty(CarbonCommonConstants.CARBON_MAX_LEVEL_CACHE_SIZE, "10");
        CacheProvider cacheProvider = CacheProvider.getInstance();
        forwardDictionaryCache =
                cacheProvider.createCache(CacheType.FORWARD_DICTIONARY, this.carbonStorePath);
    }

    @Test public void get() throws Exception {
        // delete store path
        deleteStorePath();
        String columnIdentifier = columnIdentifiers[0];
        // prepare dictionary writer and write data
        prepareWriterAndWriteData(dataSet1, columnIdentifier);
        // create dictionary column unique identifier instance
        DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
                createDictionaryColumnUniqueIdentifier(columnIdentifier);
        // get the forward dictionary object
        Dictionary forwardDictionary =
                (Dictionary) forwardDictionaryCache.get(dictionaryColumnUniqueIdentifier);
        // forward dictionary object should not be null
        assertTrue(null != forwardDictionary);
        // compare that surrogate key for data inserted and actual data should be same
        compareSurrogateKeyData(dataSet1, forwardDictionary);
        // decrement its access count
        forwardDictionary.clear();
        // remove keys from lru cache
        removeKeyFromLRUCache(forwardDictionaryCache);
    }

    @Test public void getAll() throws Exception {
        // delete store path
        deleteStorePath();
        String columnIdentifier = columnIdentifiers[0];
        // prepare dictionary writer and write data
        prepareWriterAndWriteData(dataSet1, columnIdentifier);
        List<DictionaryColumnUniqueIdentifier> dictionaryColumnUniqueIdentifiers =
                new ArrayList<>(3);
        // create dictionary column unique identifier instance
        DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
                createDictionaryColumnUniqueIdentifier(columnIdentifier);
        dictionaryColumnUniqueIdentifiers.add(dictionaryColumnUniqueIdentifier);
        // prepare dictionary writer and write data
        columnIdentifier = columnIdentifiers[1];
        prepareWriterAndWriteData(dataSet1, columnIdentifier);
        // create dictionary column unique identifier instance
        dictionaryColumnUniqueIdentifier = createDictionaryColumnUniqueIdentifier(columnIdentifier);
        dictionaryColumnUniqueIdentifiers.add(dictionaryColumnUniqueIdentifier);
        // get the forward dictionary object
        List<Dictionary> forwardDictionaryList =
                (List<Dictionary>) forwardDictionaryCache.getAll(dictionaryColumnUniqueIdentifiers);
        for (Dictionary forwardDictionary : forwardDictionaryList) {
            // forward dictionary object should not be null
            assertTrue(null != forwardDictionary);
            // compare that surrogate key for data inserted and actual data should be same
            compareSurrogateKeyData(dataSet1, forwardDictionary);
            // decrement its access count
            forwardDictionary.clear();
        }
        // remove keys from lru cache
        removeKeyFromLRUCache(forwardDictionaryCache);
    }

    @Test public void testMultipleDictionaryChunks() throws Exception {
        // delete store path
        deleteStorePath();
        String columnIdentifier = columnIdentifiers[0];
        // prepare dictionary writer and write data
        prepareWriterAndWriteData(dataSet1, columnIdentifier);
        // create dictionary column unique identifier instance
        DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
                createDictionaryColumnUniqueIdentifier(columnIdentifier);
        // get the forward dictionary object
        Dictionary forwardDictionary =
                (Dictionary) forwardDictionaryCache.get(dictionaryColumnUniqueIdentifier);
        // forward dictionary object should not be null
        assertTrue(null != forwardDictionary);
        // prepare dictionary writer and write data
        prepareWriterAndWriteData(dataSet2, columnIdentifier);
        // get the forward dictionary object
        forwardDictionary =
                (Dictionary) forwardDictionaryCache.get(dictionaryColumnUniqueIdentifier);
        // forward dictionary object should not be null
        assertTrue(null != forwardDictionary);
        // prepare expected result
        List<String> expected = new ArrayList<>(2);
        expected.addAll(dataSet1);
        expected.addAll(dataSet2);
        // compare the data
        compareSurrogateKeyData(expected, forwardDictionary);
        // decrement access count
        forwardDictionary.clear();
        // remove keys from lru cache
        removeKeyFromLRUCache(forwardDictionaryCache);
    }

    /**
     * This method will compare the actual data with expected data
     *
     * @param data
     * @param forwardDictionary
     */
    private void compareSurrogateKeyData(List<String> data, Dictionary forwardDictionary) {
        int surrogateKey = 0;
        for (int i = 0; i < data.size(); i++) {
            surrogateKey++;
            String dictionaryValue = forwardDictionary.getDictionaryValueForKey(surrogateKey);
            assertTrue(data.get(i).equals(dictionaryValue));
        }
    }
}