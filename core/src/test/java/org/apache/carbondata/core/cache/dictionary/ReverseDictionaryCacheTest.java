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

package org.apache.carbondata.core.cache.dictionary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import mockit.Mock;
import mockit.MockUp;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.reader.CarbonDictionaryColumnMetaChunk;
import org.apache.carbondata.core.util.CarbonProperties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class to test the functionality of reverse dictionary cache
 */
public class ReverseDictionaryCacheTest extends AbstractDictionaryCacheTest {

  protected Cache reverseDictionaryCache;

  @Before public void setUp() throws Exception {
    init();
    this.databaseName = props.getProperty("database", "testSchema");
    this.tableName = props.getProperty("tableName", "carbon");
    this.carbonStorePath = props.getProperty("storePath", "carbonStore");
    carbonTableIdentifier =
        new CarbonTableIdentifier(databaseName, tableName, UUID.randomUUID().toString());
    identifier = AbsoluteTableIdentifier.from(
        carbonStorePath + "/" + databaseName + "/" + tableName, carbonTableIdentifier);
    columnIdentifiers = new String[] { "name", "place" };
    deleteStorePath();
    prepareDataSet();
    createDictionaryCacheObject();
  }

  @After public void tearDown() throws Exception {
    carbonTableIdentifier = null;
    reverseDictionaryCache = null;
    identifier = null;
    deleteStorePath();
  }

  private void createDictionaryCacheObject() {
    // enable lru cache by setting cache size
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_MAX_DRIVER_LRU_CACHE_SIZE, "10");
    CacheProvider cacheProvider = CacheProvider.getInstance();
    cacheProvider.dropAllCache();
    reverseDictionaryCache =
        cacheProvider.createCache(CacheType.REVERSE_DICTIONARY);
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
    // get the reverse dictionary object
    Dictionary reverseDictionary =
        (Dictionary) reverseDictionaryCache.get(dictionaryColumnUniqueIdentifier);
    // reverse dictionary object should not be null
    assertTrue(null != reverseDictionary);
    // compare that surrogate key for data inserted and actual data should be same
    compareSurrogateKeyData(dataSet1, reverseDictionary);
    // decrement its access count
    reverseDictionary.clear();
    // remove keys from lru cache
    removeKeyFromLRUCache(reverseDictionaryCache);
  }

  @Test public void getAll() throws Exception {
    // delete store path
    deleteStorePath();
    String columnIdentifier = columnIdentifiers[0];
    // prepare dictionary writer and write data
    prepareWriterAndWriteData(dataSet1, columnIdentifier);
    List<DictionaryColumnUniqueIdentifier> dictionaryColumnUniqueIdentifiers = new ArrayList<>(3);
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
    // get the reverse dictionary object
    List<Dictionary> reverseDictionaryList =
        (List<Dictionary>) reverseDictionaryCache.getAll(dictionaryColumnUniqueIdentifiers);
    for (Dictionary reverseDictionary : reverseDictionaryList) {
      // reverse dictionary object should not be null
      assertTrue(null != reverseDictionary);
      // compare that surrogate key for data inserted and actual data should be same
      compareSurrogateKeyData(dataSet1, reverseDictionary);
      // decrement its access count
      reverseDictionary.clear();
    }
    // remove keys from lru cache
    removeKeyFromLRUCache(reverseDictionaryCache);
  }

  @Test public void getIfPresent() throws Exception {
    // delete store path
    deleteStorePath();
    String columnIdentifier = columnIdentifiers[0];
    // prepare dictionary writer and write data
    prepareWriterAndWriteData(dataSet1, columnIdentifier);
    // create dictionary column unique identifier instance
    DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
        createDictionaryColumnUniqueIdentifier(columnIdentifier);
    // get the reverse dictionary object
    Dictionary reverseDictionary =
        (Dictionary) reverseDictionaryCache.get(dictionaryColumnUniqueIdentifier);
    // reverse dictionary object should not be null
    assertTrue(null != reverseDictionary);
    reverseDictionary =
        (Dictionary) reverseDictionaryCache.getIfPresent(dictionaryColumnUniqueIdentifier);
    // compare that surrogate key for data inserted and actual data should be same
    compareSurrogateKeyData(dataSet1, reverseDictionary);
    // remove the identifier from lru cache
    reverseDictionaryCache.invalidate(dictionaryColumnUniqueIdentifier);
    // use getIfPresent API to get the reverse dictionary cache again
    reverseDictionary =
        (Dictionary) reverseDictionaryCache.getIfPresent(dictionaryColumnUniqueIdentifier);
    // as key has been removed from lru cache object should not be found
    assertTrue(null == reverseDictionary);
  }

  @Test public void testLRUCacheForMaxSize() throws Exception {
    // delete store path
    deleteStorePath();
    // mock get end offset method so that required size is greater than
    // available size limit
    new MockUp<CarbonDictionaryColumnMetaChunk>() {
      @Mock public long getEnd_offset() {
        return 123456789L;
      }
    };
    String columnIdentifier = columnIdentifiers[0];
    // prepare dictionary writer and write data
    prepareWriterAndWriteData(dataSet1, columnIdentifier);
    // create dictionary column unique identifier instance
    DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
        createDictionaryColumnUniqueIdentifier(columnIdentifier);
    // get the reverse dictionary object
    Dictionary reverseDictionary = null;
    try {
      reverseDictionary = (Dictionary) reverseDictionaryCache.get(dictionaryColumnUniqueIdentifier);
      fail("not throwing exception");
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
    }
    assertEquals(null, reverseDictionary);
  }

  @Test public void testLRUCacheForKeyDeletionAfterMaxSizeIsReached() throws Exception {
    // delete store path
    deleteStorePath();
    String columnIdentifier = columnIdentifiers[0];
    // prepare dictionary writer and write data
    prepareWriterAndWriteData(dataSet1, columnIdentifier);
    // create dictionary column unique identifier instance
    DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
        createDictionaryColumnUniqueIdentifier(columnIdentifier);
    // get the reverse dictionary object
    Dictionary reverseDictionary =
        (Dictionary) reverseDictionaryCache.get(dictionaryColumnUniqueIdentifier);
    // reverse dictionary object should not be null
    assertTrue(null != reverseDictionary);
    // decrement access count
    reverseDictionary.clear();
    // mock get end offset method so that required size is greater than
    // available size limit
    new MockUp<CarbonDictionaryColumnMetaChunk>() {
      @Mock public long getEnd_offset() {
        return 10445000L;
      }
    };
    columnIdentifier = columnIdentifiers[1];
    // prepare dictionary writer and write data
    prepareWriterAndWriteData(dataSet1, columnIdentifier);
    // create dictionary column unique identifier instance
    dictionaryColumnUniqueIdentifier = createDictionaryColumnUniqueIdentifier(columnIdentifier);
    // get the reverse dictionary object
    // lru cache should delete the existing key and empty the size for new key addition
    reverseDictionary = (Dictionary) reverseDictionaryCache.get(dictionaryColumnUniqueIdentifier);
    // reverse dictionary object should not be null
    assertTrue(null != reverseDictionary);
    // remove keys from lru cache
    removeKeyFromLRUCache(reverseDictionaryCache);
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
    // get the reverse dictionary object
    Dictionary reverseDictionary =
        (Dictionary) reverseDictionaryCache.get(dictionaryColumnUniqueIdentifier);
    // reverse dictionary object should not be null
    assertTrue(null != reverseDictionary);
    // prepare dictionary writer and write data
    prepareWriterAndWriteData(dataSet2, columnIdentifier);
    // get the reverse dictionary object
    reverseDictionary = (Dictionary) reverseDictionaryCache.get(dictionaryColumnUniqueIdentifier);
    // reverse dictionary object should not be null
    assertTrue(null != reverseDictionary);
    // prepare expected result
    List<String> expected = new ArrayList<>(2);
    expected.addAll(dataSet1);
    expected.addAll(dataSet2);
    // compare the data
    compareSurrogateKeyData(expected, reverseDictionary);
    // decrement access count
    reverseDictionary.clear();
    // remove keys from lru cache
    removeKeyFromLRUCache(reverseDictionaryCache);
  }

  /**
   * This method will compare the actual data with expected data
   *
   * @param data
   * @param reverseDictionary
   */
  private void compareSurrogateKeyData(List<String> data, Dictionary reverseDictionary) {
    int surrogateKey = 0;
    for (int i = 0; i < data.size(); i++) {
      surrogateKey++;
      assertTrue(surrogateKey == reverseDictionary.getSurrogateKey(data.get(i)));
    }
  }
  protected DictionaryColumnUniqueIdentifier createDictionaryColumnUniqueIdentifier(
	      String columnId) {
	    ColumnIdentifier columnIdentifier = new ColumnIdentifier(columnId, null, DataTypes.DOUBLE);
    return new DictionaryColumnUniqueIdentifier(identifier, columnIdentifier);
	  }
}