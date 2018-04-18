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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriter;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriterImpl;

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
    carbonTableIdentifier =
        new CarbonTableIdentifier(databaseName, tableName, UUID.randomUUID().toString());
    identifier =
        AbsoluteTableIdentifier.from(carbonStorePath + "/" + databaseName + "/" + tableName,
            carbonTableIdentifier);
    columnIdentifiers = new String[] { "name", "place" };
    deleteStorePath();
    prepareDataSet();
    createDictionaryCacheObject();
  }

  @After public void tearDown() throws Exception {
    carbonTableIdentifier = null;
    identifier = null;
    forwardDictionaryCache = null;
    deleteStorePath();
  }

  private void createDictionaryCacheObject() {
    // enable lru cache by setting cache size
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_MAX_DRIVER_LRU_CACHE_SIZE, "10");
    CacheProvider cacheProvider = CacheProvider.getInstance();
    forwardDictionaryCache =
        cacheProvider.createCache(CacheType.FORWARD_DICTIONARY);
  }

  @Test public void get() throws Exception {
    // delete store path
    deleteStorePath();
    String columnIdentifier = columnIdentifiers[0];
    // prepare dictionary writer and write data
    prepareWriterAndWriteData(dataSet1, columnIdentifier);
    // write sort index file
    writeSortIndexFile(dataSet1, columnIdentifier);
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
    // write sort index file
    writeSortIndexFile(dataSet1, columnIdentifier);
    List<DictionaryColumnUniqueIdentifier> dictionaryColumnUniqueIdentifiers = new ArrayList<>(3);
    // create dictionary column unique identifier instance
    DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
        createDictionaryColumnUniqueIdentifier(columnIdentifier);
    dictionaryColumnUniqueIdentifiers.add(dictionaryColumnUniqueIdentifier);
    // prepare dictionary writer and write data
    columnIdentifier = columnIdentifiers[1];
    prepareWriterAndWriteData(dataSet1, columnIdentifier);
    // write sort index file
    writeSortIndexFile(dataSet1, columnIdentifier);
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
    // write sort index file
    writeSortIndexFile(dataSet1, columnIdentifier);
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
    // write sort index file
    List<String> allDictionaryChunkList = new ArrayList<>(6);
    allDictionaryChunkList.addAll(dataSet1);
    allDictionaryChunkList.addAll(dataSet2);
    writeSortIndexFile(allDictionaryChunkList, columnIdentifier);
    // get the forward dictionary object
    forwardDictionary = (Dictionary) forwardDictionaryCache.get(dictionaryColumnUniqueIdentifier);
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

  @Test public void testSortedAndInvertedSortIndex() throws Exception {
    // delete store path
    deleteStorePath();
    String columnIdentifier = columnIdentifiers[0];
    // prepare dictionary writer and write data
    prepareWriterAndWriteData(dataSet3, columnIdentifier);
    // write sort index file
    writeSortIndexFile(dataSet3, columnIdentifier);
    // create dictionary column unique identifier instance
    DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
        createDictionaryColumnUniqueIdentifier(columnIdentifier);
    // get the forward dictionary object
    Dictionary forwardDictionary =
        (Dictionary) forwardDictionaryCache.get(dictionaryColumnUniqueIdentifier);
    // forward dictionary object should not be null
    assertTrue(null != forwardDictionary);
    // compare that surrogate key for data inserted and actual data should be same
    compareSurrogateKeyData(dataSet3, forwardDictionary);
    // compare the surrogate keys for given dictionary values
    compareDictionaryValueFromSortedIndex(dataSet3, forwardDictionary);
    // decrement its access count
    forwardDictionary.clear();
    // remove keys from lru cache
    removeKeyFromLRUCache(forwardDictionaryCache);
  }

  /**
   * This method will prepare the sort index data from the given data and write
   * it to a sort index file
   *
   * @param data
   * @param columnId
   * @throws IOException
   */
  private void writeSortIndexFile(List<String> data, String columnId) throws IOException {
	ColumnIdentifier columnIdentifier = new ColumnIdentifier(columnId, null, null);
    DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
        new DictionaryColumnUniqueIdentifier(identifier, columnIdentifier,
            columnIdentifier.getDataType());
    Map<String, Integer> dataToSurrogateKeyMap = new HashMap<>(data.size());
    int surrogateKey = 0;
    List<Integer> invertedIndexList = new ArrayList<>(data.size());
    for (int i = 0; i < data.size(); i++) {
      dataToSurrogateKeyMap.put(data.get(i), ++surrogateKey);
    }
    List<String> sortedKeyList = new ArrayList<>(dataToSurrogateKeyMap.keySet());
    Collections.sort(sortedKeyList);
    List<Integer> sortedIndexList = new ArrayList<>(data.size());
    int[] invertedIndexArray = new int[sortedKeyList.size()];
    for (int i = 0; i < sortedKeyList.size(); i++) {
      Integer key = dataToSurrogateKeyMap.get(sortedKeyList.get(i));
      sortedIndexList.add(key);
      invertedIndexArray[--key] = i + 1;
    }
    for (int i = 0; i < invertedIndexArray.length; i++) {
      invertedIndexList.add(invertedIndexArray[i]);
    }
    CarbonDictionarySortIndexWriter dictionarySortIndexWriter =
        new CarbonDictionarySortIndexWriterImpl(dictionaryColumnUniqueIdentifier);
    try {
      dictionarySortIndexWriter.writeSortIndex(sortedIndexList);
      dictionarySortIndexWriter.writeInvertedSortIndex(invertedIndexList);
    } finally {
      dictionarySortIndexWriter.close();
    }
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

  /**
   * This method will get the dictionary value from sorted index and compare with the data set
   *
   * @param data
   * @param forwardDictionary
   */
  private void compareDictionaryValueFromSortedIndex(List<String> data,
      Dictionary forwardDictionary) {
    int expectedSurrogateKey = 0;
    for (int i = 0; i < data.size(); i++) {
      expectedSurrogateKey++;
      String expectedDictionaryValue = data.get(i);
      int actualSurrogateKey = forwardDictionary.getSurrogateKey(expectedDictionaryValue);
      assertTrue(actualSurrogateKey == expectedSurrogateKey);
      int sortedIndex = forwardDictionary.getSortedIndex(actualSurrogateKey);
      String actualDictionaryValue =
          forwardDictionary.getDictionaryValueFromSortedIndex(sortedIndex);
      assertTrue(expectedDictionaryValue.equals(actualDictionaryValue));
    }
  }
}