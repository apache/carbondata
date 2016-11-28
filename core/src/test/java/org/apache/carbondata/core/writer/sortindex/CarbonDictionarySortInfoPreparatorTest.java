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
package org.apache.carbondata.core.writer.sortindex;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryChunksWrapper;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.util.CarbonUtilException;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * The class tests the CarbonDictionarySortInfoPreparator class that prepares the column sort info ie sortIndex
 * and inverted sort index info
 */
public class CarbonDictionarySortInfoPreparatorTest {

  private static CarbonDictionarySortInfoPreparator carbonDictionarySortInfoPreparator = null;

  @BeforeClass public static void setUp() {
    carbonDictionarySortInfoPreparator = new CarbonDictionarySortInfoPreparator();
  }

  /**
   * Tests the getDictionarySortInfo method
   *
   * @throws CarbonUtilException
   */
  @Test public void testGetDictionarySortInfo() throws CarbonUtilException {

    List<String> newDistinctValues = new ArrayList<>();
    newDistinctValues.add("abc");
    newDistinctValues.add("xyz");
    Dictionary dictionary = new Dictionary() {
      @Override public int getSortedIndex(int surrogateKey) {
        return 1;
      }

      @Override public int getSurrogateKey(String value) {
        return 1;
      }

      @Override public String getDictionaryValueForKey(int surrogateKey) {
        return "";
      }

      @Override public int getSurrogateKey(byte[] value) {
        return 1;
      }

      @Override public void clear() {
      }

      @Override public String getDictionaryValueFromSortedIndex(int sortedIndex) {
        return "";
      }

      @Override public DictionaryChunksWrapper getDictionaryChunks() {
        List<byte[]> data = new ArrayList<>();
        data.add(new byte[] { 1, 2 });
        List<List<byte[]>> dictionaryChunks = new ArrayList<>();
        dictionaryChunks.add(data);
        return new DictionaryChunksWrapper(dictionaryChunks);
      }

    };
    CarbonDictionarySortInfo carbonDictionarySortInfo = carbonDictionarySortInfoPreparator
        .getDictionarySortInfo(newDistinctValues, dictionary, DataType.ARRAY);
    int expectedGetSortIndexValue = 1;
    int expectedGetSortInvertedIndexLength = 3;
    int actualGetSortIndexValue = carbonDictionarySortInfo.getSortIndex().get(0);
    int actualGetSortInvertedIndexLength = carbonDictionarySortInfo.getSortIndexInverted().size();
    assertTrue(actualGetSortIndexValue == expectedGetSortIndexValue);
    assertTrue(actualGetSortInvertedIndexLength == expectedGetSortInvertedIndexLength);
  }

  /**
   * Tests getDictionarySortInfo when dictionary is null
   *
   * @throws CarbonUtilException
   */
  @Test public void testGetDictionarySortInfoDictionaryNullCase() throws CarbonUtilException {

    List<String> newDistinctValues = new ArrayList<>();
    newDistinctValues.add("abc");
    newDistinctValues.add("xyz");
    Dictionary dictionary = null;
    CarbonDictionarySortInfo carbonDictionarySortInfo = carbonDictionarySortInfoPreparator
        .getDictionarySortInfo(newDistinctValues, dictionary, DataType.ARRAY);
    int expectedGetSortIndexValue = 1;
    int expectedGetSortInvertedIndexLength = 2;
    int actualGetSortIndexValue = carbonDictionarySortInfo.getSortIndex().get(0);
    int actualGetSortInvertedIndexLength = carbonDictionarySortInfo.getSortIndexInverted().size();
    assertTrue(actualGetSortIndexValue == expectedGetSortIndexValue);
    assertTrue(actualGetSortInvertedIndexLength == expectedGetSortInvertedIndexLength);
  }

}
