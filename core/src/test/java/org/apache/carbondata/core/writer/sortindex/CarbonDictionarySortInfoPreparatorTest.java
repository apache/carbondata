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
package org.apache.carbondata.core.writer.sortindex;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryChunksWrapper;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
   */
  @Test public void testGetDictionarySortInfo() {

    List<String> newDistinctValues = new ArrayList<>();
    newDistinctValues.add("abc");
    newDistinctValues.add("xyz");
    Dictionary dictionary = new MockUp<Dictionary>() {
      @Mock public DictionaryChunksWrapper getDictionaryChunks() {
        List<byte[]> data = new ArrayList<>();
        data.add(new byte[] { 1, 2 });
        List<List<byte[]>> dictionaryChunks = new ArrayList<>();
        dictionaryChunks.add(data);
        return new DictionaryChunksWrapper(dictionaryChunks);
      }
    }.getMockInstance();

    new MockUp<DictionaryChunksWrapper>() {
      @Mock public int getSize() {
        return 1;
      }
    };

    CarbonDictionarySortInfo carbonDictionarySortInfo = carbonDictionarySortInfoPreparator
        .getDictionarySortInfo(newDistinctValues, dictionary, DataTypes.STRING);
    int expectedGetSortIndexValue = 1;
    int expectedGetSortInvertedIndexLength = 3;
    int actualGetSortIndexValue = carbonDictionarySortInfo.getSortIndex().get(0);
    int actualGetSortInvertedIndexLength = carbonDictionarySortInfo.getSortIndexInverted().size();
    assertEquals(actualGetSortIndexValue, expectedGetSortIndexValue);
    assertEquals(actualGetSortInvertedIndexLength, expectedGetSortInvertedIndexLength);
  }

  /**
   * Tests getDictionarySortInfo when dictionary is null
   */
  @Test public void testGetDictionarySortInfoDictionaryNullCase() {

    List<String> newDistinctValues = new ArrayList<>();
    newDistinctValues.add("abc");
    newDistinctValues.add("xyz");
    Dictionary dictionary = null;
    CarbonDictionarySortInfo carbonDictionarySortInfo = carbonDictionarySortInfoPreparator
        .getDictionarySortInfo(newDistinctValues, dictionary, DataTypes.createDefaultArrayType());
    int expectedGetSortIndexValue = 1;
    int expectedGetSortInvertedIndexLength = 2;
    int actualGetSortIndexValue = carbonDictionarySortInfo.getSortIndex().get(0);
    int actualGetSortInvertedIndexLength = carbonDictionarySortInfo.getSortIndexInverted().size();
    assertEquals(actualGetSortIndexValue, expectedGetSortIndexValue);
    assertEquals(actualGetSortInvertedIndexLength, expectedGetSortInvertedIndexLength);
  }

}
