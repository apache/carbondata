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

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class ReverseDictionaryTest {

  private static ReverseDictionary reverseDictionary;

  @BeforeClass public static void setUp() throws Exception {
    ColumnReverseDictionaryInfo columnReverseDictionaryInfo = new ColumnReverseDictionaryInfo();
    reverseDictionary = new ReverseDictionary(columnReverseDictionaryInfo);
  }

  @Test public void testToGetSurrogateKey() {
    new MockUp<ColumnReverseDictionaryInfo>() {
      @SuppressWarnings("unused") @Mock public int getSurrogateKey(byte[] value) {
        return 123;
      }
    };
    int surrogateKey = reverseDictionary.getSurrogateKey("123".getBytes());
    int expectedResult = 123;
    assertEquals(surrogateKey, expectedResult);
  }

  @Test public void testToGetDictionaryValueForKey() {
    new MockUp<ColumnReverseDictionaryInfo>() {
      @SuppressWarnings("unused") @Mock public String getDictionaryValueForKey(int surrogateKey) {
        return "123";
      }
    };
    String dictionaryValue = reverseDictionary.getDictionaryValueForKey(123);
    String expectedResult = "123";
    assertEquals(dictionaryValue, expectedResult);
  }

  @Test public void testToGetSortedIndex() {
    new MockUp<ColumnReverseDictionaryInfo>() {
      @SuppressWarnings("unused") @Mock public int getSortedIndex(int surrogateKey) {
        return 1;
      }
    };
    int sortedIndex = reverseDictionary.getSortedIndex(123);
    int expectedResult = 1;
    assertEquals(sortedIndex, expectedResult);
  }

  @Test public void testToGetDictionaryValueFromSortedIndex() {
    new MockUp<ColumnReverseDictionaryInfo>() {
      @SuppressWarnings("unused") @Mock
      public String getDictionaryValueFromSortedIndex(int sortedIndex) {
        return "A";
      }
    };
    String dictionaryValue = reverseDictionary.getDictionaryValueFromSortedIndex(123);
    String expectedResult = "A";
    assertEquals(dictionaryValue, expectedResult);
  }

  @Test public void testToGetDictionaryChunks() {
    new MockUp<ColumnReverseDictionaryInfo>() {
      @SuppressWarnings("unused") @Mock public DictionaryChunksWrapper getDictionaryChunks() {
        List<List<byte[]>> dictionaryChunks =
            Arrays.asList(Arrays.asList("123".getBytes()), Arrays.asList("321".getBytes()));
        return new DictionaryChunksWrapper(dictionaryChunks);
      }
    };
    DictionaryChunksWrapper dictionaryValue = reverseDictionary.getDictionaryChunks();
    int expectedResult = 2;
    assertEquals(dictionaryValue.getSize(), expectedResult);
  }

}
