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

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.*;

public class ForwardDictionaryTest {

  private static ForwardDictionary forwardDictionary;

  @BeforeClass public static void setUp() {
    ColumnDictionaryInfo columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.INT);
    forwardDictionary = new ForwardDictionary(columnDictionaryInfo);
  }

  @Test public void testToGetSurrogateKeyForStringInput() {
    new MockUp<ColumnDictionaryInfo>() {
      @Mock @SuppressWarnings("unused") public int getSurrogateKey(String value) {
        return 123;
      }
    };
    int expectedResult = 123;
    assertEquals(forwardDictionary.getSurrogateKey("123"), expectedResult);
  }

  @Test public void testToGetSurrogateKeyForByteInput() {
    new MockUp<ColumnDictionaryInfo>() {
      @Mock @SuppressWarnings("unused") public int getSurrogateKey(byte[] value) {
        return 123;
      }
    };
    int expectedResult = 123;
    assertEquals(forwardDictionary.getSurrogateKey("123".getBytes()), expectedResult);
  }

  @Test public void testToGetDictionaryValueForKey() {
    new MockUp<ColumnDictionaryInfo>() {
      @Mock @SuppressWarnings("unused") public String getDictionaryValueForKey(int surrogateKey) {
        System.out.print("Mocked");
        return "123";
      }
    };
    String expectedResult = "123";
    assertEquals(forwardDictionary.getDictionaryValueForKey(123), expectedResult);
  }

  @Test public void testToGetSortedIndex() {
    new MockUp<ColumnDictionaryInfo>() {
      @SuppressWarnings("unused") @Mock public int getSortedIndex(int surrogateKey) {
        System.out.print("Mocked");
        return 1;
      }
    };
    int expectedResult = 1;
    int sortedIndex = forwardDictionary.getSortedIndex(123);
    assertEquals(sortedIndex, expectedResult);
  }

  @Test public void testToGetDictionaryValueFromSortedIndex() {
    new MockUp<ColumnDictionaryInfo>() {
      @SuppressWarnings("unused") @Mock
      public String getDictionaryValueFromSortedIndex(int sortedIndex) {
        System.out.print("Mocked");
        return "A";
      }
    };
    String expectedResult = "A";
    String dictionaryValue = forwardDictionary.getDictionaryValueFromSortedIndex(123);
    assertEquals(dictionaryValue, expectedResult);
  }

  @Test public void testToGetDictionaryChunks() {
    new MockUp<ColumnDictionaryInfo>() {
      @SuppressWarnings("unused") @Mock public DictionaryChunksWrapper getDictionaryChunks() {
        System.out.print("Mocked");
        List<List<byte[]>> dictionaryChunks =
            Arrays.asList(Arrays.asList("123".getBytes()), Arrays.asList("321".getBytes()));
        return new DictionaryChunksWrapper(dictionaryChunks);
      }
    };
    DictionaryChunksWrapper dictionaryValue = forwardDictionary.getDictionaryChunks();
    int expectedResult = 2;
    assertEquals(dictionaryValue.getSize(), expectedResult);
  }

  @Test public void testToGtSurrogateKeyByIncrementalSearch() {
    new MockUp<ColumnDictionaryInfo>() {
      @SuppressWarnings("unused") @Mock
      public void getIncrementalSurrogateKeyFromDictionary(List<byte[]> byteValuesOfFilterMembers,
          List<Integer> surrogates) {
        surrogates.add(1);
      }
    };
    List<String> evaluateResultList = Arrays.asList("1", "2");
    List<Integer> surrogates = new ArrayList<>(1);
    forwardDictionary.getSurrogateKeyByIncrementalSearch(evaluateResultList, surrogates);
    Integer expectedResult = 1;
    assertEquals(surrogates.get(0), expectedResult);
  }

}
