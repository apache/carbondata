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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalType;
import org.apache.carbondata.core.util.CarbonUtil;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class ColumnDictionaryInfoTest {

  private ColumnDictionaryInfo columnDictionaryInfo;

  @Test public void testGetIncrementalSurrogateKeyFromDictionary() {
    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.STRING);

    List<String> evaluateResultList = Arrays.asList("china", "france");
    List<byte[]> byteValuesOfFilterMembers = convertListElementsIntoByteArray(evaluateResultList);

    new MockUp<CarbonUtil>() {

      @Mock public int getDictionaryChunkSize() {
        return 10000;
      }
    };

    columnDictionaryInfo.setSortOrderIndex(Arrays.asList(1, 2, 3, 4, 5, 6, 7));

    List<byte[]> chunks = Arrays.asList(new byte[] { 64, 78, 85, 35, 76, 76, 36, 33 },
        new byte[] { 98, 114, 97, 122, 105, 108 }, new byte[] { 99, 97, 110, 97, 100, 97 },
        new byte[] { 99, 104, 105, 110, 97 }, new byte[] { 102, 114, 97, 110, 99, 101 },
        new byte[] { 117, 107 }, new byte[] { 117, 117, 97 });

    List<List<byte[]>> dictionaryChunks = new CopyOnWriteArrayList<>();
    dictionaryChunks.add(chunks);

    columnDictionaryInfo.dictionaryChunks = dictionaryChunks;

    List<Integer> surrogates = new ArrayList<>();
    columnDictionaryInfo
        .getIncrementalSurrogateKeyFromDictionary(byteValuesOfFilterMembers, surrogates);

    assertThat(surrogates.size(), is(equalTo(2)));

    List<Integer> expectedSurrogates = new ArrayList<>();
    expectedSurrogates.add(4);
    expectedSurrogates.add(5);

    assertThat(surrogates, is(equalTo(expectedSurrogates)));
  }

  @Test public void testGetIncrementalSurrogateKeyFromDictionaryWithZeroSurrogate() {
    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.STRING);

    List<String> evaluateResultList = Arrays.asList("china", "france");
    List<byte[]> byteValuesOfFilterMembers = convertListElementsIntoByteArray(evaluateResultList);

    new MockUp<CarbonUtil>() {

      @Mock public int getDictionaryChunkSize() {
        return 10000;
      }
    };

    columnDictionaryInfo.setSortOrderIndex(Arrays.asList(1, 2, 3, 4, 5, 6, 7));

    List<Integer> surrogates = new ArrayList<>();
    columnDictionaryInfo
        .getIncrementalSurrogateKeyFromDictionary(byteValuesOfFilterMembers, surrogates);

    int expectedSize = 1;
    assertThat(surrogates.size(), is(equalTo(expectedSize)));

    List<Integer> expectedSurrogates = new ArrayList<>();
    expectedSurrogates.add(0);

    assertThat(surrogates, is(equalTo(expectedSurrogates)));
  }

  @Test public void testGetIncrementalSurrogateKeyFromDictionaryWithNullValue() {
    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.STRING);

    List<String> evaluateResultList = Arrays.asList("@NU#LL$!");
    List<byte[]> byteValuesOfFilterMembers = convertListElementsIntoByteArray(evaluateResultList);

    new MockUp<CarbonUtil>() {

      @Mock public int getDictionaryChunkSize() {
        return 10000;
      }
    };

    columnDictionaryInfo.setSortOrderIndex(Arrays.asList(2, 3, 1));

    List<Integer> surrogates = new ArrayList<>();
    columnDictionaryInfo
        .getIncrementalSurrogateKeyFromDictionary(byteValuesOfFilterMembers, surrogates);

    int expectedSize = 1;
    assertThat(surrogates.size(), is(equalTo(expectedSize)));

    List<Integer> expectedSurrogates = new ArrayList<>();
    expectedSurrogates.add(1);
    assertThat(surrogates, is(equalTo(expectedSurrogates)));
  }

  @Test public void testGetIncrementalSurrogateKeyFromDictionaryWithTypeException() {
    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.INT);

    List<String> evaluateResultList = Arrays.asList("china", "france");
    List<byte[]> byteValuesOfFilterMembers = convertListElementsIntoByteArray(evaluateResultList);

    new MockUp<CarbonUtil>() {

      @Mock public int getDictionaryChunkSize() {
        return 10000;
      }
    };

    columnDictionaryInfo.setSortOrderIndex(Arrays.asList(1, 2, 3, 4, 5, 6, 7));

    List<byte[]> chunks = Arrays.asList(new byte[] { 64, 78, 85, 35, 76, 76, 36, 33 },
        new byte[] { 98, 114, 97, 122, 105, 108 }, new byte[] { 99, 97, 110, 97, 100, 97 },
        new byte[] { 99, 104, 105, 110, 97 }, new byte[] { 102, 114, 97, 110, 99, 101 },
        new byte[] { 117, 107 }, new byte[] { 117, 117, 97 });

    List<List<byte[]>> dictionaryChunks = new CopyOnWriteArrayList<>();
    dictionaryChunks.add(chunks);

    columnDictionaryInfo.dictionaryChunks = dictionaryChunks;

    List<Integer> surrogates = new ArrayList<>();
    columnDictionaryInfo
        .getIncrementalSurrogateKeyFromDictionary(byteValuesOfFilterMembers, surrogates);

    int expectedSize = 2;
    assertThat(surrogates.size(), is(equalTo(expectedSize)));

    List<Integer> expectedSurrogates = new ArrayList<>();
    expectedSurrogates.add(4);
    expectedSurrogates.add(5);

    assertThat(surrogates, is(equalTo(expectedSurrogates)));
  }

  @Test public void testGetIncrementalSurrogateKeyFromDictionaryWithDoubleType() {
    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.DOUBLE);

    List<String> evaluateResultList = Arrays.asList("15999");
    List<byte[]> byteValuesOfFilterMembers = convertListElementsIntoByteArray(evaluateResultList);

    new MockUp<CarbonUtil>() {

      @Mock public int getDictionaryChunkSize() {
        return 10000;
      }
    };

    columnDictionaryInfo.setSortOrderIndex(Arrays.asList(1, 2));

    List<byte[]> chunks = Arrays.asList(new byte[] { 49, 53, 57, 57, 57 });

    List<List<byte[]>> dictionaryChunks = new CopyOnWriteArrayList<>();
    dictionaryChunks.add(chunks);

    columnDictionaryInfo.dictionaryChunks = dictionaryChunks;

    List<Integer> surrogates = new ArrayList<>();
    columnDictionaryInfo
        .getIncrementalSurrogateKeyFromDictionary(byteValuesOfFilterMembers, surrogates);

    int expectedSize = 1;
    assertThat(surrogates.size(), is(equalTo(expectedSize)));

    List<Integer> expectedSurrogates = new ArrayList<>();
    expectedSurrogates.add(1);

    assertThat(surrogates, is(equalTo(expectedSurrogates)));
  }

  @Test public void testGetIncrementalSurrogateKeyFromDictionaryWithIntType() {
    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.INT);

    List<String> evaluateResultList = Arrays.asList("998");
    List<byte[]> byteValuesOfFilterMembers = convertListElementsIntoByteArray(evaluateResultList);

    new MockUp<CarbonUtil>() {

      @Mock public int getDictionaryChunkSize() {
        return 10000;
      }
    };

    columnDictionaryInfo.setSortOrderIndex(Arrays.asList(1, 2));

    List<byte[]> chunks = Arrays.asList(new byte[] { 57, 57, 56 });

    List<List<byte[]>> dictionaryChunks = new CopyOnWriteArrayList<>();
    dictionaryChunks.add(chunks);

    columnDictionaryInfo.dictionaryChunks = dictionaryChunks;

    List<Integer> surrogates = new ArrayList<>();
    columnDictionaryInfo
        .getIncrementalSurrogateKeyFromDictionary(byteValuesOfFilterMembers, surrogates);

    int expectedSize = 1;
    assertThat(surrogates.size(), is(equalTo(expectedSize)));

    List<Integer> expectedSurrogates = new ArrayList<>();
    expectedSurrogates.add(1);

    assertThat(surrogates, is(equalTo(expectedSurrogates)));
  }

  @Test public void testGetIncrementalSurrogateKeyFromDictionaryWithDecimalType() {
    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.createDefaultDecimalType());

    List<String> evaluateResultList = Arrays.asList("150011.550");
    List<byte[]> byteValuesOfFilterMembers = convertListElementsIntoByteArray(evaluateResultList);

    new MockUp<CarbonUtil>() {

      @Mock public int getDictionaryChunkSize() {
        return 10000;
      }
    };

    columnDictionaryInfo.setSortOrderIndex(Arrays.asList(2, 3, 1));

    List<byte[]> chunks = Arrays.asList(new byte[] { 64, 78, 85, 35, 76, 76, 36, 33 },
        new byte[] { 49, 53, 48, 48, 48, 49, 46, 50, 53, 54 },
        new byte[] { 49, 53, 48, 48, 49, 49, 46, 53, 53, 48 });

    List<List<byte[]>> dictionaryChunks = new CopyOnWriteArrayList<>();
    dictionaryChunks.add(chunks);

    columnDictionaryInfo.dictionaryChunks = dictionaryChunks;

    List<Integer> surrogates = new ArrayList<>();
    columnDictionaryInfo
        .getIncrementalSurrogateKeyFromDictionary(byteValuesOfFilterMembers, surrogates);

    int expectedSize = 1;
    assertThat(surrogates.size(), is(equalTo(expectedSize)));

    List<Integer> expectedSurrogates = new ArrayList<>();
    expectedSurrogates.add(3);

    assertThat(surrogates, is(equalTo(expectedSurrogates)));
  }

  @Test public void testGetIncrementalSurrogateKeyFromDictionaryWithLongType() {
    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.LONG);

    List<String> evaluateResultList = Arrays.asList("1500115505555");
    List<byte[]> byteValuesOfFilterMembers = convertListElementsIntoByteArray(evaluateResultList);

    new MockUp<CarbonUtil>() {

      @Mock public int getDictionaryChunkSize() {
        return 10000;
      }
    };

    columnDictionaryInfo.setSortOrderIndex(Arrays.asList(2, 3, 1));

    List<byte[]> chunks = Arrays.asList(new byte[] { 64, 78, 85, 35, 76, 76, 36, 33 },
        new byte[] { 49, 53, 48, 48, 48, 49, 50, 53, 54, 52, 52, 52, 52 },
        new byte[] { 49, 53, 48, 48, 49, 49, 53, 53, 48, 53, 53, 53, 53 });

    List<List<byte[]>> dictionaryChunks = new CopyOnWriteArrayList<>();
    dictionaryChunks.add(chunks);

    columnDictionaryInfo.dictionaryChunks = dictionaryChunks;

    List<Integer> surrogates = new ArrayList<>();
    columnDictionaryInfo
        .getIncrementalSurrogateKeyFromDictionary(byteValuesOfFilterMembers, surrogates);

    int expectedSize = 1;
    assertThat(surrogates.size(), is(equalTo(expectedSize)));

    List<Integer> expectedSurrogates = new ArrayList<>();
    expectedSurrogates.add(3);

    assertThat(surrogates, is(equalTo(expectedSurrogates)));
  }

  @Test public void testAddDictionaryChunkEmpty() {
    new MockUp<CarbonUtil>() {

      @Mock public int getDictionaryChunkSize() {
        return 1;
      }
    };

    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.STRING);

    columnDictionaryInfo.dictionaryChunks = new CopyOnWriteArrayList<>();

    List<byte[]> newDictionaryChunk = Arrays.asList(new byte[] { 1, 2, 3, 4 });

    columnDictionaryInfo.addDictionaryChunk(newDictionaryChunk);

    List<List<byte[]>> expectedDictionaryChunks = new CopyOnWriteArrayList<>();
    expectedDictionaryChunks.add(newDictionaryChunk);

    assertThat(columnDictionaryInfo.dictionaryChunks, is(equalTo(expectedDictionaryChunks)));
  }

  @Test public void testAddDictionaryChunkAppend() {

    new MockUp<CarbonUtil>() {

      @Mock public int getDictionaryChunkSize() {
        return 1;
      }
    };

    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.STRING);

    columnDictionaryInfo.dictionaryChunks = new CopyOnWriteArrayList<>();

    List<byte[]> newDictionaryChunk1 = Arrays.asList(new byte[] { 1, 2, 3, 4 });

    columnDictionaryInfo.addDictionaryChunk(newDictionaryChunk1);

    List<byte[]> newDictionaryChunk2 = Arrays.asList(new byte[] { 5, 6, 7, 8 });

    columnDictionaryInfo.addDictionaryChunk(newDictionaryChunk2);

    List<List<byte[]>> expectedDictionaryChunks = new CopyOnWriteArrayList<>();
    expectedDictionaryChunks.add(newDictionaryChunk1);
    expectedDictionaryChunks.add(newDictionaryChunk2);

    assertThat(columnDictionaryInfo.dictionaryChunks, is(equalTo(expectedDictionaryChunks)));
  }

  @Test public void addDictionaryChunkWithHugeChunkSize() {

    new MockUp<CarbonUtil>() {

      @Mock public int getDictionaryChunkSize() {
        return 10;
      }
    };

    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.STRING);

    columnDictionaryInfo.dictionaryChunks = new CopyOnWriteArrayList<>();

    List<byte[]> newDictionaryChunk1 = new ArrayList<>(Arrays.asList(new byte[] { 1, 2, 3, 4 }));

    columnDictionaryInfo.addDictionaryChunk(newDictionaryChunk1);

    List<byte[]> newDictionaryChunk2 = new ArrayList<>(Arrays.asList(new byte[] { 5, 6, 7, 8 }));

    columnDictionaryInfo.addDictionaryChunk(newDictionaryChunk2);

    List<List<byte[]>> expectedDictionaryChunks = new CopyOnWriteArrayList<>();
    expectedDictionaryChunks.add(newDictionaryChunk1);

    assertThat(columnDictionaryInfo.dictionaryChunks, is(equalTo(expectedDictionaryChunks)));
  }

  @Test public void addDictionaryChunkWithSplitChunks() {

    new MockUp<CarbonUtil>() {

      @Mock public int getDictionaryChunkSize() {
        return 2;
      }
    };

    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.STRING);

    columnDictionaryInfo.dictionaryChunks = new CopyOnWriteArrayList<>();

    List<byte[]> newDictionaryChunk1 = new ArrayList<>(Arrays.asList(new byte[] { 1, 2, 3, 4 }));

    columnDictionaryInfo.addDictionaryChunk(newDictionaryChunk1);

    List<byte[]> newDictionaryChunk2 =
        new ArrayList<>(Arrays.asList(new byte[] { 5, 6, 7 }, new byte[] { 8, 9, 10 }));

    columnDictionaryInfo.addDictionaryChunk(newDictionaryChunk2);

    byte[][] expectedResult = {{ 1, 2, 3, 4 }, { 5, 6, 7 }, { 8, 9, 10 }};
    assertThat(columnDictionaryInfo.dictionaryChunks.get(0),
        hasItems(expectedResult[0], expectedResult[1]));
    assertThat(columnDictionaryInfo.dictionaryChunks.get(1), hasItems(expectedResult[2]));
  }

  @Test public void testGtSortedIndexWithMinimumSurrogateKey() {

    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.STRING);

    columnDictionaryInfo.setSortReverseOrderIndex(Arrays.asList(1, 2, 3));

    final int result = columnDictionaryInfo.getSortedIndex(0);

    int expectedResult = -1;
    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test public void testGtSortedIndexWithMaximumSurrogateKey() {

    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.STRING);

    columnDictionaryInfo.setSortReverseOrderIndex(Arrays.asList(1, 2, 3));

    final int result = columnDictionaryInfo.getSortedIndex(4);

    int expectedResult = -1;
    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test public void testGtSortedIndexWithSurrogateKey() {

    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.STRING);

    columnDictionaryInfo.setSortReverseOrderIndex(Arrays.asList(1, 2, 3));

    final int result = columnDictionaryInfo.getSortedIndex(2);

    int expectedResult = 2;
    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test public void testGetSizeOfLastDictionaryChunkWithDictionaryChunkZero() {

    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.STRING);

    final int result = columnDictionaryInfo.getSizeOfLastDictionaryChunk();

    int expectedResult = 0;
    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test public void testGetSizeOfLastDictionaryChunk() {

    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.STRING);

    columnDictionaryInfo.dictionaryChunks = new CopyOnWriteArrayList<>();

    List<byte[]> newDictionaryChunk1 = new ArrayList<>(Arrays.asList(new byte[] { 1, 2, 3, 4 }));

    columnDictionaryInfo.addDictionaryChunk(newDictionaryChunk1);

    final int result = columnDictionaryInfo.getSizeOfLastDictionaryChunk();

    int expectedResult = 1;
    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test public void testGetDictionaryValueFromSortedIndexWithMinimumSurrogateKey() {

    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.STRING);

    columnDictionaryInfo.setSortReverseOrderIndex(Arrays.asList(1, 2, 3));

    final String result = columnDictionaryInfo.getDictionaryValueFromSortedIndex(0);

    assertThat(result, is(nullValue()));
  }

  @Test public void testGetDictionaryValueFromSortedIndexWithMaximumSurrogateKey() {

    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.STRING);

    columnDictionaryInfo.setSortReverseOrderIndex(Arrays.asList(1, 2, 3));

    final String result = columnDictionaryInfo.getDictionaryValueFromSortedIndex(4);

    assertThat(result, is(nullValue()));
  }

  @Test public void testGetDictionaryValueFromSortedIndex() {

    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.STRING);

    columnDictionaryInfo.setSortReverseOrderIndex(Arrays.asList(0, 1, 2, 3));

    columnDictionaryInfo.setSortOrderIndex(Arrays.asList(1, 2, 3));

    final String result = columnDictionaryInfo.getDictionaryValueFromSortedIndex(1);

    assertThat(result, is(nullValue()));
  }

  @Test
  public void testGetSurrogateKey() {

    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.STRING);

    byte[] value = convertListElementsIntoByteArray(Arrays.asList("china")).get(0);

    new MockUp<CarbonUtil>() {

      @Mock public int getDictionaryChunkSize() {
        return 10000;
      }
    };

    columnDictionaryInfo.setSortOrderIndex(Arrays.asList(1, 2, 3, 4, 5, 6, 7));

    List<byte[]> chunks = Arrays.asList(new byte[] { 64, 78, 85, 35, 76, 76, 36, 33 },
        new byte[] { 98, 114, 97, 122, 105, 108 }, new byte[] { 99, 97, 110, 97, 100, 97 },
        new byte[] { 99, 104, 105, 110, 97 }, new byte[] { 102, 114, 97, 110, 99, 101 },
        new byte[] { 117, 107 }, new byte[] { 117, 117, 97 });

    List<List<byte[]>> dictionaryChunks = new CopyOnWriteArrayList<>();
    dictionaryChunks.add(chunks);

    columnDictionaryInfo.dictionaryChunks = dictionaryChunks;

    int result = columnDictionaryInfo.getSurrogateKey(value);

    int expectedResult = 4;
    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test
  public void testGetSurrogateKeyWithIntType() {

    columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.INT);

    byte[] value = convertListElementsIntoByteArray(Arrays.asList("998")).get(0);

    new MockUp<CarbonUtil>() {

      @Mock public int getDictionaryChunkSize() {
        return 10000;
      }
    };

    columnDictionaryInfo.setSortOrderIndex(Arrays.asList(1, 2));

    List<byte[]> chunks = Arrays.asList(new byte[] { 57, 57, 56 });

    List<List<byte[]>> dictionaryChunks = new CopyOnWriteArrayList<>();
    dictionaryChunks.add(chunks);

    columnDictionaryInfo.dictionaryChunks = dictionaryChunks;

    int result = columnDictionaryInfo.getSurrogateKey(value);

    int expectedResult = 1;
    assertThat(result, is(equalTo(expectedResult)));
  }

  private List<byte[]> convertListElementsIntoByteArray(List<String> stringList) {
    List<byte[]> byteValuesOfFilterMembers = new ArrayList<>(stringList.size());
    for (int i = 0; i < stringList.size(); i++) {
      byte[] keyData =
          stringList.get(i).getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      byteValuesOfFilterMembers.add(keyData);
    }
    return byteValuesOfFilterMembers;
  }
}
