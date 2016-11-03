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
package org.apache.carbondata.core.cache.dictionary;

import mockit.Deencapsulation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonUtil;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ColumnDictionaryInfoTest {

    private ColumnDictionaryInfo columnDictionaryInfo;

    @Test
    public void testGetIncrementalSurrogateKeyFromDictionary() {
        columnDictionaryInfo = new ColumnDictionaryInfo(DataType.STRING);

        List<String> evaluateResultList = Arrays.asList("china", "france");
        List<byte[]> byteValuesOfFilterMembers = convertListElementsIntoByteArray(evaluateResultList);

        new MockUp<CarbonUtil>() {

            @Mock
            public int getDictionaryChunkSize() {
                return 10000;
            }
        };

        Deencapsulation.invoke(columnDictionaryInfo, "setSortOrderIndex", Arrays.asList(1, 2, 3, 4, 5, 6, 7));

        List<byte[]> chunks = Arrays.asList(new byte[]{64, 78, 85, 35, 76, 76, 36, 33},
                new byte[]{98, 114, 97, 122, 105, 108},
                new byte[]{99, 97, 110, 97, 100, 97},
                new byte[]{99, 104, 105, 110, 97},
                new byte[]{102, 114, 97, 110, 99, 101},
                new byte[]{117, 107},
                new byte[]{117, 117, 97});


        List<List<byte[]>> dictionaryChunks = new CopyOnWriteArrayList<>();
        dictionaryChunks.add(chunks);

        Deencapsulation.setField(columnDictionaryInfo, "dictionaryChunks", dictionaryChunks);

        List<Integer> surrogates = new ArrayList<>();
        columnDictionaryInfo.getIncrementalSurrogateKeyFromDictionary(byteValuesOfFilterMembers,
                surrogates);

        assertThat(surrogates.size(), is(equalTo(2)));

        List<Integer> expectedSurrogates = new ArrayList<>();
        expectedSurrogates.add(4);
        expectedSurrogates.add(5);

        assertThat(surrogates, is(equalTo(expectedSurrogates)));
    }

    @Test
    public void testGetIncrementalSurrogateKeyFromDictionaryWithZeroSurrogate() {
        columnDictionaryInfo = new ColumnDictionaryInfo(DataType.STRING);

        List<String> evaluateResultList = Arrays.asList("china", "france");
        List<byte[]> byteValuesOfFilterMembers = convertListElementsIntoByteArray(evaluateResultList);

        new MockUp<CarbonUtil>() {

            @Mock
            public int getDictionaryChunkSize() {
                return 10000;
            }
        };

        Deencapsulation.invoke(columnDictionaryInfo, "setSortOrderIndex", Arrays.asList(1, 2, 3, 4, 5, 6, 7));

        List<Integer> surrogates = new ArrayList<>();
        columnDictionaryInfo.getIncrementalSurrogateKeyFromDictionary(byteValuesOfFilterMembers,
                surrogates);

        assertThat(surrogates.size(), is(equalTo(1)));

        List<Integer> expectedSurrogates = new ArrayList<>();
        expectedSurrogates.add(0);

        assertThat(surrogates, is(equalTo(expectedSurrogates)));
    }

    @Test
    public void testGetIncrementalSurrogateKeyFromDictionaryWithNullValue() {
        columnDictionaryInfo = new ColumnDictionaryInfo(DataType.STRING);

        List<String> evaluateResultList = Arrays.asList("@NU#LL$!");
        List<byte[]> byteValuesOfFilterMembers = convertListElementsIntoByteArray(evaluateResultList);

        new MockUp<CarbonUtil>() {

            @Mock
            public int getDictionaryChunkSize() {
                return 10000;
            }
        };

        Deencapsulation.invoke(columnDictionaryInfo, "setSortOrderIndex", Arrays.asList(2, 3, 1));

        List<Integer> surrogates = new ArrayList<>();
        columnDictionaryInfo.getIncrementalSurrogateKeyFromDictionary(byteValuesOfFilterMembers,
                surrogates);

        assertThat(surrogates.size(), is(equalTo(1)));

        List<Integer> expectedSurrogates = new ArrayList<>();
        expectedSurrogates.add(1);
        assertThat(surrogates, is(equalTo(expectedSurrogates)));
    }

    @Test
    public void testGetIncrementalSurrogateKeyFromDictionaryWithTypeException() {
        columnDictionaryInfo = new ColumnDictionaryInfo(DataType.INT);

        List<String> evaluateResultList = Arrays.asList("china", "france");
        List<byte[]> byteValuesOfFilterMembers = convertListElementsIntoByteArray(evaluateResultList);

        new MockUp<CarbonUtil>() {

            @Mock
            public int getDictionaryChunkSize() {
                return 10000;
            }
        };

        Deencapsulation.invoke(columnDictionaryInfo, "setSortOrderIndex", Arrays.asList(1, 2, 3, 4, 5, 6, 7));

        List<byte[]> chunks = Arrays.asList(new byte[]{64, 78, 85, 35, 76, 76, 36, 33},
                new byte[]{98, 114, 97, 122, 105, 108},
                new byte[]{99, 97, 110, 97, 100, 97},
                new byte[]{99, 104, 105, 110, 97},
                new byte[]{102, 114, 97, 110, 99, 101},
                new byte[]{117, 107},
                new byte[]{117, 117, 97});


        List<List<byte[]>> dictionaryChunks = new CopyOnWriteArrayList<>();
        dictionaryChunks.add(chunks);

        Deencapsulation.setField(columnDictionaryInfo, "dictionaryChunks", dictionaryChunks);

        List<Integer> surrogates = new ArrayList<>();
        columnDictionaryInfo.getIncrementalSurrogateKeyFromDictionary(byteValuesOfFilterMembers,
                surrogates);

        assertThat(surrogates.size(), is(equalTo(2)));

        List<Integer> expectedSurrogates = new ArrayList<>();
        expectedSurrogates.add(4);
        expectedSurrogates.add(5);

        assertThat(surrogates, is(equalTo(expectedSurrogates)));
    }

    @Test
    public void testGetIncrementalSurrogateKeyFromDictionaryWithDoubleType() {
        columnDictionaryInfo = new ColumnDictionaryInfo(DataType.DOUBLE);

        List<String> evaluateResultList = Arrays.asList("15999");
        List<byte[]> byteValuesOfFilterMembers = convertListElementsIntoByteArray(evaluateResultList);

        new MockUp<CarbonUtil>() {

            @Mock
            public int getDictionaryChunkSize() {
                return 10000;
            }
        };

        Deencapsulation.invoke(columnDictionaryInfo, "setSortOrderIndex", Arrays.asList(1, 2));

        List<byte[]> chunks = Arrays.asList(new byte[]{49, 53, 57, 57, 57});


        List<List<byte[]>> dictionaryChunks = new CopyOnWriteArrayList<>();
        dictionaryChunks.add(chunks);

        Deencapsulation.setField(columnDictionaryInfo, "dictionaryChunks", dictionaryChunks);

        List<Integer> surrogates = new ArrayList<>();
        columnDictionaryInfo.getIncrementalSurrogateKeyFromDictionary(byteValuesOfFilterMembers,
                surrogates);

        assertThat(surrogates.size(), is(equalTo(1)));

        List<Integer> expectedSurrogates = new ArrayList<>();
        expectedSurrogates.add(1);

        assertThat(surrogates, is(equalTo(expectedSurrogates)));
    }

    @Test
    public void testGetIncrementalSurrogateKeyFromDictionaryWithIntType() {
        columnDictionaryInfo = new ColumnDictionaryInfo(DataType.INT);

        List<String> evaluateResultList = Arrays.asList("998");
        List<byte[]> byteValuesOfFilterMembers = convertListElementsIntoByteArray(evaluateResultList);

        new MockUp<CarbonUtil>() {

            @Mock
            public int getDictionaryChunkSize() {
                return 10000;
            }
        };

        Deencapsulation.invoke(columnDictionaryInfo, "setSortOrderIndex", Arrays.asList(1, 2));

        List<byte[]> chunks = Arrays.asList(new byte[]{57, 57, 56});


        List<List<byte[]>> dictionaryChunks = new CopyOnWriteArrayList<>();
        dictionaryChunks.add(chunks);

        Deencapsulation.setField(columnDictionaryInfo, "dictionaryChunks", dictionaryChunks);

        List<Integer> surrogates = new ArrayList<>();
        columnDictionaryInfo.getIncrementalSurrogateKeyFromDictionary(byteValuesOfFilterMembers,
                surrogates);

        assertThat(surrogates.size(), is(equalTo(1)));

        List<Integer> expectedSurrogates = new ArrayList<>();
        expectedSurrogates.add(1);

        assertThat(surrogates, is(equalTo(expectedSurrogates)));
    }

    @Test
    public void testGetIncrementalSurrogateKeyFromDictionaryWithDecimalType() {
        columnDictionaryInfo = new ColumnDictionaryInfo(DataType.DECIMAL);

        List<String> evaluateResultList = Arrays.asList("150011.550");
        List<byte[]> byteValuesOfFilterMembers = convertListElementsIntoByteArray(evaluateResultList);

        new MockUp<CarbonUtil>() {

            @Mock
            public int getDictionaryChunkSize() {
                return 10000;
            }
        };

        Deencapsulation.invoke(columnDictionaryInfo, "setSortOrderIndex", Arrays.asList(2, 3, 1));

        List<byte[]> chunks = Arrays.asList(
                new byte[]{64, 78, 85, 35, 76, 76, 36, 33},
                new byte[]{49, 53, 48, 48, 48, 49, 46, 50, 53, 54},
                new byte[]{49, 53, 48, 48, 49, 49, 46, 53, 53, 48}
        );

        List<List<byte[]>> dictionaryChunks = new CopyOnWriteArrayList<>();
        dictionaryChunks.add(chunks);

        Deencapsulation.setField(columnDictionaryInfo, "dictionaryChunks", dictionaryChunks);

        List<Integer> surrogates = new ArrayList<>();
        columnDictionaryInfo.getIncrementalSurrogateKeyFromDictionary(byteValuesOfFilterMembers,
                surrogates);

        assertThat(surrogates.size(), is(equalTo(1)));

        List<Integer> expectedSurrogates = new ArrayList<>();
        expectedSurrogates.add(3);

        assertThat(surrogates, is(equalTo(expectedSurrogates)));
    }

    @Test
    public void testGetIncrementalSurrogateKeyFromDictionaryWithLongType() {
        columnDictionaryInfo = new ColumnDictionaryInfo(DataType.LONG);

        List<String> evaluateResultList = Arrays.asList("1500115505555");
        List<byte[]> byteValuesOfFilterMembers = convertListElementsIntoByteArray(evaluateResultList);

        new MockUp<CarbonUtil>() {

            @Mock
            public int getDictionaryChunkSize() {
                return 10000;
            }
        };

        Deencapsulation.invoke(columnDictionaryInfo, "setSortOrderIndex", Arrays.asList(2, 3, 1));

        List<byte[]> chunks = Arrays.asList(
                new byte[]{64, 78, 85, 35, 76, 76, 36, 33},
                new byte[]{49, 53, 48, 48, 48, 49, 50, 53, 54, 52, 52, 52, 52},
                new byte[]{49, 53, 48, 48, 49, 49, 53, 53, 48, 53, 53, 53, 53}
        );

        List<List<byte[]>> dictionaryChunks = new CopyOnWriteArrayList<>();
        dictionaryChunks.add(chunks);

        Deencapsulation.setField(columnDictionaryInfo, "dictionaryChunks", dictionaryChunks);

        List<Integer> surrogates = new ArrayList<>();
        columnDictionaryInfo.getIncrementalSurrogateKeyFromDictionary(byteValuesOfFilterMembers,
                surrogates);

        assertThat(surrogates.size(), is(equalTo(1)));

        List<Integer> expectedSurrogates = new ArrayList<>();
        expectedSurrogates.add(3);

        assertThat(surrogates, is(equalTo(expectedSurrogates)));
    }

    private List<byte[]> convertListElementsIntoByteArray(List<String> stringList) {
        List<byte[]> byteValuesOfFilterMembers = new ArrayList<>(stringList.size());
        for (int i = 0; i < stringList.size(); i++) {
            byte[] keyData = stringList.get(i)
                    .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
            byteValuesOfFilterMembers.add(keyData);
        }

        return byteValuesOfFilterMembers;
    }
}
