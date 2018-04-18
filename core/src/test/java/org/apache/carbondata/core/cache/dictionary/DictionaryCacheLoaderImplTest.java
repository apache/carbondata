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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.reader.CarbonDictionaryReaderImpl;
import org.apache.carbondata.core.reader.sortindex.CarbonDictionarySortIndexReaderImpl;
import org.apache.carbondata.format.ColumnDictionaryChunk;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DictionaryCacheLoaderImplTest {

  private static DictionaryCacheLoaderImpl dictionaryCacheLoader;
  private static DictionaryInfo dictionaryInfo;
  private static ColumnIdentifier columnIdentifier;
  private static DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier;

  @BeforeClass public static void setUp() {
    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("db", "table1", "1");
    AbsoluteTableIdentifier absoluteTableIdentifier = AbsoluteTableIdentifier.from("/tmp",
        carbonTableIdentifier);
    Map<String, String> columnProperties = new HashMap<>();
    columnProperties.put("prop1", "value1");
    columnProperties.put("prop2", "value2");
    columnIdentifier = new ColumnIdentifier("1", columnProperties, DataTypes.STRING);
    dictionaryColumnUniqueIdentifier =
        new DictionaryColumnUniqueIdentifier(absoluteTableIdentifier, columnIdentifier,
            columnIdentifier.getDataType());
    dictionaryCacheLoader = new DictionaryCacheLoaderImpl(dictionaryColumnUniqueIdentifier);
    dictionaryInfo = new ColumnDictionaryInfo(DataTypes.STRING);
    new MockUp<CarbonDictionaryReaderImpl>() {
      @Mock @SuppressWarnings("unused") Iterator<byte[]> read(long startOffset, long endOffset)
          throws IOException {
        ColumnDictionaryChunk columnDictionaryChunk = new ColumnDictionaryChunk();
        ByteBuffer byteBuffer1 = ByteBuffer.wrap("c".getBytes());
        ByteBuffer byteBuffer2 = ByteBuffer.wrap("d".getBytes());
        columnDictionaryChunk.setValues(Arrays.asList(byteBuffer1, byteBuffer2));
        return new ColumnDictionaryChunkIterator(Arrays.asList(columnDictionaryChunk));
      }
    };

    new MockUp<CarbonDictionarySortIndexReaderImpl>() {
      @Mock @SuppressWarnings("unused") List<Integer> readSortIndex() throws IOException {
        return Arrays.asList(1, 2);
      }

      @Mock @SuppressWarnings("unused") List<Integer> readInvertedSortIndex() throws IOException {
        return Arrays.asList(1, 2);
      }
    };
  }

  @Test public void testToLoad() throws IOException {
    new MockUp<ColumnDictionaryInfo>() {
      @Mock @SuppressWarnings("unused") int getSizeOfLastDictionaryChunk() {
        return 9999;
      }
    };
    dictionaryCacheLoader.load(dictionaryInfo, 0L, 2L, true);
    assertEquals(dictionaryInfo.getDictionaryChunks().getSize(), 4);
  }

  @Test public void testToLoadWithsizeOfOneDictionaryChunkLessThanZero() throws IOException {
    new MockUp<ColumnDictionaryInfo>() {
      @Mock @SuppressWarnings("unused") int getSizeOfLastDictionaryChunk() {
        return 10000;
      }
    };
    dictionaryCacheLoader.load(dictionaryInfo, 0L, 2L, true);
    assertEquals(dictionaryInfo.getDictionaryChunks().getSize(), 2);
  }
}
