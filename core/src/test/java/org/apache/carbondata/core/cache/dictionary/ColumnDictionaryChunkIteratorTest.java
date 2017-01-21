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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.format.ColumnDictionaryChunk;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ColumnDictionaryChunkIteratorTest {

  private static ColumnDictionaryChunkIterator columnDictionaryChunkIterator;
  private static List<byte[]> expectedResult;

  @BeforeClass public static void setUp() {
    ColumnDictionaryChunk columnDictionaryChunk1 = new ColumnDictionaryChunk();
    ByteBuffer byteBuffer3 = ByteBuffer.wrap("c".getBytes());
    ByteBuffer byteBuffer4 = ByteBuffer.wrap("d".getBytes());
    columnDictionaryChunk1.setValues(new ArrayList<ByteBuffer>());
    ColumnDictionaryChunk columnDictionaryChunk2 = new ColumnDictionaryChunk();
    columnDictionaryChunk2.setValues(Arrays.asList(byteBuffer3, byteBuffer4));
    expectedResult = prepareExpectedData();
    columnDictionaryChunkIterator = new ColumnDictionaryChunkIterator(
        Arrays.asList(columnDictionaryChunk1, columnDictionaryChunk2));
  }

  private static List<byte[]> prepareExpectedData() {
    List<byte[]> chunks = new ArrayList<>();
    chunks.add("c".getBytes());
    chunks.add("d".getBytes());
    return chunks;
  }

  @Test public void testNext() throws Exception {
    List<byte[]> actual = new ArrayList<>();
    while (columnDictionaryChunkIterator.hasNext()) {
      actual.add(columnDictionaryChunkIterator.next());
    }
    for (int i = 0; i < actual.size(); i++) {
      assertThat(expectedResult.get(i), is(equalTo(actual.get(i))));
    }
  }
}
