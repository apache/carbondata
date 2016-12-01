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

package org.apache.carbondata.core.carbon.datastore.chunk.reader.dimension;

import static junit.framework.TestCase.assertEquals;

import java.util.ArrayList;
import java.util.List;

import mockit.Mock;
import mockit.MockUp;

import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.reader.dimension.v1.CompressedDimensionChunkFileBasedReaderV1;
import org.apache.carbondata.core.carbon.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.datachunk.DataChunk;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.columnar.UnBlockIndexer;
import org.apache.carbondata.core.datastorage.store.compression.SnappyCompression;
import org.apache.carbondata.core.keygenerator.mdkey.NumberCompressor;
import org.apache.carbondata.core.util.CarbonUtil;
import org.junit.BeforeClass;
import org.junit.Test;

public class CompressedDimensionChunkFileBasedReaderTest {

  static CompressedDimensionChunkFileBasedReaderV1 compressedDimensionChunkFileBasedReader;
  static List<DataChunk> dataChunkList;

  @BeforeClass public static void setup() {
    int eachColumnBlockSize[] = { 1, 2, 4, 5 };
    dataChunkList = new ArrayList<>();

    DataChunk dataChunk = new DataChunk();
    dataChunkList.add(dataChunk);
    BlockletInfo info = new BlockletInfo();
    info.setDimensionColumnChunk(dataChunkList);
    compressedDimensionChunkFileBasedReader =
        new CompressedDimensionChunkFileBasedReaderV1(info, eachColumnBlockSize, "filePath");
  }

  @Test public void readDimensionChunksTest() {
    FileHolder fileHolder = new MockUp<FileHolder>() {
      @Mock public byte[] readByteArray(String filePath, long offset, int length) {
        byte mockedValue[] = { 1, 5, 4, 8, 7 };
        return mockedValue;
      }
    }.getMockInstance();

    new MockUp<CarbonUtil>() {
      @Mock public boolean hasEncoding(List<Encoding> encodings, Encoding encoding) {
        return true;
      }

      @Mock public int[] getUnCompressColumnIndex(int totalLength, byte[] columnIndexData,
          NumberCompressor numberCompressor) {
        int mockedValue[] = { 1, 1 };
        return mockedValue;
      }
    };

    new MockUp<SnappyCompression.SnappyByteCompression>() {
      @Mock public byte[] unCompress(byte[] compInput) {
        byte mockedValue[] = { 1 };
        return mockedValue;
      }
    };

    new MockUp<UnBlockIndexer>() {
      @Mock public byte[] uncompressData(byte[] data, int[] index, int keyLen) {
        byte mockedValue[] = { 1, 5, 4, 8, 7 };
        return mockedValue;
      }
    };

    int[][] blockIndexes = {{0,0}};
    DimensionColumnDataChunk dimensionColumnDataChunk[] =
        compressedDimensionChunkFileBasedReader.readDimensionChunks(fileHolder, blockIndexes);
    byte expectedResult[] = { 1 };
    assertEquals(dimensionColumnDataChunk[0].getAttributes().getColumnValueSize(), 1);
    for (int i = 0; i < dimensionColumnDataChunk[0].getChunkData(0).length; i++) {
      assertEquals(dimensionColumnDataChunk[0].getChunkData(0)[i], expectedResult[i]);
    }
  }

  @Test public void readDimensionChunksTestForIfStatement() {
    FileHolder fileHolder = new MockUp<FileHolder>() {
      @Mock public byte[] readByteArray(String filePath, long offset, int length) {
        byte mockedValue[] = { 1, 5, 4, 8, 7 };
        return mockedValue;
      }
    }.getMockInstance();

    new MockUp<CarbonUtil>() {
      @Mock public boolean hasEncoding(List<Encoding> encodings, Encoding encoding) {
        return true;
      }

      @Mock public int[] getUnCompressColumnIndex(int totalLength, byte[] columnIndexData,
          NumberCompressor numberCompressor) {
        int mockedValue[] = { 1, 1 };
        return mockedValue;
      }
    };

    new MockUp<SnappyCompression.SnappyByteCompression>() {
      @Mock public byte[] unCompress(byte[] compInput) {
        byte mockedValue[] = { 1 };
        return mockedValue;
      }
    };

    new MockUp<UnBlockIndexer>() {
      @Mock public byte[] uncompressData(byte[] data, int[] index, int keyLen) {
        byte mockedValue[] = { 1, 5, 4, 8, 7 };
        return mockedValue;
      }
    };

    new MockUp<DataChunk>() {
      @Mock public boolean isRowMajor() {
        return true;
      }
    };
    int[][] blockIndexes = {{0,0}};
    DimensionColumnDataChunk dimensionColumnDataChunk[] =
        compressedDimensionChunkFileBasedReader.readDimensionChunks(fileHolder, blockIndexes);

    byte expectedResult[] = { 1 };
    assertEquals(dimensionColumnDataChunk[0].getAttributes().getColumnValueSize(), 1);

    for (int i = 0; i < dimensionColumnDataChunk[0].getChunkData(0).length; i++) {
      assertEquals(dimensionColumnDataChunk[0].getChunkData(0)[i], expectedResult[i]);
    }
  }
}
