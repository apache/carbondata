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

package org.apache.carbondata.core.datastore.page.encoding.dimension.legacy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorage;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForNoInvertedIndexForShort;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForShort;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.format.Encoding;

public class DirectDictDimensionIndexCodec extends IndexStorageCodec {

  public DirectDictDimensionIndexCodec(boolean isSort, boolean isInvertedIndex) {
    super(isSort, isInvertedIndex);
  }

  @Override
  public String getName() {
    return "DirectDictDimensionIndexCodec";
  }

  @Override
  public ColumnPageEncoder createEncoder(Map<String, String> parameter) {
    return new IndexStorageEncoder() {
      @Override
      void encodeIndexStorage(ColumnPage input) {
        BlockIndexerStorage<ByteBuffer[]> indexStorage;
        boolean isDictionary = input.isLocalDictGeneratedPage();

        // if need to build invertIndex or RLE, the columnpage should to be organized in Row,
        // in the other words, we get the data of columnpage as an array, in which each element
        // presenets a row. But if no need to build both invertIndex and RLE, it will increase
        // extra overhead, considering data in columnpage was already stored as flattened data,
        // and the compression is also on flattened  data, to organized data in ROW is actually
        // increase the overheadof "Expand" and "Flatten" with on invertIndex and RLE.
        // Overall, isFlatted presents do we flatten the data? if need to build invertIndex or RLE,
        // isFlattened is set to ture, otherwise, isFlattened is set to false.
        boolean isFlattened = !isInvertedIndex && !isDictionary;

        // when isFlattened is true, data[0] is the flattened data of the columnpage.
        // when isFlattened is false, data[i] is the ith row of the columnpage.
        ByteBuffer[] data = input.getByteBufferArrayPage(isFlattened);
        if (isInvertedIndex) {
          indexStorage = new BlockIndexerStorageForShort(data, isDictionary, !isDictionary, isSort);
        } else {
          indexStorage = new BlockIndexerStorageForNoInvertedIndexForShort(data, isDictionary);
        }
        Compressor compressor = CompressorFactory.getInstance().getCompressor(
            input.getColumnCompressorName());
        ByteBuffer flattened = isFlattened ? data[0] : ByteUtil.flatten(indexStorage.getDataPage());
        ByteBuffer compressed = compressor.compressByte(flattened);
        super.compressedDataPage = ByteUtil.byteBufferToBytes(compressed);
        super.indexStorage = indexStorage;
      }

      @Override
      protected List<Encoding> getEncodingList() {
        List<Encoding> encodings = new ArrayList<>();
        encodings.add(Encoding.DICTIONARY);
        encodings.add(Encoding.RLE);
        if (isInvertedIndex) {
          encodings.add(Encoding.INVERTED_INDEX);
        }
        return encodings;
      }
    };
  }

}
