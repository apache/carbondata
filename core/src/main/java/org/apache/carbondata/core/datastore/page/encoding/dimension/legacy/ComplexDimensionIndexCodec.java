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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorage;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForShort;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.format.Encoding;

public class ComplexDimensionIndexCodec extends IndexStorageCodec {

  public ComplexDimensionIndexCodec(boolean isSort, boolean isInvertedIndex) {
    super(isSort, isInvertedIndex);
  }

  @Override
  public String getName() {
    return "ComplexDimensionIndexCodec";
  }

  @Override
  public ColumnPageEncoder createEncoder(Map<String, String> parameter) {
    return new IndexStorageEncoder() {
      @Override
      void encodeIndexStorage(ColumnPage inputPage) {
        BlockIndexerStorage<byte[][]> indexStorage =
            new BlockIndexerStorageForShort(inputPage.getByteArrayPage(), false, false, false);
        byte[] flattened = ByteUtil.flatten(indexStorage.getDataPage());
        Compressor compressor = CompressorFactory.getInstance().getCompressor(
            inputPage.getColumnCompressorName());
        byte[] compressed = compressor.compressByte(flattened);
        super.indexStorage = indexStorage;
        super.compressedDataPage = compressed;
      }

      @Override
      protected List<Encoding> getEncodingList() {
        List<Encoding> encodings = new ArrayList<>();
        encodings.add(Encoding.DICTIONARY);
        encodings.add(Encoding.INVERTED_INDEX);
        return encodings;
      }

    };
  }

}
