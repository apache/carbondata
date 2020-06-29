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
import org.apache.carbondata.core.datastore.columnar.ByteArrayBlockIndexerStorage;
import org.apache.carbondata.core.datastore.columnar.ByteArrayBlockIndexerStorageWithoutRowId;
import org.apache.carbondata.core.datastore.columnar.DummyBlockIndexerStorage;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.format.Encoding;

public class PlainDimensionIndexCodec extends IndexStorageCodec {
  /**
   * whether this column is varchar data type(long string)
   */
  private boolean isVarcharType;

  public PlainDimensionIndexCodec(boolean isSort, boolean isInvertedIndex,
      boolean isVarcharType) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2851
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2852
    super(isSort, isInvertedIndex);
    this.isVarcharType = isVarcharType;
  }

  @Override
  public String getName() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3731
    return "PlainDimensionIndexCodec";
  }

  @Override
  public ColumnPageEncoder createEncoder(Map<String, String> parameter) {
    return new IndexStorageEncoder() {

      @Override
      protected void encodeIndexStorage(ColumnPage input) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2896
        BlockIndexerStorage<byte[][]> indexStorage;
        boolean isDictionary = input.isLocalDictGeneratedPage();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2851
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2852
        Compressor compressor = CompressorFactory.getInstance().getCompressor(
            input.getColumnCompressorName());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3731
        if (isInvertedIndex || isDictionary) {
          byte[][] byteArray = input.getByteArrayPage();
          indexStorage = isInvertedIndex ?
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3730
            new ByteArrayBlockIndexerStorage(byteArray, isDictionary, !isDictionary, isSort) :
            new ByteArrayBlockIndexerStorageWithoutRowId(byteArray, true);
          byte[] compressInput = ByteUtil.flatten(indexStorage.getDataPage());
          super.compressedDataPage = compressor.compressByte(compressInput);
        } else {
          ByteBuffer data = input.getByteBuffer();
          indexStorage = new DummyBlockIndexerStorage();
          super.compressedDataPage = compressor.compressByte(data);
        }
        super.indexStorage = indexStorage;
      }

      @Override
      protected List<Encoding> getEncodingList() {
        List<Encoding> encodings = new ArrayList<>();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2420
        if (isVarcharType) {
          encodings.add(Encoding.DIRECT_COMPRESS_VARCHAR);
        } else if (indexStorage.getRowIdPageLengthInBytes() > 0) {
          encodings.add(Encoding.INVERTED_INDEX);
        }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2669
        if (indexStorage.getDataRlePageLengthInBytes() > 0) {
          encodings.add(Encoding.RLE);
        }
        return encodings;
      }
    };
  }
}
