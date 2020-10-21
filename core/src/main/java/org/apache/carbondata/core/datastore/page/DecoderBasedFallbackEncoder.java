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

package org.apache.carbondata.core.datastore.page;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonUtil;

public class DecoderBasedFallbackEncoder implements Callable<FallbackEncodedColumnPage> {
  /**
   * actual local dictionary generated column page
   */
  private EncodedColumnPage encodedColumnPage;

  /**
   * actual index in the page
   * this is required as in a blocklet few pages will be local dictionary
   * encoded and few pages will be plain text encoding
   * in this case local dictionary encoded page
   */
  private int pageIndex;

  private LocalDictionaryGenerator localDictionaryGenerator;

  public DecoderBasedFallbackEncoder(EncodedColumnPage encodedColumnPage, int pageIndex,
      LocalDictionaryGenerator localDictionaryGenerator) {
    this.encodedColumnPage = encodedColumnPage;
    this.pageIndex = pageIndex;
    this.localDictionaryGenerator = localDictionaryGenerator;
  }

  @Override
  public FallbackEncodedColumnPage call() throws Exception {
    int pageSize =
        encodedColumnPage.getActualPage().getPageSize();
    int offset = 0;

    // disable encoding using local dictionary
    encodedColumnPage.getActualPage().disableLocalDictEncoding();

    // create a new column page which will have actual data instead of encoded data
    ColumnPage actualDataColumnPage =
        ColumnPage.newPage(encodedColumnPage.getActualPage().getColumnPageEncoderMeta(),
            encodedColumnPage.getActualPage().getPageSize());

    actualDataColumnPage.setStatsCollector(encodedColumnPage.getActualPage().statsCollector);

    fillDataFromLocalDictionaryData(pageSize, offset, actualDataColumnPage);

    // get column spec for existing column page
    TableSpec.ColumnSpec columnSpec = encodedColumnPage.getActualPage().getColumnSpec();
    FallbackEncodedColumnPage fallBackEncodedColumnPage =
        CarbonUtil.getFallBackEncodedColumnPage(actualDataColumnPage, pageIndex, columnSpec);
    // here freeing the memory of new column page created as fallback is done and
    // fallBackEncodedColumnPage is created using new page of actual data
    // This is required to free the memory once it is of no use
    actualDataColumnPage.freeMemory();
    encodedColumnPage.cleanBuffer();
    return fallBackEncodedColumnPage;
  }

  private void fillDataFromLocalDictionaryData(int pageSize, int offset,
      ColumnPage actualDataColumnPage) throws IOException {
    ByteBuffer encodedData = encodedColumnPage.getEncodedData();
    byte[] pageData = new byte[encodedData.remaining()];
    ByteBuffer buffer = encodedData.asReadOnlyBuffer();
    buffer.get(pageData);

    // uncompress the encoded column page
    Compressor compressor = CompressorFactory.getInstance().getCompressor(
        encodedColumnPage.getActualPage().getColumnPageEncoderMeta().getCompressorName());

    byte[] encoderMeta = encodedColumnPage.getPageMetadata().encoder_meta.get(0).array();
    ByteArrayInputStream stream = new ByteArrayInputStream(encoderMeta);
    DataInputStream in = new DataInputStream(stream);
    ColumnPageEncoderMeta metadata = new ColumnPageEncoderMeta();
    metadata.readFields(in);
    // get the actual data for each dictionary data and put the actual data in new page
    int rowId = 0;
    if (metadata.getStoreDataType() == DataTypes.BYTE) {
      byte[] unCompressByte = compressor
          .unCompressByte(pageData, offset, encodedColumnPage.getPageMetadata().data_page_length);
      for (int i = 0; i < pageSize; i++) {
        actualDataColumnPage.putBytes(rowId++,
            localDictionaryGenerator.getDictionaryKeyBasedOnValue(unCompressByte[i]));
      }
    } else if (metadata.getStoreDataType() == DataTypes.SHORT) {
      short[] unCompressShort = compressor
          .unCompressShort(pageData, offset, encodedColumnPage.getPageMetadata().data_page_length);
      for (int i = 0; i < pageSize; i++) {
        actualDataColumnPage.putBytes(rowId++,
            localDictionaryGenerator.getDictionaryKeyBasedOnValue(unCompressShort[i]));
      }
    } else if (metadata.getStoreDataType() == DataTypes.SHORT_INT) {
      byte[] unCompressShort = compressor
          .unCompressByte(pageData, offset, encodedColumnPage.getPageMetadata().data_page_length);
      for (int i = 0; i < pageSize; i++) {
        actualDataColumnPage.putBytes(rowId++,
            localDictionaryGenerator.getDictionaryKeyBasedOnValue(unCompressShort[i]));
      }
    } else if (metadata.getStoreDataType() == DataTypes.INT) {
      int[] unCompressInt = compressor
          .unCompressInt(pageData, offset, encodedColumnPage.getPageMetadata().data_page_length);
      for (int i = 0; i < pageSize; i++) {
        actualDataColumnPage.putBytes(rowId++,
            localDictionaryGenerator.getDictionaryKeyBasedOnValue(unCompressInt[i]));
      }
    } else {
      throw new UnsupportedOperationException("Cannot compress dictionary");
    }
  }

}
