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

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.columnar.UnBlockIndexer;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.Encoding;

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

  @Override public FallbackEncodedColumnPage call() throws Exception {
    int pageSize =
        encodedColumnPage.getActualPage().getPageSize();
    int offset = 0;
    int[] reverseInvertedIndex = new int[pageSize];
    for (int i = 0; i < pageSize; i++) {
      reverseInvertedIndex[i] = i;
    }
    int[] rlePage;

    // uncompress the encoded column page
    byte[] bytes = CompressorFactory.getInstance().getCompressor(
        encodedColumnPage.getActualPage().getColumnPageEncoderMeta().getCompressorName())
        .unCompressByte(encodedColumnPage.getEncodedData().array(), offset,
            encodedColumnPage.getPageMetadata().data_page_length);

    offset += encodedColumnPage.getPageMetadata().data_page_length;
    ByteBuffer data = ByteBuffer.wrap(encodedColumnPage.getEncodedData().array());

    // if encoded with inverted index, get all the inverted indexes
    if (CarbonUtil
        .hasEncoding(encodedColumnPage.getPageMetadata().encoders, Encoding.INVERTED_INDEX)) {
      int[] invertedIndexes = CarbonUtil
          .getUnCompressColumnIndex(encodedColumnPage.getPageMetadata().rowid_page_length, data,
              offset);
      offset += encodedColumnPage.getPageMetadata().rowid_page_length;
      // get all the reverse inverted index
      reverseInvertedIndex = CarbonUtil.getInvertedReverseIndex(invertedIndexes);
    }
    // if rle is applied then uncompress then actual data based on rle
    if (CarbonUtil.hasEncoding(encodedColumnPage.getPageMetadata().encoders, Encoding.RLE)) {
      rlePage =
          CarbonUtil.getIntArray(data, offset, encodedColumnPage.getPageMetadata().rle_page_length);
      // uncompress the data with rle indexes
      bytes = UnBlockIndexer
          .uncompressData(bytes, rlePage, CarbonCommonConstants.LOCAL_DICT_ENCODED_BYTEARRAY_SIZE);
    }

    // disable encoding using local dictionary
    encodedColumnPage.getActualPage().disableLocalDictEncoding();

    // create a new column page which will have actual data instead of encoded data
    ColumnPage actualDataColumnPage =
        ColumnPage.newPage(encodedColumnPage.getActualPage().getColumnPageEncoderMeta(),
            encodedColumnPage.getActualPage().getPageSize());

    // uncompressed data from encoded column page is dictionary data, get the dictionary data using
    // keygenerator
    KeyGenerator keyGenerator = KeyGeneratorFactory
        .getKeyGenerator(new int[] { CarbonCommonConstants.LOCAL_DICTIONARY_MAX + 1 });

    actualDataColumnPage.setStatsCollector(encodedColumnPage.getActualPage().statsCollector);

    // get the actual data for each dictionary data and put the actual data in new page
    int rowId = 0;
    for (int i = 0; i < pageSize; i++) {
      int index = reverseInvertedIndex[i] * 3;
      int keyArray = (int) keyGenerator.getKeyArray(bytes, index)[0];
      actualDataColumnPage
          .putBytes(rowId++, localDictionaryGenerator.getDictionaryKeyBasedOnValue(keyArray));
    }

    // get column spec for existing column page
    TableSpec.ColumnSpec columnSpec = encodedColumnPage.getActualPage().getColumnSpec();
    FallbackEncodedColumnPage fallBackEncodedColumnPage =
        CarbonUtil.getFallBackEncodedColumnPage(actualDataColumnPage, pageIndex, columnSpec);
    // here freeing the memory of new column page created as fallback is done and
    // fallBackEncodedColumnPage is created using new page of actual data
    // This is required to free the memory once it is of no use
    actualDataColumnPage.freeMemory();
    return fallBackEncodedColumnPage;
  }

}
