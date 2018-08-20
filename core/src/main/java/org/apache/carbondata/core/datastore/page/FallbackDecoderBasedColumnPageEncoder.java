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

import java.util.concurrent.Callable;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.CarbonUtil;

public class FallbackDecoderBasedColumnPageEncoder implements Callable<FallbackEncodedColumnPage> {
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

  public FallbackDecoderBasedColumnPageEncoder(EncodedColumnPage encodedColumnPage, int pageIndex,
      LocalDictionaryGenerator localDictionaryGenerator) {
    this.encodedColumnPage = encodedColumnPage;
    this.pageIndex = pageIndex;
    this.localDictionaryGenerator = localDictionaryGenerator;
  }

  @Override public FallbackEncodedColumnPage call() throws Exception {

    // uncompress the encoded column page
    byte[] bytes = CompressorFactory.getInstance().getCompressor()
        .unCompressByte(encodedColumnPage.getEncodedData().array(), 0,
            encodedColumnPage.getPageMetadata().getData_page_length());

    // disable encoding using local dictionary
    encodedColumnPage.getActualPage().disableLocalDictEncoding();

    // get column spec for existing column page
    TableSpec.ColumnSpec columnSpec = encodedColumnPage.getActualPage().getColumnSpec();

    // get the dataType of column
    DataType dataType = encodedColumnPage.getActualPage().getDataType();

    // create a new column page which will have actual data instead of encoded data
    ColumnPage actualDataColumnPage = ColumnPage
        .newPage(columnSpec, dataType,
            encodedColumnPage.getActualPage().getPageSize());

    // uncompressed data from encoded column page is dictionary data, get the dictionary data using
    // keygenerator
    KeyGenerator keyGenerator = KeyGeneratorFactory
        .getKeyGenerator(new int[] { CarbonCommonConstants.LOCAL_DICTIONARY_MAX + 1 });

    actualDataColumnPage.setStatsCollector(encodedColumnPage.getActualPage().statsCollector);

    // get the actual data for each dictionary data and put the actual data in new page
    int rowId = 0;
    for (int i = 0; i < bytes.length; i = i + 3) {
      int keyArray = (int) keyGenerator.getKeyArray(bytes, i)[0];
      actualDataColumnPage
          .putData(rowId++, localDictionaryGenerator.getDictionaryKeyBasedOnValue(keyArray));
    }

    FallbackEncodedColumnPage fallBackEncodedColumnPage =
        CarbonUtil.getFallBackEncodedColumnPage(actualDataColumnPage, pageIndex, columnSpec);
    // here freeing the memory of new column page created as fallback is done and
    // fallBackEncodedColumnPage is created using new page of actual data
    // This is required to free the memory once it is of no use
    actualDataColumnPage.freeMemory();
    return fallBackEncodedColumnPage;
  }
}
