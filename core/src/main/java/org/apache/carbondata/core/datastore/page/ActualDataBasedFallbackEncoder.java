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

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * Below class will be used to encode column pages for which local dictionary was generated
 * but all the pages in blocklet was not encoded with local dictionary.
 * This is required as all the pages of a column in blocklet either it will be local dictionary
 * encoded or without local dictionary encoded.
 */
public class ActualDataBasedFallbackEncoder
    implements Callable<FallbackEncodedColumnPage> {

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

  public ActualDataBasedFallbackEncoder(EncodedColumnPage encodedColumnPage,
      int pageIndex) {
    this.encodedColumnPage = encodedColumnPage;
    this.pageIndex = pageIndex;
  }

  @Override public FallbackEncodedColumnPage call() throws Exception {
    // disable encoding using local dictionary
    encodedColumnPage.getActualPage().disableLocalDictEncoding();

    // get column spec for existing column page
    TableSpec.ColumnSpec columnSpec = encodedColumnPage.getActualPage().getColumnSpec();
    FallbackEncodedColumnPage fallbackEncodedColumnPage = CarbonUtil
        .getFallBackEncodedColumnPage(encodedColumnPage.getActualPage(), pageIndex, columnSpec);
    // here freeing the memory of raw column page as fallback is done and column page will not
    // be used.
    // This is required to free the memory once it is of no use
    encodedColumnPage.freeMemory();
    return fallbackEncodedColumnPage;
  }
}
