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
package org.apache.carbondata.processing.store.writer.v3;

import java.util.concurrent.ExecutorService;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.blocklet.EncodedBlocklet;
import org.apache.carbondata.core.datastore.page.EncodedTablePage;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.TablePage;

public class BlockletDataHolder {

  /**
   * current data size
   */
  private long currentSize;

  private EncodedBlocklet encodedBlocklet;

  public BlockletDataHolder(ExecutorService fallbackpool, CarbonFactDataHandlerModel model) {
    encodedBlocklet = new EncodedBlocklet(fallbackpool, Boolean.parseBoolean(
        CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.LOCAL_DICTIONARY_DECODER_BASED_FALLBACK,
                CarbonCommonConstants.LOCAL_DICTIONARY_DECODER_BASED_FALLBACK_DEFAULT)),
        model.getColumnLocalDictGenMap());
  }

  public void clear() {
    currentSize = 0;
    encodedBlocklet.clear();
  }

  public void addPage(TablePage rawTablePage) {
    EncodedTablePage encodedTablePage = rawTablePage.getEncodedTablePage();
    currentSize += encodedTablePage.getEncodedSize();
    encodedBlocklet.addEncodedTablePage(encodedTablePage);
  }

  public long getSize() {
    // increasing it by 15 percent for data chunk 3 of each column each page
    return currentSize + ((currentSize * 15) / 100);
  }

  public int getNumberOfPagesAdded() {
    return encodedBlocklet.getNumberOfPages();
  }

  public int getTotalRows() {
    return encodedBlocklet.getBlockletSize();
  }

  public EncodedBlocklet getEncodedBlocklet() {
    return encodedBlocklet;
  }
}
