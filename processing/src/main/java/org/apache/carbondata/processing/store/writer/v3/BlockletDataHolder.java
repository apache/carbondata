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

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.datastore.page.EncodedTablePage;
import org.apache.carbondata.processing.store.TablePage;

public class BlockletDataHolder {
  private List<EncodedTablePage> encodedTablePage;
  private List<TablePage> rawTablePages;
  private long currentSize;

  public BlockletDataHolder() {
    this.encodedTablePage = new ArrayList<>();
    this.rawTablePages = new ArrayList<>();
  }

  public void clear() {
    encodedTablePage.clear();
    rawTablePages.clear();
    currentSize = 0;
  }

  public void addPage(TablePage rawTablePage) {
    EncodedTablePage encodedTablePage = rawTablePage.getEncodedTablePage();
    this.encodedTablePage.add(encodedTablePage);
    this.rawTablePages.add(rawTablePage);
    currentSize += encodedTablePage.getEncodedSize();
  }

  public long getSize() {
    // increasing it by 15 percent for data chunk 3 of each column each page
    return currentSize + ((currentSize * 15) / 100);
  }

  public int getNumberOfPagesAdded() {
    return encodedTablePage.size();
  }

  public int getTotalRows() {
    int rows = 0;
    for (EncodedTablePage nh : encodedTablePage) {
      rows += nh.getPageSize();
    }
    return rows;
  }

  public List<EncodedTablePage> getEncodedTablePages() {
    return encodedTablePage;
  }

  public List<TablePage> getRawTablePages() {
    return rawTablePages;
  }
}
