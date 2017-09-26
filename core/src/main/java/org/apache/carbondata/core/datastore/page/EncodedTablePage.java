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

import java.io.IOException;

import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.datastore.page.key.TablePageKey;

/**
 * Table page that after encoding and compression.
 */
public class EncodedTablePage {

  // encoded data and metadata for each dimension column
  private EncodedColumnPage[] dimensionPages;

  // encoded data and metadata for each measure column
  private EncodedColumnPage[] measurePages;

  // key of this page
  private TablePageKey pageKey;

  // number of row in this page
  private int pageSize;

  // size in bytes of all encoded columns (including data and metadate)
  private int encodedSize;

  public static EncodedTablePage newEmptyInstance() {
    EncodedTablePage page = new EncodedTablePage();
    page.pageSize = 0;
    page.encodedSize = 0;
    page.dimensionPages = new EncodedColumnPage[0];
    page.measurePages = new EncodedColumnPage[0];
    return page;
  }

  public static EncodedTablePage newInstance(int pageSize,
      EncodedColumnPage[] dimensionPages, EncodedColumnPage[] measurePages,
      TablePageKey tablePageKey) throws IOException {
    return new EncodedTablePage(pageSize, dimensionPages, measurePages, tablePageKey);
  }

  private EncodedTablePage() {
  }

  private EncodedTablePage(int pageSize,
      EncodedColumnPage[] dimensionPages, EncodedColumnPage[] measurePages,
      TablePageKey tablePageKey) throws IOException {
    this.dimensionPages = dimensionPages;
    this.measurePages = measurePages;
    this.pageSize = pageSize;
    this.pageKey = tablePageKey;
    this.encodedSize = calculatePageSize(dimensionPages, measurePages);
  }

  // return size in bytes of this encoded page
  private int calculatePageSize(EncodedColumnPage[] dimensionPages,
      EncodedColumnPage[] measurePages) throws IOException {
    int size = 0;
    for (EncodedColumnPage dimensionPage : dimensionPages) {
      size += dimensionPage.getTotalSerializedSize();
    }
    for (EncodedColumnPage measurePage : measurePages) {
      size += measurePage.getTotalSerializedSize();
    }
    return size;
  }

  public int getEncodedSize() {
    return encodedSize;
  }

  public int getPageSize() {
    return pageSize;
  }

  public int getNumDimensions() {
    return dimensionPages.length;
  }

  public int getNumMeasures() {
    return measurePages.length;
  }

  public TablePageKey getPageKey() {
    return pageKey;
  }

  public EncodedColumnPage getDimension(int dimensionIndex) {
    return dimensionPages[dimensionIndex];
  }

  public EncodedColumnPage getMeasure(int measureIndex) {
    return measurePages[measureIndex];
  }

  public EncodedColumnPage[] getDimensions() {
    return dimensionPages;
  }

  public EncodedColumnPage[] getMeasures() {
    return measurePages;
  }
}
