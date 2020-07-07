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

import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;

/**
 * Table page that after encoding and compression.
 */
public class EncodedTablePage {

  // encoded data and metadata for each dimension column
  private EncodedColumnPage[] dimensionPages;

  // encoded data and metadata for each measure column
  private EncodedColumnPage[] measurePages;

  // number of row in this page
  private int pageSize;

  // size in bytes of all encoded columns (including data and metadata)
  private int encodedSize;

  public static EncodedTablePage newInstance(int pageSize,
      EncodedColumnPage[] dimensionPages, EncodedColumnPage[] measurePages) {
    return new EncodedTablePage(pageSize, dimensionPages, measurePages);
  }

  private EncodedTablePage(int pageSize,
      EncodedColumnPage[] dimensionPages, EncodedColumnPage[] measurePages) {
    this.dimensionPages = dimensionPages;
    this.measurePages = measurePages;
    this.pageSize = pageSize;
    this.encodedSize = calculatePageSize(dimensionPages, measurePages);
  }

  // return size in bytes of this encoded page
  private int calculatePageSize(EncodedColumnPage[] dimensionPages,
      EncodedColumnPage[] measurePages) {
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
