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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.columnar.IndexStorage;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.EncodedDimensionPage;
import org.apache.carbondata.core.datastore.page.encoding.EncodedMeasurePage;
import org.apache.carbondata.core.datastore.page.key.TablePageKey;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * Table page that after encoding and compression.
 */
public class EncodedTablePage {

  // encoded data and metadata for each dimension column
  private EncodedDimensionPage[] dimensions;

  // encoded data and metadata for each measure column
  private EncodedMeasurePage[] measures;

  // key of this page
  private TablePageKey pageKey;

  // number of row in this page
  private int pageSize;

  // true if it is last page of all input rows
  private boolean isLastPage;

  // size in bytes of all encoded columns (including data and metadate)
  private int encodedSize;

  public static EncodedTablePage newEmptyInstance() {
    EncodedTablePage page = new EncodedTablePage();
    page.pageSize = 0;
    page.encodedSize = 0;
    page.measures = new EncodedMeasurePage[0];
    page.dimensions = new EncodedDimensionPage[0];
    return page;
  }

  public static EncodedTablePage newInstance(int pageSize,
      EncodedDimensionPage[] dimensions, EncodedMeasurePage[] measures,
      TablePageKey tablePageKey) {
    return new EncodedTablePage(pageSize, dimensions, measures, tablePageKey);
  }

  private EncodedTablePage() {
  }

  private EncodedTablePage(int pageSize, EncodedDimensionPage[] encodedDimensions,
      EncodedMeasurePage[] encodedMeasures, TablePageKey tablePageKey) {
    this.dimensions = encodedDimensions;
    this.measures = encodedMeasures;
    this.pageSize = pageSize;
    this.pageKey = tablePageKey;
    this.encodedSize = calculatePageSize(encodedDimensions, encodedMeasures);
  }

  // return size in bytes of this encoded page
  private int calculatePageSize(EncodedDimensionPage[] encodedDimensions,
      EncodedMeasurePage[] encodedMeasures) {
    int size = 0;
    int totalEncodedDimensionDataLength = 0;
    int totalEncodedMeasuredDataLength = 0;
    // add row id index length
    for (EncodedDimensionPage dimension : dimensions) {
      IndexStorage indexStorage = dimension.getIndexStorage();
      if (!indexStorage.isAlreadySorted()) {
        size += indexStorage.getRowIdPageLengthInBytes() +
            indexStorage.getRowIdRlePageLengthInBytes() +
            CarbonCommonConstants.INT_SIZE_IN_BYTE;
      }
      if (indexStorage.getDataRlePageLengthInBytes() > 0) {
        size += indexStorage.getDataRlePageLengthInBytes();
      }
      totalEncodedDimensionDataLength += dimension.getEncodedData().length;
    }
    for (EncodedColumnPage measure : measures) {
      size += measure.getEncodedData().length;
    }

    for (EncodedDimensionPage encodedDimension : encodedDimensions) {
      size += CarbonUtil.getByteArray(encodedDimension.getDataChunk2()).length;
    }
    for (EncodedMeasurePage encodedMeasure : encodedMeasures) {
      size += CarbonUtil.getByteArray(encodedMeasure.getDataChunk2()).length;
    }
    size += totalEncodedDimensionDataLength + totalEncodedMeasuredDataLength;
    return size;
  }

  public int getEncodedSize() {
    return encodedSize;
  }

  public int getPageSize() {
    return pageSize;
  }

  public int getNumDimensions() {
    return dimensions.length;
  }

  public int getNumMeasures() {
    return measures.length;
  }

  public TablePageKey getPageKey() {
    return pageKey;
  }

  public boolean isLastPage() {
    return isLastPage;
  }

  public void setIsLastPage(boolean isWriteAll) {
    this.isLastPage = isWriteAll;
  }

  public EncodedMeasurePage getMeasure(int measureIndex) {
    return measures[measureIndex];
  }

  public EncodedMeasurePage[] getMeasures() {
    return measures;
  }

  public EncodedDimensionPage getDimension(int dimensionIndex) {
    return dimensions[dimensionIndex];
  }

  public EncodedDimensionPage[] getDimensions() {
    return dimensions;
  }
}
