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

package org.apache.carbondata.core.scan.scanner;

import java.io.IOException;

import org.apache.carbondata.core.datastore.ReusableDataBuffer;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

/**
 * Page loads lazily, it means it decompresses and fills the vector when execution engine wants
 * to access it.It is useful in case of filter queries with high cardinality columns.
 */
public class LazyPageLoader {

  private LazyBlockletLoader lazyBlockletLoader;

  private LazyBlockletLoader.LazyChunkWrapper lazyChunkWrapper;

  private boolean isMeasure;

  private int pageNumber;

  private ColumnVectorInfo vectorInfo;

  private QueryStatisticsModel queryStatisticsModel;

  private ReusableDataBuffer reusableDataBuffer;

  public LazyPageLoader(LazyBlockletLoader lazyBlockletLoader, int index, boolean isMeasure,
      int pageNumber, ColumnVectorInfo vectorInfo, ReusableDataBuffer reusableDataBuffer) {
    this.lazyBlockletLoader = lazyBlockletLoader;
    this.lazyChunkWrapper = lazyBlockletLoader.getLazyChunkWrapper(index, isMeasure);
    this.isMeasure = isMeasure;
    this.pageNumber = pageNumber;
    this.vectorInfo = vectorInfo;
    this.queryStatisticsModel = lazyBlockletLoader.getQueryStatisticsModel();
    this.reusableDataBuffer = reusableDataBuffer;
  }

  public void loadPage() {
    if (lazyChunkWrapper.getRawColumnChunk() == null) {
      try {
        lazyBlockletLoader.load();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    long startTime = System.currentTimeMillis();
    if (isMeasure) {
      ((MeasureRawColumnChunk) lazyChunkWrapper.getRawColumnChunk())
          .convertToColumnPageAndFillVector(pageNumber, vectorInfo, reusableDataBuffer);
    } else {
      ((DimensionRawColumnChunk) lazyChunkWrapper.getRawColumnChunk())
          .convertToDimColDataChunkAndFillVector(pageNumber, vectorInfo, reusableDataBuffer);
    }
    if (queryStatisticsModel.isEnabled()) {
      QueryStatistic pageUncompressTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
          .get(QueryStatisticsConstants.PAGE_UNCOMPRESS_TIME);
      pageUncompressTime.addCountStatistic(QueryStatisticsConstants.PAGE_UNCOMPRESS_TIME,
          pageUncompressTime.getCount() + (System.currentTimeMillis() - startTime));
    }
  }

}
