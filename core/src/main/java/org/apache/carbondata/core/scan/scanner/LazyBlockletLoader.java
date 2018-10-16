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

import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.chunk.AbstractRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

/**
 * Reads the blocklet column chunks lazily, it means it reads the column chunks from disk when
 * execution engine wants to access it.
 * It is useful in case of filter queries with high cardinality columns.
 */
public class LazyBlockletLoader {

  private RawBlockletColumnChunks rawBlockletColumnChunks;

  private BlockExecutionInfo blockExecutionInfo;

  private LazyChunkWrapper[] dimLazyWrapperChunks;

  private LazyChunkWrapper[] msrLazyWrapperChunks;

  private boolean isLoaded;

  private QueryStatisticsModel queryStatisticsModel;

  public LazyBlockletLoader(RawBlockletColumnChunks rawBlockletColumnChunks,
      BlockExecutionInfo blockExecutionInfo, DimensionRawColumnChunk[] dimensionRawColumnChunks,
      MeasureRawColumnChunk[] measureRawColumnChunks, QueryStatisticsModel queryStatisticsModel) {
    this.rawBlockletColumnChunks = rawBlockletColumnChunks;
    this.blockExecutionInfo = blockExecutionInfo;
    this.dimLazyWrapperChunks = new LazyChunkWrapper[dimensionRawColumnChunks.length];
    this.msrLazyWrapperChunks = new LazyChunkWrapper[measureRawColumnChunks.length];
    for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
      dimLazyWrapperChunks[i] = new LazyChunkWrapper(dimensionRawColumnChunks[i]);
    }
    for (int i = 0; i < measureRawColumnChunks.length; i++) {
      msrLazyWrapperChunks[i] = new LazyChunkWrapper(measureRawColumnChunks[i]);
    }
    this.queryStatisticsModel = queryStatisticsModel;
  }

  public void load() throws IOException {
    if (!isLoaded) {
      readBlocklet();
    }
  }

  public LazyChunkWrapper getLazyChunkWrapper(int index, boolean isMeasure) {
    if (isMeasure) {
      return msrLazyWrapperChunks[index];
    } else {
      return dimLazyWrapperChunks[index];
    }
  }

  private synchronized void readBlocklet() throws IOException {
    FileReader fileReader = rawBlockletColumnChunks.getFileReader();

    long readTime = System.currentTimeMillis();
    int[][] allSelectedDimensionColumnIndexRange =
        blockExecutionInfo.getAllSelectedDimensionColumnIndexRange();
    DimensionRawColumnChunk[] projectionListDimensionChunk = rawBlockletColumnChunks.getDataBlock()
        .readDimensionChunks(fileReader, allSelectedDimensionColumnIndexRange);
    for (int[] columnIndexRange : allSelectedDimensionColumnIndexRange) {
      for (int i = columnIndexRange[0]; i < columnIndexRange[1] + 1; i++) {
        dimLazyWrapperChunks[i].rawColumnChunk = projectionListDimensionChunk[i];
      }
    }
    /*
     * in case projection if the projected dimension are not loaded in the dimensionColumnDataChunk
     * then loading them
     */
    int[] projectionListDimensionIndexes = blockExecutionInfo.getProjectionListDimensionIndexes();
    for (int projectionListDimensionIndex : projectionListDimensionIndexes) {
      if (null == dimLazyWrapperChunks[projectionListDimensionIndex].rawColumnChunk) {
        dimLazyWrapperChunks[projectionListDimensionIndex].rawColumnChunk =
            rawBlockletColumnChunks.getDataBlock()
                .readDimensionChunk(fileReader, projectionListDimensionIndex);
      }
    }


    int[][] allSelectedMeasureColumnIndexRange =
        blockExecutionInfo.getAllSelectedMeasureIndexRange();
    MeasureRawColumnChunk[] projectionListMeasureChunk = rawBlockletColumnChunks.getDataBlock()
        .readMeasureChunks(fileReader, allSelectedMeasureColumnIndexRange);
    for (int[] columnIndexRange : allSelectedMeasureColumnIndexRange) {
      for (int i = columnIndexRange[0]; i < columnIndexRange[1] + 1; i++) {
        msrLazyWrapperChunks[i].rawColumnChunk = projectionListMeasureChunk[i];
      }
    }
    /*
     * in case projection if the projected measure are not loaded in the ColumnPage
     * then loading them
     */
    int[] projectionListMeasureIndexes = blockExecutionInfo.getProjectionListMeasureIndexes();
    for (int projectionListMeasureIndex : projectionListMeasureIndexes) {
      if (null == msrLazyWrapperChunks[projectionListMeasureIndex].rawColumnChunk) {
        msrLazyWrapperChunks[projectionListMeasureIndex].rawColumnChunk =
            rawBlockletColumnChunks.getDataBlock()
                .readMeasureChunk(fileReader, projectionListMeasureIndex);
      }
    }
    readTime = System.currentTimeMillis() - readTime;
    QueryStatistic time = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.READ_BLOCKlET_TIME);
    time.addCountStatistic(QueryStatisticsConstants.READ_BLOCKlET_TIME,
        time.getCount() + readTime);
    isLoaded = true;
  }

  public QueryStatisticsModel getQueryStatisticsModel() {
    return queryStatisticsModel;
  }

  public static class LazyChunkWrapper {

    private AbstractRawColumnChunk rawColumnChunk;

    public LazyChunkWrapper(AbstractRawColumnChunk rawColumnChunk) {
      this.rawColumnChunk = rawColumnChunk;
    }

    public AbstractRawColumnChunk getRawColumnChunk() {
      return rawColumnChunk;
    }

    public void setRawColumnChunk(AbstractRawColumnChunk rawColumnChunk) {
      this.rawColumnChunk = rawColumnChunk;
    }
  }

}
