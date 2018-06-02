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
package org.apache.carbondata.core.scan.filter.executer;

import java.io.IOException;

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;

/**
 * This class is a util to read RawBlockletColumnChunks in order to eliminate duplicate code
 */
public class RawColumnChunkUtil {

  public static void readDimensionRawColumnChunk(RawBlockletColumnChunks rawBlockletColumnChunks,
      DimColumnResolvedFilterInfo dimColEvaluatorInfo,
      SegmentProperties segmentProperties) throws IOException {
    int chunkIndex = segmentProperties.getDimensionOrdinalToChunkMapping().get(
        dimColEvaluatorInfo.getColumnIndex());
    readDimensionRawColumnChunk(rawBlockletColumnChunks, dimColEvaluatorInfo, chunkIndex);
  }

  public static void readDimensionRawColumnChunk(RawBlockletColumnChunks rawBlockletColumnChunks,
      DimColumnResolvedFilterInfo dimColEvaluatorInfo, int chunkIndex) throws IOException {
    if (null == rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex]) {
      DimensionRawColumnChunk rawColumnChunk = rawBlockletColumnChunks.getDataBlock()
          .readDimensionChunk(rawBlockletColumnChunks.getFileReader(), chunkIndex);
      rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex] = rawColumnChunk;
    }
  }

  public static void readMeasureRawColumnChunk(RawBlockletColumnChunks rawBlockletColumnChunks,
      MeasureColumnResolvedFilterInfo msrColEvaluatorInfo,
      SegmentProperties segmentProperties) throws IOException {
    int chunkIndex = segmentProperties.getMeasuresOrdinalToChunkMapping().get(
        msrColEvaluatorInfo.getColumnIndex());
    readMeasureRawColumnChunk(rawBlockletColumnChunks, chunkIndex);
  }

  public static void readMeasureRawColumnChunk(RawBlockletColumnChunks rawBlockletColumnChunks,
      int chunkIndex) throws IOException {
    if (null == rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex]) {
      rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex] =
          rawBlockletColumnChunks.getDataBlock()
              .readMeasureChunk(rawBlockletColumnChunks.getFileReader(), chunkIndex);
    }
  }
}
