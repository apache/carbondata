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
package org.apache.carbondata.core.datastore.chunk.reader;

import org.apache.carbondata.core.datastore.chunk.reader.dimension.v1.CompressedDimensionChunkFileBasedReaderV1;
import org.apache.carbondata.core.datastore.chunk.reader.dimension.v2.CompressedDimensionChunkFileBasedReaderV2;
import org.apache.carbondata.core.datastore.chunk.reader.dimension.v3.CompressedDimChunkFileBasedPageLevelReaderV3;
import org.apache.carbondata.core.datastore.chunk.reader.dimension.v3.CompressedDimensionChunkFileBasedReaderV3;
import org.apache.carbondata.core.datastore.chunk.reader.measure.v1.CompressedMeasureChunkFileBasedReaderV1;
import org.apache.carbondata.core.datastore.chunk.reader.measure.v2.CompressedMeasureChunkFileBasedReaderV2;
import org.apache.carbondata.core.datastore.chunk.reader.measure.v3.CompressedMeasureChunkFileBasedReaderV3;
import org.apache.carbondata.core.datastore.chunk.reader.measure.v3.CompressedMsrChunkFileBasedPageLevelReaderV3;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;

/**
 * Factory class to get the data reader instance based on version
 */
public class CarbonDataReaderFactory {

  /**
   * static instance
   */
  private static final CarbonDataReaderFactory CARBON_DATA_READER_FACTORY =
      new CarbonDataReaderFactory();

  /**
   * private constructor
   */
  private CarbonDataReaderFactory() {

  }

  /**
   * To get the instance of the reader factor
   *
   * @return reader factory
   */
  public static CarbonDataReaderFactory getInstance() {
    return CARBON_DATA_READER_FACTORY;
  }

  /**
   * Below method will be used to get the dimension column chunk reader based on version number
   *
   * @param version             reader version
   * @param blockletInfo        blocklet info
   * @param eachColumnValueSize size of each dimension column
   * @param filePath            carbon data file path
   * @return dimension column data reader based on version number
   */
  public DimensionColumnChunkReader getDimensionColumnChunkReader(ColumnarFormatVersion version,
      BlockletInfo blockletInfo, int[] eachColumnValueSize, String filePath,
      boolean readPagebyPage) {
    switch (version) {
      case V1:
        return new CompressedDimensionChunkFileBasedReaderV1(blockletInfo, eachColumnValueSize,
            filePath);
      case V2:
        return new CompressedDimensionChunkFileBasedReaderV2(blockletInfo, eachColumnValueSize,
            filePath);
      case V3:
        if (readPagebyPage) {
          return new CompressedDimChunkFileBasedPageLevelReaderV3(blockletInfo, eachColumnValueSize,
              filePath);
        } else {
          return new CompressedDimensionChunkFileBasedReaderV3(blockletInfo, eachColumnValueSize,
              filePath);
        }
      default:
        throw new UnsupportedOperationException("Unsupported columnar format version: " + version);
    }
  }

  /**
   * Below method will be used to get the measure column chunk reader based version number
   *
   * @param version      reader version
   * @param blockletInfo blocklet info
   * @param filePath     carbon data file path
   * @return measure column data reader based on version number
   */
  public MeasureColumnChunkReader getMeasureColumnChunkReader(ColumnarFormatVersion version,
      BlockletInfo blockletInfo, String filePath, boolean readPagebyPage) {
    switch (version) {
      case V1:
        return new CompressedMeasureChunkFileBasedReaderV1(blockletInfo, filePath);
      case V2:
        return new CompressedMeasureChunkFileBasedReaderV2(blockletInfo, filePath);
      case V3:
        if (readPagebyPage) {
          return new CompressedMsrChunkFileBasedPageLevelReaderV3(blockletInfo, filePath);
        } else {
          return new CompressedMeasureChunkFileBasedReaderV3(blockletInfo, filePath);
        }
      default:
        throw new UnsupportedOperationException("Unsupported columnar format version: " + version);
    }
  }
}
