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
package org.apache.carbondata.core.datastore.chunk.reader.measure.v1;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.reader.measure.AbstractMeasureChunkReader;
import org.apache.carbondata.core.datastore.compression.ReaderCompressModel;
import org.apache.carbondata.core.datastore.compression.ValueCompressionHolder;
import org.apache.carbondata.core.datastore.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.datachunk.DataChunk;
import org.apache.carbondata.core.util.ValueCompressionUtil;

/**
 * Compressed measure chunk reader
 */
public class CompressedMeasureChunkFileBasedReaderV1 extends AbstractMeasureChunkReader {

  /**
   * measure chunk have the information about the metadata present in the file
   */
  private final List<DataChunk> measureColumnChunks;

  /**
   * Constructor to get minimum parameter to create instance of this class
   *
   * @param blockletInfo BlockletInfo
   * @param filePath     file from which data will be read
   */
  public CompressedMeasureChunkFileBasedReaderV1(final BlockletInfo blockletInfo,
      final String filePath) {
    super(filePath);
    this.measureColumnChunks = blockletInfo.getMeasureColumnChunk();
  }

  /**
   * Method to read the blocks data based on block indexes
   *
   * @param fileReader   file reader to read the blocks
   * @param blockIndexes blocks to be read
   * @return measure data chunks
   */
  @Override public MeasureColumnDataChunk[] readMeasureChunks(final FileHolder fileReader,
      final int[][] blockIndexes) throws IOException {
    MeasureColumnDataChunk[] datChunk = new MeasureColumnDataChunk[measureColumnChunks.size()];
    for (int i = 0; i < blockIndexes.length; i++) {
      for (int j = blockIndexes[i][0]; j <= blockIndexes[i][1]; j++) {
        datChunk[j] = readMeasureChunk(fileReader, j);
      }
    }
    return datChunk;
  }

  /**
   * Method to read the blocks data based on block index
   *
   * @param fileReader file reader to read the blocks
   * @param blockIndex block to be read
   * @return measure data chunk
   */
  @Override public MeasureColumnDataChunk readMeasureChunk(final FileHolder fileReader,
      final int blockIndex) throws IOException {
    ValueEncoderMeta meta = measureColumnChunks.get(blockIndex).getValueEncoderMeta().get(0);
    ReaderCompressModel compressModel = ValueCompressionUtil.getReaderCompressModel(meta);

    ValueCompressionHolder values = compressModel.getValueCompressionHolder();
    byte[] dataPage = fileReader
            .readByteArray(filePath, measureColumnChunks.get(blockIndex).getDataPageOffset(),
                    measureColumnChunks.get(blockIndex).getDataPageLength());

    // unCompress data
    values.uncompress(compressModel.getConvertedDataType(), dataPage, 0,
            measureColumnChunks.get(blockIndex).getDataPageLength(), compressModel.getMantissa(),
            compressModel.getMaxValue());

    CarbonReadDataHolder measureDataHolder = new CarbonReadDataHolder(values);

    // create and set the data chunk
    MeasureColumnDataChunk datChunk = new MeasureColumnDataChunk();
    datChunk.setMeasureDataHolder(measureDataHolder);
    // set the enun value indexes
    datChunk
        .setNullValueIndexHolder(measureColumnChunks.get(blockIndex).getNullValueIndexForColumn());
    return datChunk;
  }

}
