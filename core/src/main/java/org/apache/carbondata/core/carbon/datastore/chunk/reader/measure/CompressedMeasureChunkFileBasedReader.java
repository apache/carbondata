/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.core.carbon.datastore.chunk.reader.measure;

import java.util.List;

import org.apache.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.carbon.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.datachunk.DataChunk;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * Compressed measure chunk reader
 */
public class CompressedMeasureChunkFileBasedReader extends AbstractMeasureChunkReader {

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
  public CompressedMeasureChunkFileBasedReader(final BlockletInfo blockletInfo,
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
      final int[][] blockIndexes) {
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
      final int blockIndex) {
    MeasureColumnDataChunk datChunk = new MeasureColumnDataChunk();
    // create a new uncompressor
    final ValueCompressionModel compressionModel = CarbonUtil
        .getValueCompressionModel(measureColumnChunks.get(blockIndex).getValueEncoderMeta());
    UnCompressValue values =
        compressionModel.getUnCompressValues()[0].getNew().getCompressorObject();
    // create a new uncompressor
    // read data from file and set to uncompressor
    // read data from file and set to uncompressor
    values.setValue(fileReader
        .readByteArray(filePath, measureColumnChunks.get(blockIndex).getDataPageOffset(),
            measureColumnChunks.get(blockIndex).getDataPageLength()));
    // get the data holder after uncompressing
    CarbonReadDataHolder measureDataHolder =
        values.uncompress(compressionModel.getChangedDataType()[0])
            .getValues(compressionModel.getDecimal()[0], compressionModel.getMaxValue()[0]);
    // set the data chunk
    datChunk.setMeasureDataHolder(measureDataHolder);
    // set the enun value indexes
    datChunk
        .setNullValueIndexHolder(measureColumnChunks.get(blockIndex).getNullValueIndexForColumn());
    return datChunk;
  }

}
