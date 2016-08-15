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
import org.apache.carbondata.core.carbon.metadata.blocklet.datachunk.DataChunk;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;

/**
 * Compressed measure chunk reader
 */
public class CompressedMeasureChunkFileBasedReader extends AbstractMeasureChunkReader {

  /**
   * Constructor to get minimum parameter to create instance of this class
   *
   * @param measureColumnChunk measure chunk metadata
   * @param compression        model metadata which was to used to compress and uncompress
   *                           the measure value
   * @param filePath           file from which data will be read
   */
  public CompressedMeasureChunkFileBasedReader(List<DataChunk> measureColumnChunk,
      ValueCompressionModel compressionModel, String filePath) {
    super(measureColumnChunk, compressionModel, filePath, false);
  }

  /**
   * Method to read the blocks data based on block indexes
   *
   * @param fileReader   file reader to read the blocks
   * @param blockIndexes blocks to be read
   * @return measure data chunks
   */
  @Override public MeasureColumnDataChunk[] readMeasureChunks(FileHolder fileReader,
      int... blockIndexes) {
    MeasureColumnDataChunk[] datChunk = new MeasureColumnDataChunk[values.length];
    for (int i = 0; i < blockIndexes.length; i++) {
      datChunk[blockIndexes[i]] = readMeasureChunk(fileReader, blockIndexes[i]);
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
  @Override public MeasureColumnDataChunk readMeasureChunk(FileHolder fileReader, int blockIndex) {
    MeasureColumnDataChunk datChunk = new MeasureColumnDataChunk();
    // create a new uncompressor
    ValueCompressonHolder.UnCompressValue copy = values[blockIndex].getNew();
    // read data from file and set to uncompressor
    copy.setValue(fileReader
        .readByteArray(filePath, measureColumnChunk.get(blockIndex).getDataPageOffset(),
            measureColumnChunk.get(blockIndex).getDataPageLength()));
    // get the data holder after uncompressing
    CarbonReadDataHolder measureDataHolder =
        copy.uncompress(compressionModel.getChangedDataType()[blockIndex])
            .getValues(compressionModel.getDecimal()[blockIndex],
                compressionModel.getMaxValue()[blockIndex]);
    // set the data chunk
    datChunk.setMeasureDataHolder(measureDataHolder);
    // set the enun value indexes
    datChunk
        .setNullValueIndexHolder(measureColumnChunk.get(blockIndex).getNullValueIndexForColumn());
    return datChunk;
  }

}
