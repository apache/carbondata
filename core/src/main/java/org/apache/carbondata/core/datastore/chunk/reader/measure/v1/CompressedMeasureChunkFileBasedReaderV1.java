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
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.reader.measure.AbstractMeasureChunkReader;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.datachunk.DataChunk;

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
    super(filePath, blockletInfo.getNumberOfRows());
    this.measureColumnChunks = blockletInfo.getMeasureColumnChunk();
  }

  /**
   * Method to read the blocks data based on block indexes
   *
   * @param fileReader   file reader to read the blocks
   * @param columnIndexRange blocks to be read
   * @return measure data chunks
   */
  @Override public MeasureRawColumnChunk[] readRawMeasureChunks(FileReader fileReader,
      int[][] columnIndexRange) throws IOException {
    MeasureRawColumnChunk[] datChunk = new MeasureRawColumnChunk[measureColumnChunks.size()];
    for (int i = 0; i < columnIndexRange.length; i++) {
      for (int j = columnIndexRange[i][0]; j <= columnIndexRange[i][1]; j++) {
        datChunk[j] = readRawMeasureChunk(fileReader, j);
      }
    }
    return datChunk;
  }

  /**
   * Method to read the blocks data based on block index
   *
   * @param fileReader file reader to read the blocks
   * @param columnIndex column to be read
   * @return measure data chunk
   */
  @Override public MeasureRawColumnChunk readRawMeasureChunk(FileReader fileReader, int columnIndex)
      throws IOException {
    DataChunk dataChunk = measureColumnChunks.get(columnIndex);
    ByteBuffer buffer = fileReader
        .readByteBuffer(filePath, dataChunk.getDataPageOffset(), dataChunk.getDataPageLength());
    MeasureRawColumnChunk rawColumnChunk = new MeasureRawColumnChunk(columnIndex, buffer, 0,
        dataChunk.getDataPageLength(), this);
    rawColumnChunk.setFileReader(fileReader);
    rawColumnChunk.setPagesCount(1);
    rawColumnChunk.setRowCount(new int[] { numberOfRows });
    return rawColumnChunk;
  }

  @Override
  public ColumnPage decodeColumnPage(MeasureRawColumnChunk measureRawColumnChunk,
      int pageNumber) throws IOException, MemoryException {
    int blockIndex = measureRawColumnChunk.getColumnIndex();
    DataChunk dataChunk = measureColumnChunks.get(blockIndex);
    ValueEncoderMeta meta = dataChunk.getValueEncoderMeta().get(0);
    ColumnPageDecoder codec = encodingFactory.createDecoderLegacy(meta);
    ColumnPage decodedPage = codec.decode(measureRawColumnChunk.getRawData().array(),
        (int) measureRawColumnChunk.getOffSet(), dataChunk.getDataPageLength());
    decodedPage.setNullBits(dataChunk.getNullValueIndexForColumn());

    return decodedPage;
  }
}
