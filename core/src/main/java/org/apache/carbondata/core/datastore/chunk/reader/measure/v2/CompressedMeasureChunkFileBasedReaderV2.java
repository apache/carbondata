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
package org.apache.carbondata.core.datastore.chunk.reader.measure.v2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.reader.measure.AbstractMeasureChunkReaderV2V3Format;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.DataChunk2;

/**
 * Class to read the measure column data for version 2
 */
public class CompressedMeasureChunkFileBasedReaderV2 extends AbstractMeasureChunkReaderV2V3Format {

  /**
   * Constructor to get minimum parameter to create instance of this class
   *
   * @param blockletInfo BlockletInfo
   * @param filePath     file from which data will be read
   */
  public CompressedMeasureChunkFileBasedReaderV2(final BlockletInfo blockletInfo,
      final String filePath) {
    super(blockletInfo, filePath);
  }

  @Override
  public MeasureRawColumnChunk readRawMeasureChunk(FileReader fileReader, int columnIndex)
      throws IOException {
    int dataLength = 0;
    if (measureColumnChunkOffsets.size() - 1 == columnIndex) {
      DataChunk2 metadataChunk = null;
      synchronized (fileReader) {
        metadataChunk = CarbonUtil.readDataChunk(ByteBuffer.wrap(fileReader
                .readByteArray(filePath, measureColumnChunkOffsets.get(columnIndex),
                    measureColumnChunkLength.get(columnIndex))), 0,
            measureColumnChunkLength.get(columnIndex));
      }
      dataLength = measureColumnChunkLength.get(columnIndex) + metadataChunk.data_page_length;
    } else {
      long currentMeasureOffset = measureColumnChunkOffsets.get(columnIndex);
      dataLength = (int) (measureColumnChunkOffsets.get(columnIndex + 1) - currentMeasureOffset);
    }
    ByteBuffer buffer = null;
    synchronized (fileReader) {
      buffer = fileReader
          .readByteBuffer(filePath, measureColumnChunkOffsets.get(columnIndex), dataLength);
    }
    MeasureRawColumnChunk rawColumnChunk =
        new MeasureRawColumnChunk(columnIndex, buffer, 0, dataLength, this);
    rawColumnChunk.setFileReader(fileReader);
    rawColumnChunk.setPagesCount(1);
    rawColumnChunk.setRowCount(new int[] { numberOfRows });
    return rawColumnChunk;
  }

  /**
   * Below method will be used to read measure chunk data in group.
   * This method will be useful to avoid multiple IO while reading the
   * data from
   *
   * @param fileReader               file reader to read the data
   * @param startColumnIndex first column blocklet index to be read
   * @param endColumnIndex   end column blocklet index to be read
   * @return measure raw chunkArray
   * @throws IOException
   */
  protected MeasureRawColumnChunk[] readRawMeasureChunksInGroup(FileReader fileReader,
      int startColumnIndex, int endColumnIndex) throws IOException {
    long currentMeasureOffset = measureColumnChunkOffsets.get(startColumnIndex);
    ByteBuffer buffer = null;
    synchronized (fileReader) {
      buffer = fileReader.readByteBuffer(filePath, currentMeasureOffset,
          (int) (measureColumnChunkOffsets.get(endColumnIndex + 1) - currentMeasureOffset));
    }
    MeasureRawColumnChunk[] dataChunks =
        new MeasureRawColumnChunk[endColumnIndex - startColumnIndex + 1];
    int runningLength = 0;
    int index = 0;
    for (int i = startColumnIndex; i <= endColumnIndex; i++) {
      int currentLength =
          (int) (measureColumnChunkOffsets.get(i + 1) - measureColumnChunkOffsets.get(i));
      MeasureRawColumnChunk measureRawColumnChunk =
          new MeasureRawColumnChunk(i, buffer, runningLength, currentLength, this);
      measureRawColumnChunk.setRowCount(new int[] { numberOfRows });
      measureRawColumnChunk.setFileReader(fileReader);
      measureRawColumnChunk.setPagesCount(1);
      dataChunks[index] = measureRawColumnChunk;
      runningLength += currentLength;
      index++;
    }
    return dataChunks;
  }

  public ColumnPage decodeColumnPage(MeasureRawColumnChunk measureRawColumnChunk,
      int pageNumber) throws IOException, MemoryException {
    int copyPoint = (int) measureRawColumnChunk.getOffSet();
    int blockIndex = measureRawColumnChunk.getColumnIndex();
    ByteBuffer rawData = measureRawColumnChunk.getRawData();
    DataChunk2 measureColumnChunk = CarbonUtil.readDataChunk(rawData, copyPoint,
        measureColumnChunkLength.get(blockIndex));
    copyPoint += measureColumnChunkLength.get(blockIndex);

    ColumnPage page = decodeMeasure(measureRawColumnChunk, measureColumnChunk, copyPoint);
    page.setNullBits(getNullBitSet(measureColumnChunk.presence));
    return page;
  }

  protected ColumnPage decodeMeasure(MeasureRawColumnChunk measureRawColumnChunk,
      DataChunk2 measureColumnChunk, int copyPoint) throws MemoryException, IOException {
    assert (measureColumnChunk.getEncoder_meta().size() > 0);
    List<ByteBuffer> encoder_meta = measureColumnChunk.getEncoder_meta();
    byte[] encodedMeta = encoder_meta.get(0).array();

    ValueEncoderMeta meta = CarbonUtil.deserializeEncoderMetaV2(encodedMeta);
    ColumnPageDecoder codec = encodingFactory.createDecoderLegacy(meta);
    byte[] rawData = measureRawColumnChunk.getRawData().array();
    return codec.decode(rawData, copyPoint, measureColumnChunk.data_page_length);
  }
}
