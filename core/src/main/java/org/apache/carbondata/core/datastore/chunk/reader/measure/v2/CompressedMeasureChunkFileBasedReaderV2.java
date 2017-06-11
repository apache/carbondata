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
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.reader.measure.AbstractMeasureChunkReaderV2V3Format;
import org.apache.carbondata.core.datastore.compression.ValueCompressionHolder;
import org.apache.carbondata.core.datastore.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.datastore.page.statistics.MeasurePageStatsVO;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.CompressionFinder;
import org.apache.carbondata.core.util.ValueCompressionUtil;
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

  @Override public MeasureRawColumnChunk readRawMeasureChunk(FileHolder fileReader, int blockIndex)
      throws IOException {
    int dataLength = 0;
    if (measureColumnChunkOffsets.size() - 1 == blockIndex) {
      dataLength = measureColumnChunkLength.get(blockIndex);
    } else {
      long currentMeasureOffset = measureColumnChunkOffsets.get(blockIndex);
      dataLength = (int) (measureColumnChunkOffsets.get(blockIndex + 1) - currentMeasureOffset);
    }
    ByteBuffer buffer = null;
    synchronized (fileReader) {
      buffer = fileReader
          .readByteBuffer(filePath, measureColumnChunkOffsets.get(blockIndex), dataLength);
    }
    MeasureRawColumnChunk rawColumnChunk =
        new MeasureRawColumnChunk(blockIndex, buffer, 0, dataLength, this);
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
   * @param startColumnBlockletIndex first column blocklet index to be read
   * @param endColumnBlockletIndex   end column blocklet index to be read
   * @return measure raw chunkArray
   * @throws IOException
   */
  protected MeasureRawColumnChunk[] readRawMeasureChunksInGroup(FileHolder fileReader,
      int startColumnBlockletIndex, int endColumnBlockletIndex) throws IOException {
    long currentMeasureOffset = measureColumnChunkOffsets.get(startColumnBlockletIndex);
    ByteBuffer buffer = null;
    synchronized (fileReader) {
      buffer = fileReader.readByteBuffer(filePath, currentMeasureOffset,
          (int) (measureColumnChunkOffsets.get(endColumnBlockletIndex + 1) - currentMeasureOffset));
    }
    MeasureRawColumnChunk[] dataChunks =
        new MeasureRawColumnChunk[endColumnBlockletIndex - startColumnBlockletIndex + 1];
    int runningLength = 0;
    int index = 0;
    for (int i = startColumnBlockletIndex; i <= endColumnBlockletIndex; i++) {
      int currentLength =
          (int) (measureColumnChunkOffsets.get(i + 1) - measureColumnChunkOffsets.get(i));
      MeasureRawColumnChunk measureRawColumnChunk =
          new MeasureRawColumnChunk(i, buffer, runningLength, currentLength, this);
      measureRawColumnChunk.setFileReader(fileReader);
      measureRawColumnChunk.setRowCount(new int[] { numberOfRows });
      measureRawColumnChunk.setPagesCount(1);
      dataChunks[index] = measureRawColumnChunk;
      runningLength += currentLength;
      index++;
    }
    return dataChunks;
  }

  public MeasureColumnDataChunk convertToMeasureChunk(MeasureRawColumnChunk measureRawColumnChunk,
      int pageNumber) throws IOException {
    MeasureColumnDataChunk datChunk = new MeasureColumnDataChunk();
    DataChunk2 measureColumnChunk = null;
    int copyPoint = measureRawColumnChunk.getOffSet();
    int blockIndex = measureRawColumnChunk.getBlockletId();
    ByteBuffer rawData = measureRawColumnChunk.getRawData();
    if (measureColumnChunkOffsets.size() - 1 == blockIndex) {
      measureColumnChunk =
          CarbonUtil.readDataChunk(rawData, copyPoint, measureColumnChunkLength.get(blockIndex));
      synchronized (measureRawColumnChunk.getFileReader()) {
        rawData = measureRawColumnChunk.getFileReader().readByteBuffer(filePath,
            measureColumnChunkOffsets.get(blockIndex) + measureColumnChunkLength.get(blockIndex),
            measureColumnChunk.data_page_length);
      }
    } else {
      measureColumnChunk =
          CarbonUtil.readDataChunk(rawData, copyPoint, measureColumnChunkLength.get(blockIndex));
      copyPoint += measureColumnChunkLength.get(blockIndex);
    }
    List<ValueEncoderMeta> valueEncodeMeta = new ArrayList<>();
    for (int i = 0; i < measureColumnChunk.getEncoder_meta().size(); i++) {
      valueEncodeMeta.add(
          CarbonUtil.deserializeEncoderMetaV2(measureColumnChunk.getEncoder_meta().get(i).array()));
    }

    MeasurePageStatsVO stats = CarbonUtil.getMeasurePageStats(valueEncodeMeta);
    int measureCount = valueEncodeMeta.size();
    CompressionFinder[] finders = new CompressionFinder[measureCount];
    DataType[] convertedType = new DataType[measureCount];
    for (int i = 0; i < measureCount; i++) {
      CompressionFinder compresssionFinder =
          ValueCompressionUtil.getCompressionFinder(stats.getMax(i), stats.getMin(i),
              stats.getDecimal(i), stats.getDataType(i), stats.getDataTypeSelected(i));
      finders[i] = compresssionFinder;
      convertedType[i] = compresssionFinder.getConvertedDataType();
    }

    ValueCompressionHolder values = ValueCompressionUtil.getValueCompressionHolder(finders)[0];

    // uncompress
    values.uncompress(convertedType[0], rawData.array(), copyPoint,
        measureColumnChunk.data_page_length, stats.getDecimal(0),
        stats.getMax(0), numberOfRows);

    CarbonReadDataHolder measureDataHolder = new CarbonReadDataHolder(values);

    // set the data chunk
    datChunk.setMeasureDataHolder(measureDataHolder);

    // set the enun value indexes
    datChunk.setNullValueIndexHolder(getPresenceMeta(measureColumnChunk.presence));
    return datChunk;
  }
}
