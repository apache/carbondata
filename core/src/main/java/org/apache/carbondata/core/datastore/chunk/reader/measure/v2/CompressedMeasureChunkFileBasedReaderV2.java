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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.reader.measure.AbstractMeasureChunkReader;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.compression.ValueCompressionHolder;
import org.apache.carbondata.core.datastore.compression.WriterCompressModel;
import org.apache.carbondata.core.datastore.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.datachunk.PresenceMeta;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.DataChunk2;

/**
 * Class to read the measure column data for version 2
 */
public class CompressedMeasureChunkFileBasedReaderV2 extends AbstractMeasureChunkReader {

  /**
   * measure column chunks offset
   */
  protected List<Long> measureColumnChunkOffsets;

  /**
   * measure column chunks length
   */
  protected List<Integer> measureColumnChunkLength;

  /**
   * Constructor to get minimum parameter to create instance of this class
   *
   * @param blockletInfo BlockletInfo
   * @param filePath     file from which data will be read
   */
  public CompressedMeasureChunkFileBasedReaderV2(final BlockletInfo blockletInfo,
      final String filePath) {
    super(filePath, blockletInfo.getNumberOfRows());
    this.measureColumnChunkOffsets = blockletInfo.getMeasureChunkOffsets();
    this.measureColumnChunkLength = blockletInfo.getMeasureChunksLength();
  }

  /**
   * Below method will be used to convert the thrift presence meta to wrapper
   * presence meta
   *
   * @param presentMetadataThrift
   * @return wrapper presence meta
   */
  protected static PresenceMeta getPresenceMeta(
      org.apache.carbondata.format.PresenceMeta presentMetadataThrift) {
    PresenceMeta presenceMeta = new PresenceMeta();
    presenceMeta.setRepresentNullValues(presentMetadataThrift.isRepresents_presence());
    presenceMeta.setBitSet(BitSet.valueOf(CompressorFactory.getInstance().getCompressor()
        .unCompressByte(presentMetadataThrift.getPresent_bit_stream())));
    return presenceMeta;
  }

  /**
   * Below method will be used to read the chunk based on block indexes
   * Reading logic of below method is: Except last column all the column chunk
   * can be read in group if not last column then read data of all the column
   * present in block index together then process it. For last column read is
   * separately and process
   *
   * @param fileReader   file reader to read the blocks from file
   * @param blockIndexes blocks range to be read
   * @return measure column chunks
   * @throws IOException
   */
  public MeasureRawColumnChunk[] readRawMeasureChunks(FileHolder fileReader, int[][] blockIndexes)
      throws IOException {
    // read the column chunk based on block index and add
    MeasureRawColumnChunk[] dataChunks =
        new MeasureRawColumnChunk[measureColumnChunkOffsets.size()];
    if (blockIndexes.length == 0) {
      return dataChunks;
    }
    MeasureRawColumnChunk[] groupChunk = null;
    int index = 0;
    for (int i = 0; i < blockIndexes.length - 1; i++) {
      index = 0;
      groupChunk = readRawMeasureChunksInGroup(fileReader, blockIndexes[i][0], blockIndexes[i][1]);
      for (int j = blockIndexes[i][0]; j <= blockIndexes[i][1]; j++) {
        dataChunks[j] = groupChunk[index++];
      }
    }
    if (blockIndexes[blockIndexes.length - 1][0] == measureColumnChunkOffsets.size() - 1) {
      dataChunks[blockIndexes[blockIndexes.length - 1][0]] =
          readRawMeasureChunk(fileReader, blockIndexes[blockIndexes.length - 1][0]);
    } else {
      groupChunk = readRawMeasureChunksInGroup(fileReader, blockIndexes[blockIndexes.length - 1][0],
          blockIndexes[blockIndexes.length - 1][1]);
      index = 0;
      for (int j = blockIndexes[blockIndexes.length - 1][0];
           j <= blockIndexes[blockIndexes.length - 1][1]; j++) {
        dataChunks[j] = groupChunk[index++];
      }
    }
    return dataChunks;
  }

  @Override public MeasureRawColumnChunk readRawMeasureChunk(FileHolder fileReader, int blockIndex)
      throws IOException {
    byte[] data;
    if (measureColumnChunkOffsets.size() - 1 == blockIndex) {
      data = fileReader.readByteArray(filePath, measureColumnChunkOffsets.get(blockIndex),
          measureColumnChunkLength.get(blockIndex));
    } else {
      long currentMeasureOffset = measureColumnChunkOffsets.get(blockIndex);
      data = fileReader.readByteArray(filePath, currentMeasureOffset,
          (int) (measureColumnChunkOffsets.get(blockIndex + 1) - currentMeasureOffset));
    }
    MeasureRawColumnChunk rawColumnChunk =
        new MeasureRawColumnChunk(blockIndex, data, 0, data.length, this);
    rawColumnChunk.setFileReader(fileReader);
    rawColumnChunk.setPagesCount(1);
    rawColumnChunk.setRowCount(new int[] { numberOfRows });
    return rawColumnChunk;
  }

  private MeasureRawColumnChunk[] readRawMeasureChunksInGroup(FileHolder fileReader,
      int startBlockIndex, int endBlockIndex) throws IOException {
    long currentMeasureOffset = measureColumnChunkOffsets.get(startBlockIndex);
    byte[] data = fileReader.readByteArray(filePath, currentMeasureOffset,
        (int) (measureColumnChunkOffsets.get(endBlockIndex + 1) - currentMeasureOffset));
    MeasureRawColumnChunk[] dataChunks =
        new MeasureRawColumnChunk[endBlockIndex - startBlockIndex + 1];
    int runningLength = 0;
    int index = 0;
    for (int i = startBlockIndex; i <= endBlockIndex; i++) {
      int currentLength =
          (int) (measureColumnChunkOffsets.get(i + 1) - measureColumnChunkOffsets.get(i));
      MeasureRawColumnChunk measureRawColumnChunk =
          new MeasureRawColumnChunk(i, data, runningLength, currentLength, this);
      measureRawColumnChunk.setFileReader(fileReader);
      measureRawColumnChunk.setRowCount(new int[] { numberOfRows });
      measureRawColumnChunk.setPagesCount(1);
      dataChunks[index] = measureRawColumnChunk;
      runningLength += currentLength;
      index++;
    }
    return dataChunks;
  }

  public MeasureColumnDataChunk convertToMeasureChunk(FileHolder fileReader, int blockIndex,
      byte[] rawData, int offset, int length, int pageNumber) throws IOException {
    MeasureColumnDataChunk datChunk = new MeasureColumnDataChunk();
    DataChunk2 measureColumnChunk = null;
    byte[] data = null;
    int copyPoint = offset;
    if (measureColumnChunkOffsets.size() - 1 == blockIndex) {
      measureColumnChunk =
          CarbonUtil.readDataChunk(rawData, copyPoint, measureColumnChunkLength.get(blockIndex));
      synchronized (fileReader) {
        data = fileReader.readByteArray(filePath,
            measureColumnChunkOffsets.get(blockIndex) + measureColumnChunkLength.get(blockIndex),
            measureColumnChunk.data_page_length);
      }
    } else {
      data = rawData;
      measureColumnChunk =
          CarbonUtil.readDataChunk(rawData, copyPoint, measureColumnChunkLength.get(blockIndex));
      copyPoint += measureColumnChunkLength.get(blockIndex);
    }
    List<ValueEncoderMeta> valueEncodeMeta = new ArrayList<>();
    for (int i = 0; i < measureColumnChunk.getEncoder_meta().size(); i++) {
      valueEncodeMeta.add(
          CarbonUtil.deserializeEncoderMeta(measureColumnChunk.getEncoder_meta().get(i).array()));
    }
    WriterCompressModel compressionModel = CarbonUtil.getValueCompressionModel(valueEncodeMeta);

    ValueCompressionHolder values = compressionModel.getValueCompressionHolder()[0];

    // uncompress
    values.uncompress(compressionModel.getConvertedDataType()[0], data, copyPoint,
        measureColumnChunk.data_page_length, compressionModel.getMantissa()[0],
        compressionModel.getMaxValue()[0], numberOfRows);

    CarbonReadDataHolder measureDataHolder = new CarbonReadDataHolder(values);

    // set the data chunk
    datChunk.setMeasureDataHolder(measureDataHolder);

    // set the enun value indexes
    datChunk.setNullValueIndexHolder(getPresenceMeta(measureColumnChunk.presence));
    return datChunk;
  }

  @Override
  public MeasureColumnDataChunk convertToMeasureChunk(
      MeasureRawColumnChunk measureRawColumnChunk, int pageNumber)
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
}
