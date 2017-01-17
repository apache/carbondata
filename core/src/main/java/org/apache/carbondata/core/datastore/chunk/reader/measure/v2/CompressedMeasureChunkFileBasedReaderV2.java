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
  private List<Long> measureColumnChunkOffsets;

  /**
   * measure column chunks length
   */
  private List<Short> measureColumnChunkLength;

  /**
   * Constructor to get minimum parameter to create instance of this class
   *
   * @param blockletInfo BlockletInfo
   * @param filePath     file from which data will be read
   */
  public CompressedMeasureChunkFileBasedReaderV2(final BlockletInfo blockletInfo,
      final String filePath) {
    super(filePath);
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
  private static PresenceMeta getPresenceMeta(
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
  public MeasureColumnDataChunk[] readMeasureChunks(FileHolder fileReader, int[][] blockIndexes)
      throws IOException {
    // read the column chunk based on block index and add
    MeasureColumnDataChunk[] dataChunks =
        new MeasureColumnDataChunk[measureColumnChunkOffsets.size()];
    if (blockIndexes.length == 0) {
      return dataChunks;
    }
    MeasureColumnDataChunk[] groupChunk = null;
    int index = 0;
    for (int i = 0; i < blockIndexes.length - 1; i++) {
      index = 0;
      groupChunk = readMeasureChunksInGroup(fileReader, blockIndexes[i][0], blockIndexes[i][1]);
      for (int j = blockIndexes[i][0]; j <= blockIndexes[i][1]; j++) {
        dataChunks[j] = groupChunk[index++];
      }
    }
    if (blockIndexes[blockIndexes.length - 1][0] == measureColumnChunkOffsets.size() - 1) {
      dataChunks[blockIndexes[blockIndexes.length - 1][0]] =
          readMeasureChunk(fileReader, blockIndexes[blockIndexes.length - 1][0]);
    } else {
      groupChunk = readMeasureChunksInGroup(fileReader, blockIndexes[blockIndexes.length - 1][0],
          blockIndexes[blockIndexes.length - 1][1]);
      index = 0;
      for (int j = blockIndexes[blockIndexes.length - 1][0];
           j <= blockIndexes[blockIndexes.length - 1][1]; j++) {
        dataChunks[j] = groupChunk[index++];
      }
    }
    return dataChunks;
  }

  /**
   * Method to read the blocks data based on block index
   *
   * @param fileReader file reader to read the blocks
   * @param blockIndex block to be read
   * @return measure data chunk
   * @throws IOException
   */
  @Override public MeasureColumnDataChunk readMeasureChunk(FileHolder fileReader, int blockIndex)
      throws IOException {
    MeasureColumnDataChunk datChunk = new MeasureColumnDataChunk();
    DataChunk2 measureColumnChunk = null;
    byte[] measureDataChunk = null;
    byte[] data = null;
    int copyPoint = 0;
    if (measureColumnChunkOffsets.size() - 1 == blockIndex) {
      measureDataChunk = fileReader
          .readByteArray(filePath, measureColumnChunkOffsets.get(blockIndex),
              measureColumnChunkLength.get(blockIndex));
      measureColumnChunk = CarbonUtil
          .readDataChunk(measureDataChunk, copyPoint, measureColumnChunkLength.get(blockIndex));
      data = fileReader.readByteArray(filePath,
          measureColumnChunkOffsets.get(blockIndex) + measureColumnChunkLength.get(blockIndex),
          measureColumnChunk.data_page_length);
    } else {
      long currentMeasureOffset = measureColumnChunkOffsets.get(blockIndex);
      data = fileReader.readByteArray(filePath, currentMeasureOffset,
          (int) (measureColumnChunkOffsets.get(blockIndex + 1) - currentMeasureOffset));
      measureColumnChunk =
          CarbonUtil.readDataChunk(data, copyPoint, measureColumnChunkLength.get(blockIndex));
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
    values.uncompress(compressionModel.getConvertedDataType()[0], data,
        copyPoint, measureColumnChunk.data_page_length, compressionModel.getMantissa()[0],
            compressionModel.getMaxValue()[0]);

    CarbonReadDataHolder measureDataHolder = new CarbonReadDataHolder(values);

    // set the data chunk
    datChunk.setMeasureDataHolder(measureDataHolder);

    // set the enun value indexes
    datChunk.setNullValueIndexHolder(getPresenceMeta(measureColumnChunk.presence));
    return datChunk;
  }

  /**
   * Below method will be used to read the dimension chunks in group. This is
   * to enhance the IO performance. Will read the data from start index to end
   * index(including)
   *
   * @param fileReader      stream used for reading
   * @param startBlockIndex start block index
   * @param endBlockIndex   end block index
   * @return measure column chunk array
   * @throws IOException
   */
  private MeasureColumnDataChunk[] readMeasureChunksInGroup(FileHolder fileReader,
      int startBlockIndex, int endBlockIndex) throws IOException {
    long currentMeasureOffset = measureColumnChunkOffsets.get(startBlockIndex);
    byte[] data = fileReader.readByteArray(filePath, currentMeasureOffset,
        (int) (measureColumnChunkOffsets.get(endBlockIndex + 1) - currentMeasureOffset));
    MeasureColumnDataChunk[] dataChunks =
        new MeasureColumnDataChunk[endBlockIndex - startBlockIndex + 1];
    MeasureColumnDataChunk dataChunk = null;
    int index = 0;
    int copyPoint = 0;
    DataChunk2 measureColumnChunk = null;
    for (int i = startBlockIndex; i <= endBlockIndex; i++) {
      dataChunk = new MeasureColumnDataChunk();
      measureColumnChunk =
          CarbonUtil.readDataChunk(data, copyPoint, measureColumnChunkLength.get(i));
      copyPoint += measureColumnChunkLength.get(i);
      List<ValueEncoderMeta> valueEncodeMeta = new ArrayList<>();
      for (int j = 0; j < measureColumnChunk.getEncoder_meta().size(); j++) {
        valueEncodeMeta.add(
            CarbonUtil.deserializeEncoderMeta(measureColumnChunk.getEncoder_meta().get(j).array()));
      }
      WriterCompressModel compressionModel = CarbonUtil.getValueCompressionModel(valueEncodeMeta);

      ValueCompressionHolder values = compressionModel.getValueCompressionHolder()[0];

      // uncompress
      values.uncompress(compressionModel.getConvertedDataType()[0], data, copyPoint,
              measureColumnChunk.data_page_length, compressionModel.getMantissa()[0],
              compressionModel.getMaxValue()[0]);

      CarbonReadDataHolder measureDataHolder = new CarbonReadDataHolder(values);

      copyPoint += measureColumnChunk.data_page_length;
      // set the data chunk
      dataChunk.setMeasureDataHolder(measureDataHolder);

      // set the enun value indexes
      dataChunk.setNullValueIndexHolder(getPresenceMeta(measureColumnChunk.presence));
      dataChunks[index++] = dataChunk;
    }
    return dataChunks;
  }
}
