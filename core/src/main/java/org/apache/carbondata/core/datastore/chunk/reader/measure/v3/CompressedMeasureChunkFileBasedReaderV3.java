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
package org.apache.carbondata.core.datastore.chunk.reader.measure.v3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.reader.measure.v2.CompressedMeasureChunkFileBasedReaderV2;
import org.apache.carbondata.core.datastore.compression.ValueCompressionHolder;
import org.apache.carbondata.core.datastore.compression.WriterCompressModel;
import org.apache.carbondata.core.datastore.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.DataChunk3;
import org.apache.commons.lang.ArrayUtils;

public class CompressedMeasureChunkFileBasedReaderV3
    extends CompressedMeasureChunkFileBasedReaderV2 {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CompressedMeasureChunkFileBasedReaderV3.class.getName());
  
  private long measureOffsets;

  public CompressedMeasureChunkFileBasedReaderV3(BlockletInfo blockletInfo, String filePath) {
    super(blockletInfo, filePath);
    measureOffsets = blockletInfo.getMeasureOffsets();
  }

  @Override public MeasureRawColumnChunk readRawMeasureChunk(FileHolder fileReader, int blockIndex)
      throws IOException {
    int dataLength = 0;
    if (measureColumnChunkOffsets.size() - 1 == blockIndex) {
      dataLength = (int) (measureOffsets - measureColumnChunkOffsets.get(blockIndex));
    } else {
      dataLength = (int) (measureColumnChunkOffsets.get(blockIndex + 1) - measureColumnChunkOffsets
          .get(blockIndex));
    }
    ByteBuffer buffer = ByteBuffer.allocateDirect(dataLength);
    synchronized (fileReader) {
      fileReader.readByteBuffer(filePath,buffer, measureColumnChunkOffsets.get(blockIndex), dataLength);
    }
    MeasureRawColumnChunk rawColumnChunk =
        new MeasureRawColumnChunk(blockIndex, buffer, 0, dataLength, this);
    DataChunk3 dataChunk =
        CarbonUtil.readDataChunk3(buffer, 0, measureColumnChunkLength.get(blockIndex));
    int numberOfPages = dataChunk.getPage_length().size();
    byte[][] maxValueOfEachPage = new byte[numberOfPages][];
    byte[][] minValueOfEachPage = new byte[numberOfPages][];
    int[] eachPageLength = new int[numberOfPages];
    for (int i = 0; i < minValueOfEachPage.length; i++) {
      maxValueOfEachPage[i] =
          dataChunk.getData_chunk_list().get(i).getMin_max().getMax_values().get(0).array();
      minValueOfEachPage[i] =
          dataChunk.getData_chunk_list().get(i).getMin_max().getMin_values().get(0).array();
      eachPageLength[i] = dataChunk.getData_chunk_list().get(i).getNumberOfRowsInpage();
    }
    rawColumnChunk.setDataChunk3(dataChunk);
    rawColumnChunk.setFileReader(fileReader);
    rawColumnChunk.setPagesCount(dataChunk.getPage_length().size());
    rawColumnChunk.setMaxValues(maxValueOfEachPage);
    rawColumnChunk.setMinValues(minValueOfEachPage);
    rawColumnChunk.setRowCount(eachPageLength);
    rawColumnChunk.setLengths(ArrayUtils
        .toPrimitive(dataChunk.page_length.toArray(new Integer[dataChunk.page_length.size()])));
    rawColumnChunk.setOffsets(ArrayUtils
        .toPrimitive(dataChunk.page_offset.toArray(new Integer[dataChunk.page_offset.size()])));
    return rawColumnChunk;
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

  private MeasureRawColumnChunk[] readRawMeasureChunksInGroup(FileHolder fileReader,
      int startBlockIndex, int endBlockIndex) throws IOException {
    long currentMeasureOffset = measureColumnChunkOffsets.get(startBlockIndex);
    ByteBuffer buffer = ByteBuffer.allocateDirect( (int) (measureColumnChunkOffsets.get(endBlockIndex + 1) - currentMeasureOffset));
    synchronized (fileReader) {
      fileReader.readByteBuffer(filePath,buffer, currentMeasureOffset,
          (int) (measureColumnChunkOffsets.get(endBlockIndex + 1) - currentMeasureOffset));
    }
    MeasureRawColumnChunk[] measureDataChunk =
        new MeasureRawColumnChunk[endBlockIndex - startBlockIndex + 1];
    try {
    int runningLength = 0;
    int index = 0;
    for (int i = startBlockIndex; i <= endBlockIndex; i++) {
      int currentLength =
          (int) (measureColumnChunkOffsets.get(i + 1) - measureColumnChunkOffsets.get(i));
      MeasureRawColumnChunk measureRawColumnChunk =
          new MeasureRawColumnChunk(i, buffer, runningLength, currentLength, this);
      DataChunk3 dataChunk =
          CarbonUtil.readDataChunk3(buffer, runningLength, measureColumnChunkLength.get(i));

      int numberOfPages = dataChunk.getPage_length().size();
      byte[][] maxValueOfEachPage = new byte[numberOfPages][];
      byte[][] minValueOfEachPage = new byte[numberOfPages][];
      int[] eachPageLength = new int[numberOfPages];
      for (int j = 0; j < minValueOfEachPage.length; j++) {
        maxValueOfEachPage[j] =
            dataChunk.getData_chunk_list().get(j).getMin_max().getMax_values().get(0).array();
        minValueOfEachPage[j] =
            dataChunk.getData_chunk_list().get(j).getMin_max().getMin_values().get(0).array();
        eachPageLength[j] = dataChunk.getData_chunk_list().get(j).getNumberOfRowsInpage();
      }
      measureRawColumnChunk.setDataChunk3(dataChunk);;
      measureRawColumnChunk.setFileReader(fileReader);
      measureRawColumnChunk.setPagesCount(dataChunk.getPage_length().size());
      measureRawColumnChunk.setMaxValues(maxValueOfEachPage);
      measureRawColumnChunk.setMinValues(minValueOfEachPage);
      measureRawColumnChunk.setRowCount(eachPageLength);
      measureRawColumnChunk.setLengths(ArrayUtils
          .toPrimitive(dataChunk.page_length.toArray(new Integer[dataChunk.page_length.size()])));
      measureRawColumnChunk.setOffsets(ArrayUtils
          .toPrimitive(dataChunk.page_offset.toArray(new Integer[dataChunk.page_offset.size()])));
      measureDataChunk[index] = measureRawColumnChunk;
      runningLength += currentLength;
      index++;
    }
    } catch(Throwable t) {
      LOGGER.error(t, "************************ File path: " + filePath);
      throw t;
      
    }
    return measureDataChunk;
  }

  public MeasureColumnDataChunk convertToMeasureChunk(MeasureRawColumnChunk measureRawColumnChunk,
      int pageNumber) throws IOException {
    MeasureColumnDataChunk datChunk = new MeasureColumnDataChunk();
    DataChunk3 dataChunk3 = measureRawColumnChunk.getDataChunk3();
    DataChunk2 measureColumnChunk = dataChunk3.getData_chunk_list().get(pageNumber);
    int copyPoint = measureRawColumnChunk.getOffSet() + measureColumnChunkLength
        .get(measureRawColumnChunk.getBlockId()) + dataChunk3.getPage_offset().get(pageNumber);
    List<ValueEncoderMeta> valueEncodeMeta = new ArrayList<>();
    for (int i = 0; i < measureColumnChunk.getEncoder_meta().size(); i++) {
      valueEncodeMeta.add(CarbonUtil
          .deserializeEncoderMetaNew(measureColumnChunk.getEncoder_meta().get(i).array()));
    }
    WriterCompressModel compressionModel = CarbonUtil.getValueCompressionModel(valueEncodeMeta);
    ValueCompressionHolder values = compressionModel.getValueCompressionHolder()[0];
    // uncompress
    long nanoTime = System.nanoTime();
    byte[] data = new byte[measureColumnChunk.data_page_length];
    ByteBuffer rawData = measureRawColumnChunk.getRawData();
    rawData.position(copyPoint);
    rawData.get(data);
    values
        .uncompress(compressionModel.getConvertedDataType()[0], data,
            0, measureColumnChunk.data_page_length, compressionModel.getMantissa()[0],
            compressionModel.getMaxValue()[0], measureRawColumnChunk.getRowCount()[pageNumber]);
    CarbonReadDataHolder measureDataHolder = new CarbonReadDataHolder(values);
    measureRawColumnChunk.getFileReader().getStatisticObject().setTimeTakenForValueCompression(System.nanoTime()-nanoTime);
    // set the data chunk
    datChunk.setMeasureDataHolder(measureDataHolder);
    // set the enun value indexes
    datChunk.setNullValueIndexHolder(getPresenceMeta(measureColumnChunk.presence));
    return datChunk;
  }
}
