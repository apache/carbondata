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
import org.apache.carbondata.format.DataChunk3;

import org.apache.commons.lang.ArrayUtils;

/**
 * Measure column V3 Reader class which will be used to read and uncompress
 * V3 format data
 * data format
 * Data Format
 * <FileHeader>
 * <Column1 Data ChunkV3><Column1<Page1><Page2><Page3><Page4>>
 * <Column2 Data ChunkV3><Column2<Page1><Page2><Page3><Page4>>
 * <Column3 Data ChunkV3><Column3<Page1><Page2><Page3><Page4>>
 * <Column4 Data ChunkV3><Column4<Page1><Page2><Page3><Page4>>
 * <File Footer>
 */
public class CompressedMeasureChunkFileBasedReaderV3 extends AbstractMeasureChunkReaderV2V3Format {

  /**
   * end position of last measure in carbon data file
   */
  private long measureOffsets;

  public CompressedMeasureChunkFileBasedReaderV3(BlockletInfo blockletInfo, String filePath) {
    super(blockletInfo, filePath);
    measureOffsets = blockletInfo.getMeasureOffsets();
  }

  /**
   * Below method will be used to read the measure column data form carbon data file
   * 1. Get the length of the data to be read
   * 2. Allocate the direct buffer
   * 3. read the data from file
   * 4. Get the data chunk object from data read
   * 5. Create the raw chunk object and fill the details
   *
   * @param fileReader          reader for reading the column from carbon data file
   * @param blockletColumnIndex          blocklet index of the column in carbon data file
   * @return measure raw chunk
   */
  @Override public MeasureRawColumnChunk readRawMeasureChunk(FileHolder fileReader,
      int blockletColumnIndex) throws IOException {
    int dataLength = 0;
    // to calculate the length of the data to be read
    // column other than last column we can subtract the offset of current column with
    // next column and get the total length.
    // but for last column we need to use lastDimensionOffset which is the end position
    // of the last dimension, we can subtract current dimension offset from lastDimesionOffset
    if (measureColumnChunkOffsets.size() - 1 == blockletColumnIndex) {
      dataLength = (int) (measureOffsets - measureColumnChunkOffsets.get(blockletColumnIndex));
    } else {
      dataLength =
          (int) (measureColumnChunkOffsets.get(blockletColumnIndex + 1) - measureColumnChunkOffsets
              .get(blockletColumnIndex));
    }
    ByteBuffer buffer = null;
    // read the data from carbon data file
    synchronized (fileReader) {
      buffer = fileReader
          .readByteBuffer(filePath, measureColumnChunkOffsets.get(blockletColumnIndex), dataLength);
    }
    // get the data chunk which will have all the details about the data pages
    DataChunk3 dataChunk =
        CarbonUtil.readDataChunk3(buffer, 0, measureColumnChunkLength.get(blockletColumnIndex));
    // creating a raw chunks instance and filling all the details
    MeasureRawColumnChunk rawColumnChunk =
        new MeasureRawColumnChunk(blockletColumnIndex, buffer, 0, dataLength, this);
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
    rawColumnChunk.setDataChunkV3(dataChunk);
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
   * Below method will be used to read the multiple measure column data in group
   * and divide into measure raw chunk object
   * Steps for reading
   * 1. Get the length of the data to be read
   * 2. Allocate the direct buffer
   * 3. read the data from file
   * 4. Get the data chunk object from file for each column
   * 5. Create the raw chunk object and fill the details for each column
   * 6. increment the offset of the data
   *
   * @param fileReader
   *        reader which will be used to read the measure columns data from file
   * @param startColumnBlockletIndex
   *        blocklet index of the first measure column
   * @param endColumnBlockletIndex
   *        blocklet index of the last measure column
   * @return MeasureRawColumnChunk array
   */
  protected MeasureRawColumnChunk[] readRawMeasureChunksInGroup(FileHolder fileReader,
      int startColumnBlockletIndex, int endColumnBlockletIndex) throws IOException {
    // to calculate the length of the data to be read
    // column we can subtract the offset of start column offset with
    // end column+1 offset and get the total length.
    long currentMeasureOffset = measureColumnChunkOffsets.get(startColumnBlockletIndex);
    ByteBuffer buffer = null;
    // read the data from carbon data file
    synchronized (fileReader) {
      buffer = fileReader.readByteBuffer(filePath, currentMeasureOffset,
          (int) (measureColumnChunkOffsets.get(endColumnBlockletIndex + 1) - currentMeasureOffset));
    }
    // create raw chunk for each measure column
    MeasureRawColumnChunk[] measureDataChunk =
        new MeasureRawColumnChunk[endColumnBlockletIndex - startColumnBlockletIndex + 1];
    int runningLength = 0;
    int index = 0;
    for (int i = startColumnBlockletIndex; i <= endColumnBlockletIndex; i++) {
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
      measureRawColumnChunk.setDataChunkV3(dataChunk);
      ;
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
    return measureDataChunk;
  }

  /**
   * Below method will be used to convert the compressed measure chunk raw data to actual data
   *
   * @param measureRawColumnChunk measure raw chunk
   * @param pageNumber            number
   * @return DimensionColumnDataChunk
   */
  @Override public MeasureColumnDataChunk convertToMeasureChunk(
      MeasureRawColumnChunk measureRawColumnChunk, int pageNumber) throws IOException {
    MeasureColumnDataChunk datChunk = new MeasureColumnDataChunk();
    // data chunk of blocklet column
    DataChunk3 dataChunk3 = measureRawColumnChunk.getDataChunkV3();
    // data chunk of page
    DataChunk2 measureColumnChunk = dataChunk3.getData_chunk_list().get(pageNumber);
    // calculating the start point of data
    // as buffer can contain multiple column data, start point will be datachunkoffset +
    // data chunk length + page offset
    int copyPoint = measureRawColumnChunk.getOffSet() + measureColumnChunkLength
        .get(measureRawColumnChunk.getBlockletId()) + dataChunk3.getPage_offset().get(pageNumber);
    List<ValueEncoderMeta> valueEncodeMeta = new ArrayList<>();
    for (int i = 0; i < measureColumnChunk.getEncoder_meta().size(); i++) {
      valueEncodeMeta.add(
          CarbonUtil.deserializeEncoderMetaV3(measureColumnChunk.getEncoder_meta().get(i).array()));
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
    ByteBuffer rawData = measureRawColumnChunk.getRawData();
    values.uncompress(convertedType[0], rawData.array(), copyPoint,
        measureColumnChunk.data_page_length, stats.getDecimal(0),
        stats.getMax(0), measureRawColumnChunk.getRowCount()[pageNumber]);
    CarbonReadDataHolder measureDataHolder = new CarbonReadDataHolder(values);
    // set the data chunk
    datChunk.setMeasureDataHolder(measureDataHolder);
    // set the null value indexes
    datChunk.setNullValueIndexHolder(getPresenceMeta(measureColumnChunk.presence));
    return datChunk;
  }
}
