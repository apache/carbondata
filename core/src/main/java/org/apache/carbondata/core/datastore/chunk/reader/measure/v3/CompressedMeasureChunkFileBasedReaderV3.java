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
import java.util.List;

import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.reader.measure.AbstractMeasureChunkReaderV2V3Format;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.DataChunk3;
import org.apache.carbondata.format.Encoding;

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
   * @param columnIndex         column to be read
   * @return measure raw chunk
   */
  @Override public MeasureRawColumnChunk readRawMeasureChunk(FileHolder fileReader,
      int columnIndex) throws IOException {
    int dataLength = 0;
    // to calculate the length of the data to be read
    // column other than last column we can subtract the offset of current column with
    // next column and get the total length.
    // but for last column we need to use lastDimensionOffset which is the end position
    // of the last dimension, we can subtract current dimension offset from lastDimesionOffset
    if (measureColumnChunkOffsets.size() - 1 == columnIndex) {
      dataLength = (int) (measureOffsets - measureColumnChunkOffsets.get(columnIndex));
    } else {
      dataLength =
          (int) (measureColumnChunkOffsets.get(columnIndex + 1) - measureColumnChunkOffsets
              .get(columnIndex));
    }
    ByteBuffer buffer = null;
    // read the data from carbon data file
    synchronized (fileReader) {
      buffer = fileReader
          .readByteBuffer(filePath, measureColumnChunkOffsets.get(columnIndex), dataLength);
    }
    // get the data chunk which will have all the details about the data pages
    DataChunk3 dataChunk =
        CarbonUtil.readDataChunk3(buffer, 0, measureColumnChunkLength.get(columnIndex));
    // creating a raw chunks instance and filling all the details
    MeasureRawColumnChunk rawColumnChunk =
        new MeasureRawColumnChunk(columnIndex, buffer, 0, dataLength, this);
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
   * @param rawColumnPage measure raw chunk
   * @param pageNumber            number
   * @return DimensionColumnDataChunk
   */
  @Override
  public ColumnPage convertToColumnPage(
      MeasureRawColumnChunk rawColumnPage, int pageNumber)
      throws IOException, MemoryException {
    // data chunk of blocklet column
    DataChunk3 dataChunk3 = rawColumnPage.getDataChunkV3();
    // data chunk of page
    DataChunk2 pageMetadata = dataChunk3.getData_chunk_list().get(pageNumber);
    // calculating the start point of data
    // as buffer can contain multiple column data, start point will be datachunkoffset +
    // data chunk length + page offset
    int offset = rawColumnPage.getOffSet() +
        measureColumnChunkLength.get(rawColumnPage.getColumnIndex()) +
        dataChunk3.getPage_offset().get(pageNumber);
    ColumnPage decodedPage = decodeMeasure(pageMetadata, rawColumnPage.getRawData(), offset);
    decodedPage.setNullBits(getNullBitSet(pageMetadata.presence));
    return decodedPage;
  }

  /**
   * Decode measure column page with page header and raw data starting from offset
   */
  private ColumnPage decodeMeasure(DataChunk2 pageMetadata, ByteBuffer pageData, int offset)
      throws MemoryException, IOException {
    List<Encoding> encodings = pageMetadata.getEncoders();
    List<ByteBuffer> encoderMetas = pageMetadata.getEncoder_meta();
    ColumnPageDecoder codec = strategy.createDecoder(encodings, encoderMetas);
    return codec.decode(pageData.array(), offset, pageMetadata.data_page_length);
  }

}
