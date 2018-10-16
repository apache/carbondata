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
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.reader.measure.AbstractMeasureChunkReaderV2V3Format;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
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
  @Override public MeasureRawColumnChunk readRawMeasureChunk(FileReader fileReader,
      int columnIndex) throws IOException {
    int dataLength = 0;
    // to calculate the length of the data to be read
    // column other than last column we can subtract the offset of current column with
    // next column and get the total length.
    // but for last column we need to use lastDimensionOffset which is the end position
    // of the last dimension, we can subtract current dimension offset from lastDimensionOffset
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

    return getMeasureRawColumnChunk(fileReader, columnIndex, 0,  dataLength, buffer,
        dataChunk);
  }

  MeasureRawColumnChunk getMeasureRawColumnChunk(FileReader fileReader, int columnIndex,
      long offset, int dataLength, ByteBuffer buffer, DataChunk3 dataChunk) {
    // creating a raw chunks instance and filling all the details
    MeasureRawColumnChunk rawColumnChunk =
        new MeasureRawColumnChunk(columnIndex, buffer, offset, dataLength, this);
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
   * @param startColumnIndex
   *        column index of the first measure column
   * @param endColumnIndex
   *        column index of the last measure column
   * @return MeasureRawColumnChunk array
   */
  protected MeasureRawColumnChunk[] readRawMeasureChunksInGroup(FileReader fileReader,
      int startColumnIndex, int endColumnIndex) throws IOException {
    // to calculate the length of the data to be read
    // column we can subtract the offset of start column offset with
    // end column+1 offset and get the total length.
    long currentMeasureOffset = measureColumnChunkOffsets.get(startColumnIndex);
    ByteBuffer buffer = null;
    // read the data from carbon data file
    synchronized (fileReader) {
      buffer = fileReader.readByteBuffer(filePath, currentMeasureOffset,
          (int) (measureColumnChunkOffsets.get(endColumnIndex + 1) - currentMeasureOffset));
    }
    // create raw chunk for each measure column
    MeasureRawColumnChunk[] measureDataChunk =
        new MeasureRawColumnChunk[endColumnIndex - startColumnIndex + 1];
    int runningLength = 0;
    int index = 0;
    for (int i = startColumnIndex; i <= endColumnIndex; i++) {
      int currentLength =
          (int) (measureColumnChunkOffsets.get(i + 1) - measureColumnChunkOffsets.get(i));
      DataChunk3 dataChunk =
          CarbonUtil.readDataChunk3(buffer, runningLength, measureColumnChunkLength.get(i));
      MeasureRawColumnChunk measureRawColumnChunk =
          getMeasureRawColumnChunk(fileReader, i, runningLength, currentLength, buffer, dataChunk);
      measureDataChunk[index] = measureRawColumnChunk;
      runningLength += currentLength;
      index++;
    }
    return measureDataChunk;
  }

  /**
   * Below method will be used to convert the compressed measure chunk raw data to actual data
   *
   * @param rawColumnChunk measure raw chunk
   * @param pageNumber            number
   * @return DimensionColumnPage
   */
  @Override
  public ColumnPage decodeColumnPage(
      MeasureRawColumnChunk rawColumnChunk, int pageNumber)
      throws IOException, MemoryException {
    return decodeColumnPage(rawColumnChunk, pageNumber, null);
  }

  @Override
  public void decodeColumnPageAndFillVector(MeasureRawColumnChunk measureRawColumnChunk,
      int pageNumber, ColumnVectorInfo vectorInfo) throws IOException, MemoryException {
    decodeColumnPage(measureRawColumnChunk, pageNumber, vectorInfo);
  }

  private ColumnPage decodeColumnPage(
      MeasureRawColumnChunk rawColumnChunk, int pageNumber, ColumnVectorInfo vectorInfo)
      throws IOException, MemoryException {
    // data chunk of blocklet column
    DataChunk3 dataChunk3 = rawColumnChunk.getDataChunkV3();
    // data chunk of page
    DataChunk2 pageMetadata = dataChunk3.getData_chunk_list().get(pageNumber);
    String compressorName = CarbonMetadataUtil.getCompressorNameFromChunkMeta(
        pageMetadata.getChunk_meta());
    this.compressor = CompressorFactory.getInstance().getCompressor(compressorName);
    // calculating the start point of data
    // as buffer can contain multiple column data, start point will be datachunkoffset +
    // data chunk length + page offset
    int offset = (int) rawColumnChunk.getOffSet() +
        measureColumnChunkLength.get(rawColumnChunk.getColumnIndex()) +
        dataChunk3.getPage_offset().get(pageNumber);
    BitSet nullBitSet = QueryUtil.getNullBitSet(pageMetadata.presence, this.compressor);
    ColumnPage decodedPage =
        decodeMeasure(pageMetadata, rawColumnChunk.getRawData(), offset, vectorInfo, nullBitSet);
    if (decodedPage == null) {
      return null;
    }
    decodedPage.setNullBits(nullBitSet);
    return decodedPage;
  }

  /**
   * Decode measure column page with page header and raw data starting from offset
   */
  protected ColumnPage decodeMeasure(DataChunk2 pageMetadata, ByteBuffer pageData, int offset,
      ColumnVectorInfo vectorInfo, BitSet nullBitSet) throws MemoryException, IOException {
    List<Encoding> encodings = pageMetadata.getEncoders();
    List<ByteBuffer> encoderMetas = pageMetadata.getEncoder_meta();
    String compressorName =
        CarbonMetadataUtil.getCompressorNameFromChunkMeta(pageMetadata.getChunk_meta());
    ColumnPageDecoder codec =
        encodingFactory.createDecoder(encodings, encoderMetas, compressorName, vectorInfo != null);
    if (vectorInfo != null) {
      codec
          .decodeAndFillVector(pageData.array(), offset, pageMetadata.data_page_length, vectorInfo,
              nullBitSet, false);
      return null;
    } else {
      return codec.decode(pageData.array(), offset, pageMetadata.data_page_length);
    }
  }
}
