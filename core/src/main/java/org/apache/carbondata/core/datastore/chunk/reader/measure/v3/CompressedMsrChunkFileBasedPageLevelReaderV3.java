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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.DataChunk3;

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
public class CompressedMsrChunkFileBasedPageLevelReaderV3
    extends CompressedMeasureChunkFileBasedReaderV3 {

  /**
   * end position of last measure in carbon data file
   */
  private long measureOffsets;

  public CompressedMsrChunkFileBasedPageLevelReaderV3(BlockletInfo blockletInfo, String filePath) {
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
   * @param blockletColumnIndex blocklet index of the column in carbon data file
   * @return measure raw chunk
   */
  @Override public MeasureRawColumnChunk readRawMeasureChunk(FileReader fileReader,
      int blockletColumnIndex) throws IOException {
    int dataLength = 0;
    // to calculate the length of the data to be read
    // column other than last column we can subtract the offset of current column with
    // next column and get the total length.
    // but for last column we need to use lastDimensionOffset which is the end position
    // of the last dimension, we can subtract current dimension offset from lastDimensionOffset
    if (measureColumnChunkOffsets.size() - 1 == blockletColumnIndex) {
      dataLength = (int) (measureOffsets - measureColumnChunkOffsets.get(blockletColumnIndex));
    } else {
      dataLength =
          (int) (measureColumnChunkOffsets.get(blockletColumnIndex + 1) - measureColumnChunkOffsets
              .get(blockletColumnIndex));
    }
    ByteBuffer buffer;
    // read the data from carbon data file
    synchronized (fileReader) {
      buffer = fileReader
          .readByteBuffer(filePath, measureColumnChunkOffsets.get(blockletColumnIndex),
              measureColumnChunkLength.get(blockletColumnIndex));
    }
    // get the data chunk which will have all the details about the data pages
    DataChunk3 dataChunk = CarbonUtil.readDataChunk3(new ByteArrayInputStream(buffer.array()));
    return getMeasureRawColumnChunk(fileReader, blockletColumnIndex,
        measureColumnChunkOffsets.get(blockletColumnIndex), dataLength, null, dataChunk);
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
   * @param fileReader         reader which will be used to read the measure columns data from file
   * @param startColumnBlockletIndex blocklet index of the first measure column
   * @param endColumnBlockletIndex   blocklet index of the last measure column
   * @return MeasureRawColumnChunk array
   */
  protected MeasureRawColumnChunk[] readRawMeasureChunksInGroup(FileReader fileReader,
      int startColumnBlockletIndex, int endColumnBlockletIndex) throws IOException {
    // create raw chunk for each measure column
    MeasureRawColumnChunk[] measureDataChunk =
        new MeasureRawColumnChunk[endColumnBlockletIndex - startColumnBlockletIndex + 1];
    int index = 0;
    for (int i = startColumnBlockletIndex; i <= endColumnBlockletIndex; i++) {
      measureDataChunk[index] = readRawMeasureChunk(fileReader, i);
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
  @Override public ColumnPage decodeColumnPage(
      MeasureRawColumnChunk rawColumnPage, int pageNumber)
      throws IOException, MemoryException {
    // data chunk of blocklet column
    DataChunk3 dataChunk3 = rawColumnPage.getDataChunkV3();
    // data chunk of page
    DataChunk2 pageMetadata = dataChunk3.getData_chunk_list().get(pageNumber);
    String compressorName = CarbonMetadataUtil.getCompressorNameFromChunkMeta(
        pageMetadata.getChunk_meta());
    this.compressor = CompressorFactory.getInstance().getCompressor(compressorName);
    // calculating the start point of data
    // as buffer can contain multiple column data, start point will be datachunkoffset +
    // data chunk length + page offset
    long offset = rawColumnPage.getOffSet() + measureColumnChunkLength
        .get(rawColumnPage.getColumnIndex()) + dataChunk3.getPage_offset().get(pageNumber);
    ByteBuffer buffer = rawColumnPage.getFileReader()
        .readByteBuffer(filePath, offset, pageMetadata.data_page_length);

    BitSet nullBitSet = QueryUtil.getNullBitSet(pageMetadata.presence, this.compressor);
    ColumnPage decodedPage = decodeMeasure(pageMetadata, buffer, 0, null, nullBitSet);
    decodedPage.setNullBits(nullBitSet);
    return decodedPage;
  }

}