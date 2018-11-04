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
package org.apache.carbondata.core.datastore.chunk.reader.dimension.v3;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.DataChunk3;
import org.apache.carbondata.format.Encoding;

/**
 * Dimension column V3 Reader class which will be used to read and uncompress
 * V3 format data. It reads the data in each page at once unlike whole blocklet. It is
 * used for memory constraint operations like compaction.
 * data format
 * Data Format
 * <FileHeader>
 * <Column1 Data ChunkV3><Column1<Page1><Page2><Page3><Page4>>
 * <Column2 Data ChunkV3><Column2<Page1><Page2><Page3><Page4>>
 * <Column3 Data ChunkV3><Column3<Page1><Page2><Page3><Page4>>
 * <Column4 Data ChunkV3><Column4<Page1><Page2><Page3><Page4>>
 * <File Footer>
 */
public class CompressedDimChunkFileBasedPageLevelReaderV3
    extends CompressedDimensionChunkFileBasedReaderV3 {

  /**
   * end position of last dimension in carbon data file
   */
  private long lastDimensionOffsets;

  public CompressedDimChunkFileBasedPageLevelReaderV3(BlockletInfo blockletInfo,
      int[] eachColumnValueSize, String filePath) {
    super(blockletInfo, eachColumnValueSize, filePath);
    lastDimensionOffsets = blockletInfo.getDimensionOffset();
  }

  /**
   * Below method will be used to read the dimension column data form carbon data file
   * Steps for reading
   * 1. Get the length of the data to be read
   * 2. Allocate the direct buffer
   * 3. read the data from file
   * 4. Get the data chunk object from data read
   * 5. Create the raw chunk object and fill the details
   *
   * @param fileReader          reader for reading the column from carbon data file
   * @param blockletColumnIndex blocklet index of the column in carbon data file
   * @return dimension raw chunk
   */
  @Override
  public DimensionRawColumnChunk readRawDimensionChunk(FileReader fileReader,
      int blockletColumnIndex) throws IOException {
    // get the current dimension offset
    long currentDimensionOffset = dimensionChunksOffset.get(blockletColumnIndex);
    int length = 0;
    // to calculate the length of the data to be read
    // column other than last column we can subtract the offset of current column with
    // next column and get the total length.
    // but for last column we need to use lastDimensionOffset which is the end position
    // of the last dimension, we can subtract current dimension offset from lastDimensionOffset
    if (dimensionChunksOffset.size() - 1 == blockletColumnIndex) {
      length = (int) (lastDimensionOffsets - currentDimensionOffset);
    } else {
      length = (int) (dimensionChunksOffset.get(blockletColumnIndex + 1) - currentDimensionOffset);
    }
    ByteBuffer buffer;
    // read the data from carbon data file
    synchronized (fileReader) {
      buffer = fileReader.readByteBuffer(filePath, currentDimensionOffset,
          dimensionChunksLength.get(blockletColumnIndex));
    }
    // get the data chunk which will have all the details about the data pages
    DataChunk3 dataChunk = CarbonUtil.readDataChunk3(new ByteArrayInputStream(buffer.array()));
    DimensionRawColumnChunk rawColumnChunk =
        getDimensionRawColumnChunk(fileReader, blockletColumnIndex, currentDimensionOffset, length,
            null, dataChunk);

    return rawColumnChunk;
  }

  /**
   * Below method will be used to read the multiple dimension column data in group
   * and divide into dimension raw chunk object
   * Steps for reading
   * 1. Get the length of the data to be read
   * 2. Allocate the direct buffer
   * 3. read the data from file
   * 4. Get the data chunk object from file for each column
   * 5. Create the raw chunk object and fill the details for each column
   * 6. increment the offset of the data
   *
   * @param fileReader      reader which will be used to read the dimension columns data from file
   * @param startBlockletColumnIndex blocklet index of the first dimension column
   * @param endBlockletColumnIndex   blocklet index of the last dimension column
   * @ DimensionRawColumnChunk array
   */
  protected DimensionRawColumnChunk[] readRawDimensionChunksInGroup(FileReader fileReader,
      int startBlockletColumnIndex, int endBlockletColumnIndex) throws IOException {
    // create raw chunk for each dimension column
    DimensionRawColumnChunk[] dimensionDataChunks =
        new DimensionRawColumnChunk[endBlockletColumnIndex - startBlockletColumnIndex + 1];
    int index = 0;
    for (int i = startBlockletColumnIndex; i <= endBlockletColumnIndex; i++) {
      dimensionDataChunks[index] = readRawDimensionChunk(fileReader, i);
      index++;
    }
    return dimensionDataChunks;
  }

  /**
   * Below method will be used to convert the compressed dimension chunk raw data to actual data
   *
   * @param dimensionRawColumnChunk dimension raw chunk
   * @param pageNumber              number
   * @return DimensionColumnDataChunk
   */
  @Override public DimensionColumnPage decodeColumnPage(
      DimensionRawColumnChunk dimensionRawColumnChunk, int pageNumber)
      throws IOException, MemoryException {
    // data chunk of page
    DataChunk2 pageMetadata = null;
    // data chunk of blocklet column
    DataChunk3 dataChunk3 = dimensionRawColumnChunk.getDataChunkV3();

    pageMetadata = dataChunk3.getData_chunk_list().get(pageNumber);

    if (compressor == null) {
      this.compressor = CompressorFactory.getInstance().getCompressor(
          CarbonMetadataUtil.getCompressorNameFromChunkMeta(pageMetadata.getChunk_meta()));
    }
    // calculating the start point of data
    // as buffer can contain multiple column data, start point will be datachunkoffset +
    // data chunk length + page offset
    long offset = dimensionRawColumnChunk.getOffSet() + dimensionChunksLength
        .get(dimensionRawColumnChunk.getColumnIndex()) + dataChunk3.getPage_offset()
        .get(pageNumber);
    int length = pageMetadata.data_page_length;
    if (CarbonUtil.hasEncoding(pageMetadata.encoders, Encoding.INVERTED_INDEX)) {
      length += pageMetadata.rowid_page_length;
    }

    if (CarbonUtil.hasEncoding(pageMetadata.encoders, Encoding.RLE)) {
      length += pageMetadata.rle_page_length;
    }
    // get the data buffer
    ByteBuffer rawData = dimensionRawColumnChunk.getFileReader()
        .readByteBuffer(filePath, offset, length);

    return decodeDimension(dimensionRawColumnChunk, rawData, pageMetadata, 0, null);
  }
}