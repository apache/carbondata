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
package org.apache.carbondata.core.datastore.chunk.reader.dimension.v2;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.ColumnGroupDimensionDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.VariableLengthDimensionDataChunk;
import org.apache.carbondata.core.datastore.chunk.reader.dimension.AbstractChunkReader;
import org.apache.carbondata.core.datastore.columnar.UnBlockIndexer;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.Encoding;

/**
 * Compressed dimension chunk reader class for version 2
 */
public class CompressedDimensionChunkFileBasedReaderV2 extends AbstractChunkReader {

  /**
   * dimension chunks offset
   */
  private List<Long> dimensionChunksOffset;

  /**
   * dimension chunks length
   */
  private List<Short> dimensionChunksLength;

  /**
   * Constructor to get minimum parameter to create instance of this class
   *
   * @param blockletInfo
   * @param eachColumnValueSize
   * @param filePath
   */
  public CompressedDimensionChunkFileBasedReaderV2(final BlockletInfo blockletInfo,
      final int[] eachColumnValueSize, final String filePath) {
    super(eachColumnValueSize, filePath, blockletInfo.getNumberOfRows());
    this.dimensionChunksOffset = blockletInfo.getDimensionChunkOffsets();
    this.dimensionChunksLength = blockletInfo.getDimensionChunksLength();

  }

  /**
   * Below method will be used to read the chunk based on block indexes
   * Reading logic of below method is:
   * Except last column all the column chunk can be read in group
   * if not last column then read data of all the column present in block index
   * together then process it.
   * For last column read is separately and process
   *
   * @param fileReader   file reader to read the blocks from file
   * @param blockIndexes blocks range to be read
   * @return dimension column chunks
   */
  @Override public DimensionRawColumnChunk[] readRawDimensionChunks(final FileHolder fileReader,
      final int[][] blockIndexes) throws IOException {
    // read the column chunk based on block index and add
    DimensionRawColumnChunk[] dataChunks =
        new DimensionRawColumnChunk[dimensionChunksOffset.size()];
    // if blocklet index is empty then return empry data chunk
    if (blockIndexes.length == 0) {
      return dataChunks;
    }
    DimensionRawColumnChunk[] groupChunk = null;
    int index = 0;
    // iterate till block indexes -1 as block index will be in sorted order, so to avoid
    // the last column reading in group
    for (int i = 0; i < blockIndexes.length - 1; i++) {
      index = 0;
      groupChunk =
          readRawDimensionChunksInGroup(fileReader, blockIndexes[i][0], blockIndexes[i][1]);
      for (int j = blockIndexes[i][0]; j <= blockIndexes[i][1]; j++) {
        dataChunks[j] = groupChunk[index++];
      }
    }
    // check last index is present in block index, if it is present then read separately
    if (blockIndexes[blockIndexes.length - 1][0] == dimensionChunksOffset.size() - 1) {
      dataChunks[blockIndexes[blockIndexes.length - 1][0]] =
          readRawDimensionChunk(fileReader, blockIndexes[blockIndexes.length - 1][0]);
    }
    // otherwise read the data in group
    else {
      groupChunk =
          readRawDimensionChunksInGroup(fileReader, blockIndexes[blockIndexes.length - 1][0],
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
   * Below method will be used to read the chunk based on block index
   *
   * @param fileReader file reader to read the blocks from file
   * @param blockIndex block to be read
   * @return dimension column chunk
   */
  public DimensionRawColumnChunk readRawDimensionChunk(FileHolder fileReader,
      int blockIndex) throws IOException {
    byte[] data;
    if (dimensionChunksOffset.size() - 1 == blockIndex) {
      // Incase of last block read only for datachunk and read remaining while converting it.
      data = fileReader.readByteArray(filePath, dimensionChunksOffset.get(blockIndex),
          dimensionChunksLength.get(blockIndex));
    } else {
      long currentDimensionOffset = dimensionChunksOffset.get(blockIndex);
      data = fileReader.readByteArray(filePath, currentDimensionOffset,
          (int) (dimensionChunksOffset.get(blockIndex + 1) - currentDimensionOffset));
    }
    DimensionRawColumnChunk rawColumnChunk =
        new DimensionRawColumnChunk(blockIndex, data, 0, data.length, this);
    rawColumnChunk.setFileHolder(fileReader);
    rawColumnChunk.setPagesCount(1);
    rawColumnChunk.setRowCount(new int[]{numberOfRows});
    return rawColumnChunk;
  }

  private DimensionRawColumnChunk[] readRawDimensionChunksInGroup(FileHolder fileReader,
      int startBlockIndex, int endBlockIndex) throws IOException {
    long currentDimensionOffset = dimensionChunksOffset.get(startBlockIndex);
    byte[] data = fileReader.readByteArray(filePath, currentDimensionOffset,
        (int) (dimensionChunksOffset.get(endBlockIndex + 1) - currentDimensionOffset));
    DimensionRawColumnChunk[] dataChunks =
        new DimensionRawColumnChunk[endBlockIndex - startBlockIndex + 1];
    int index = 0;
    int runningLength = 0;
    for (int i = startBlockIndex; i <= endBlockIndex; i++) {
      int currentLength = (int)(dimensionChunksOffset.get(i + 1) - dimensionChunksOffset.get(i));
      dataChunks[index] = new DimensionRawColumnChunk(i, data, runningLength, currentLength, this);
      dataChunks[index].setFileHolder(fileReader);
      dataChunks[index].setPagesCount(1);
      dataChunks[index].setRowCount(new int[]{numberOfRows});
      runningLength += currentLength;
      index++;
    }
    return dataChunks;
  }

  public DimensionColumnDataChunk convertToDimensionChunk(FileHolder fileReader,
      int blockIndex, byte[] rawData, int offset, int length, int pageNumber) throws IOException {
    byte[] dataPage = null;
    int[] invertedIndexes = null;
    int[] invertedIndexesReverse = null;
    int[] rlePage = null;
    DataChunk2 dimensionColumnChunk = null;
    byte[] data = null;
    int copySourcePoint = offset;
    if (dimensionChunksOffset.size() - 1 == blockIndex) {
      dimensionColumnChunk = CarbonUtil
          .readDataChunk(rawData, copySourcePoint, length);
      int totalDimensionDataLength =
          dimensionColumnChunk.data_page_length + dimensionColumnChunk.rle_page_length
              + dimensionColumnChunk.rowid_page_length;
      data = fileReader.readByteArray(filePath,
          dimensionChunksOffset.get(blockIndex) + dimensionChunksLength.get(blockIndex),
          totalDimensionDataLength);
    } else {
      data = rawData;
      dimensionColumnChunk =
          CarbonUtil.readDataChunk(data, copySourcePoint, dimensionChunksLength.get(blockIndex));
      copySourcePoint += dimensionChunksLength.get(blockIndex);
    }

    // first read the data and uncompressed it
    dataPage =
        COMPRESSOR.unCompressByte(data, copySourcePoint, dimensionColumnChunk.data_page_length);
    copySourcePoint += dimensionColumnChunk.data_page_length;
    // if row id block is present then read the row id chunk and uncompress it
    if (hasEncoding(dimensionColumnChunk.encoders, Encoding.INVERTED_INDEX)) {
      invertedIndexes = CarbonUtil
          .getUnCompressColumnIndex(dimensionColumnChunk.rowid_page_length, data, numberComressor,
              copySourcePoint);
      copySourcePoint += dimensionColumnChunk.rowid_page_length;
      // get the reverse index
      invertedIndexesReverse = getInvertedReverseIndex(invertedIndexes);
    }
    // if rle is applied then read the rle block chunk and then uncompress
    //then actual data based on rle block
    if (hasEncoding(dimensionColumnChunk.encoders, Encoding.RLE)) {
      rlePage =
          numberComressor.unCompress(data, copySourcePoint, dimensionColumnChunk.rle_page_length);
      // uncompress the data with rle indexes
      dataPage = UnBlockIndexer.uncompressData(dataPage, rlePage, eachColumnValueSize[blockIndex]);
      rlePage = null;
    }
    // fill chunk attributes
    DimensionColumnDataChunk columnDataChunk = null;

    if (dimensionColumnChunk.isRowMajor()) {
      // to store fixed length column chunk values
      columnDataChunk = new ColumnGroupDimensionDataChunk(dataPage, eachColumnValueSize[blockIndex],
          numberOfRows);
    }
    // if no dictionary column then first create a no dictionary column chunk
    // and set to data chunk instance
    else if (!hasEncoding(dimensionColumnChunk.encoders, Encoding.DICTIONARY)) {
      columnDataChunk =
          new VariableLengthDimensionDataChunk(dataPage, invertedIndexes, invertedIndexesReverse,
              numberOfRows);
    } else {
      // to store fixed length column chunk values
      columnDataChunk =
          new FixedLengthDimensionDataChunk(dataPage, invertedIndexes, invertedIndexesReverse,
              numberOfRows, eachColumnValueSize[blockIndex]);
    }
    return columnDataChunk;
  }

  /**
   * Below method will be used to check whether particular encoding is present
   * in the dimension or not
   *
   * @param encoding encoding to search
   * @return if encoding is present in dimension
   */
  private boolean hasEncoding(List<Encoding> encodings, Encoding encoding) {
    return encodings.contains(encoding);
  }

}
