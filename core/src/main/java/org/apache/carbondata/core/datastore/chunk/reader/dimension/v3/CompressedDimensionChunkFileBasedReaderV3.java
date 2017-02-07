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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.ColumnGroupDimensionDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.VariableLengthDimensionDataChunk;
import org.apache.carbondata.core.datastore.chunk.reader.dimension.v2.CompressedDimensionChunkFileBasedReaderV2;
import org.apache.carbondata.core.datastore.columnar.UnBlockIndexer;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.DataChunk3;
import org.apache.carbondata.format.Encoding;
import org.apache.commons.lang.ArrayUtils;

public class CompressedDimensionChunkFileBasedReaderV3
    extends CompressedDimensionChunkFileBasedReaderV2 {

  private long dimensionOffsets;

  public CompressedDimensionChunkFileBasedReaderV3(BlockletInfo blockletInfo,
      int[] eachColumnValueSize, String filePath) {
    super(blockletInfo, eachColumnValueSize, filePath);
    dimensionOffsets = blockletInfo.getDimensionOffset();
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
  public DimensionRawColumnChunk readRawDimensionChunk(FileHolder fileReader, int blockIndex)
      throws IOException {
    long currentDimensionOffset = dimensionChunksOffset.get(blockIndex);
    int length = 0;
    if (dimensionChunksOffset.size() - 1 == blockIndex) {
      length = (int) (dimensionOffsets - currentDimensionOffset);
    } else {
      length = (int) (dimensionChunksOffset.get(blockIndex + 1) - currentDimensionOffset);
    }
    ByteBuffer buffer = ByteBuffer.allocateDirect(length);
    fileReader.readByteBuffer(filePath, buffer, currentDimensionOffset, length);
    DataChunk3 dataChunk = CarbonUtil.readDataChunk3(buffer, 0, length);
    DimensionRawColumnChunk rawColumnChunk =
        new DimensionRawColumnChunk(blockIndex, buffer, 0, length, this);
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
    rawColumnChunk.setFileHolder(fileReader);
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

  protected DimensionRawColumnChunk[] readRawDimensionChunksInGroup(FileHolder fileReader,
      int startBlockIndex, int endBlockIndex) throws IOException {
    long currentDimensionOffset = dimensionChunksOffset.get(startBlockIndex);
    ByteBuffer buffer = ByteBuffer.allocateDirect( (int) (dimensionChunksOffset.get(endBlockIndex + 1) - currentDimensionOffset));
    fileReader.readByteBuffer(filePath, buffer, currentDimensionOffset,
        (int) (dimensionChunksOffset.get(endBlockIndex + 1) - currentDimensionOffset));
    DimensionRawColumnChunk[] dimensionDataChunks =
        new DimensionRawColumnChunk[endBlockIndex - startBlockIndex + 1];
    int index = 0;
    int runningLength = 0;
    for (int i = startBlockIndex; i <= endBlockIndex; i++) {
      int currentLength = (int) (dimensionChunksOffset.get(i + 1) - dimensionChunksOffset.get(i));
      dimensionDataChunks[index] =
          new DimensionRawColumnChunk(i, buffer, runningLength, currentLength, this);
      DataChunk3 dataChunk =
          CarbonUtil.readDataChunk3(buffer, runningLength, dimensionChunksLength.get(i));
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
      dimensionDataChunks[index].setDataChunk3(dataChunk);
      dimensionDataChunks[index].setFileHolder(fileReader);
      dimensionDataChunks[index].setPagesCount(dataChunk.getPage_length().size());
      dimensionDataChunks[index].setMaxValues(maxValueOfEachPage);
      dimensionDataChunks[index].setMinValues(minValueOfEachPage);
      dimensionDataChunks[index].setRowCount(eachPageLength);
      dimensionDataChunks[index].setLengths(ArrayUtils
          .toPrimitive(dataChunk.page_length.toArray(new Integer[dataChunk.page_length.size()])));
      dimensionDataChunks[index].setOffsets(ArrayUtils
          .toPrimitive(dataChunk.page_offset.toArray(new Integer[dataChunk.page_offset.size()])));
      runningLength += currentLength;
      index++;
    }
    return dimensionDataChunks;
  }

  @Override public DimensionColumnDataChunk convertToDimensionChunk(
      DimensionRawColumnChunk dimensionRawColumnChunk, int pageNumber) throws IOException {
    byte[] dataPage = null;
    int[] invertedIndexes = null;
    int[] invertedIndexesReverse = null;
    int[] rlePage = null;
    DataChunk2 dimensionColumnChunk = null;
    DataChunk3 dataChunk3 = dimensionRawColumnChunk.getDataChunk3();
    ByteBuffer rawData = dimensionRawColumnChunk.getRawData();
    dimensionColumnChunk = dataChunk3.getData_chunk_list().get(pageNumber);
    int copySourcePoint = dimensionRawColumnChunk.getOffSet() + dimensionChunksLength
        .get(dimensionRawColumnChunk.getBlockId()) + dataChunk3.getPage_offset().get(pageNumber);
    long currentTimeMillis = System.nanoTime();
    byte[] data = new byte[dimensionColumnChunk.data_page_length];
    rawData.position(copySourcePoint);
    rawData.get(data);
    // first read the data and uncompressed it
    dataPage =
        COMPRESSOR.unCompressByte(data, 0, dimensionColumnChunk.data_page_length);
    dimensionRawColumnChunk.getFileReader().getStatisticObject().setTimeTakenForSnappyUnCompression((System.nanoTime()-currentTimeMillis));
    copySourcePoint += dimensionColumnChunk.data_page_length;
    // if row id block is present then read the row id chunk and uncompress it
    if (hasEncoding(dimensionColumnChunk.encoders, Encoding.INVERTED_INDEX)) {
      currentTimeMillis = System.nanoTime();
      invertedIndexes = CarbonUtil
          .getUnCompressColumnIndex(dimensionColumnChunk.rowid_page_length, rawData,
              copySourcePoint);
      copySourcePoint += dimensionColumnChunk.rowid_page_length;
      // get the reverse index
      invertedIndexesReverse = getInvertedReverseIndex(invertedIndexes);
      dimensionRawColumnChunk.getFileReader().getStatisticObject().setTimeTakenForInvertedIndex((System.nanoTime()-currentTimeMillis));
    }
    // if rle is applied then read the rle block chunk and then uncompress
    //then actual data based on rle block
    if (hasEncoding(dimensionColumnChunk.encoders, Encoding.RLE)) {
      currentTimeMillis = System.nanoTime();
      rlePage =
          CarbonUtil.getIntArray(rawData, copySourcePoint, dimensionColumnChunk.rle_page_length);
      // uncompress the data with rle indexes
      dataPage = UnBlockIndexer.uncompressData(dataPage, rlePage,
          eachColumnValueSize[dimensionRawColumnChunk.getBlockId()]);
      rlePage = null;
      dimensionRawColumnChunk.getFileReader().getStatisticObject().setTimeTakenForRle((System.nanoTime()-currentTimeMillis));
    }
    currentTimeMillis = System.nanoTime();
    // fill chunk attributes
    DimensionColumnDataChunk columnDataChunk = null;

    if (dimensionColumnChunk.isRowMajor()) {
      // to store fixed length column chunk values
      columnDataChunk = new ColumnGroupDimensionDataChunk(dataPage,
          eachColumnValueSize[dimensionRawColumnChunk.getBlockId()], dimensionRawColumnChunk.getRowCount()[pageNumber]);
    }
    // if no dictionary column then first create a no dictionary column chunk
    // and set to data chunk instance
    else if (!hasEncoding(dimensionColumnChunk.encoders, Encoding.DICTIONARY)) {
      columnDataChunk =
          new VariableLengthDimensionDataChunk(dataPage, invertedIndexes, invertedIndexesReverse,
              dimensionRawColumnChunk.getRowCount()[pageNumber]);
    } else {
      // to store fixed length column chunk values
      columnDataChunk =
          new FixedLengthDimensionDataChunk(dataPage, invertedIndexes, invertedIndexesReverse,
              dimensionRawColumnChunk.getRowCount()[pageNumber], eachColumnValueSize[dimensionRawColumnChunk.getBlockId()]);
    }
    dimensionRawColumnChunk.getFileReader().getStatisticObject().setTimeTakenForAllocatingTheObject(System.nanoTime()-currentTimeMillis);
    return columnDataChunk;
  }
}
