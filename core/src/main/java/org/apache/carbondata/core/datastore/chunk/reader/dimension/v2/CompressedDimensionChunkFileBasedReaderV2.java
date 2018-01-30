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
import java.nio.ByteBuffer;

import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.ColumnGroupDimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.FixedLengthDimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.VariableLengthDimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.reader.dimension.AbstractChunkReaderV2V3Format;
import org.apache.carbondata.core.datastore.columnar.UnBlockIndexer;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.Encoding;

/**
 * Compressed dimension chunk reader class for version 2
 */
public class CompressedDimensionChunkFileBasedReaderV2 extends AbstractChunkReaderV2V3Format {

  /**
   * Constructor to get minimum parameter to create instance of this class
   *
   * @param blockletInfo
   * @param eachColumnValueSize
   * @param filePath
   */
  public CompressedDimensionChunkFileBasedReaderV2(final BlockletInfo blockletInfo,
      final int[] eachColumnValueSize, final String filePath) {
    super(blockletInfo, eachColumnValueSize, filePath);
  }

  /**
   * Below method will be used to read the chunk based on block index
   *
   * @param fileReader    file reader to read the blocks from file
   * @param columnIndex   column to be read
   * @return dimension column chunk
   */
  public DimensionRawColumnChunk readRawDimensionChunk(FileReader fileReader, int columnIndex)
      throws IOException {
    int length = 0;
    if (dimensionChunksOffset.size() - 1 == columnIndex) {
      // Incase of last block read only for datachunk and read remaining while converting it.
      length = dimensionChunksLength.get(columnIndex);
    } else {
      long currentDimensionOffset = dimensionChunksOffset.get(columnIndex);
      length = (int) (dimensionChunksOffset.get(columnIndex + 1) - currentDimensionOffset);
    }
    ByteBuffer buffer = null;
    synchronized (fileReader) {
      buffer =
          fileReader.readByteBuffer(filePath, dimensionChunksOffset.get(columnIndex), length);
    }
    DimensionRawColumnChunk rawColumnChunk =
        new DimensionRawColumnChunk(columnIndex, buffer, 0, length, this);
    rawColumnChunk.setFileReader(fileReader);
    rawColumnChunk.setPagesCount(1);
    rawColumnChunk.setRowCount(new int[] { numberOfRows });
    return rawColumnChunk;
  }

  /**
   * Below method will be used to read measure chunk data in group.
   * This method will be useful to avoid multiple IO while reading the
   * data from
   *
   * @param fileReader               file reader to read the data
   * @param startColumnBlockletIndex first column blocklet index to be read
   * @param endColumnBlockletIndex   end column blocklet index to be read
   * @return measure raw chunkArray
   * @throws IOException
   */
  protected DimensionRawColumnChunk[] readRawDimensionChunksInGroup(FileReader fileReader,
      int startColumnBlockletIndex, int endColumnBlockletIndex) throws IOException {
    long currentDimensionOffset = dimensionChunksOffset.get(startColumnBlockletIndex);
    ByteBuffer buffer = null;
    synchronized (fileReader) {
      buffer = fileReader.readByteBuffer(filePath, currentDimensionOffset,
          (int) (dimensionChunksOffset.get(endColumnBlockletIndex + 1) - currentDimensionOffset));
    }
    DimensionRawColumnChunk[] dataChunks =
        new DimensionRawColumnChunk[endColumnBlockletIndex - startColumnBlockletIndex + 1];
    int index = 0;
    int runningLength = 0;
    for (int i = startColumnBlockletIndex; i <= endColumnBlockletIndex; i++) {
      int currentLength = (int) (dimensionChunksOffset.get(i + 1) - dimensionChunksOffset.get(i));
      dataChunks[index] =
          new DimensionRawColumnChunk(i, buffer, runningLength, currentLength, this);
      dataChunks[index].setFileReader(fileReader);
      dataChunks[index].setPagesCount(1);
      dataChunks[index].setRowCount(new int[] { numberOfRows });
      runningLength += currentLength;
      index++;
    }
    return dataChunks;
  }

  public DimensionColumnPage decodeColumnPage(
      DimensionRawColumnChunk dimensionRawColumnChunk, int pageNumber) throws IOException {
    byte[] dataPage = null;
    int[] invertedIndexes = null;
    int[] invertedIndexesReverse = null;
    int[] rlePage = null;
    DataChunk2 dimensionColumnChunk = null;
    int copySourcePoint = (int) dimensionRawColumnChunk.getOffSet();
    int blockIndex = dimensionRawColumnChunk.getColumnIndex();
    ByteBuffer rawData = dimensionRawColumnChunk.getRawData();
    if (dimensionChunksOffset.size() - 1 == blockIndex) {
      dimensionColumnChunk =
          CarbonUtil.readDataChunk(rawData, copySourcePoint, dimensionRawColumnChunk.getLength());
      int totalDimensionDataLength =
          dimensionColumnChunk.data_page_length + dimensionColumnChunk.rle_page_length
              + dimensionColumnChunk.rowid_page_length;
      synchronized (dimensionRawColumnChunk.getFileReader()) {
        rawData = dimensionRawColumnChunk.getFileReader().readByteBuffer(filePath,
            dimensionChunksOffset.get(blockIndex) + dimensionChunksLength.get(blockIndex),
            totalDimensionDataLength);
      }
    } else {
      dimensionColumnChunk =
          CarbonUtil.readDataChunk(rawData, copySourcePoint, dimensionChunksLength.get(blockIndex));
      copySourcePoint += dimensionChunksLength.get(blockIndex);
    }

    // first read the data and uncompressed it
    dataPage = COMPRESSOR
        .unCompressByte(rawData.array(), copySourcePoint, dimensionColumnChunk.data_page_length);
    copySourcePoint += dimensionColumnChunk.data_page_length;
    // if row id block is present then read the row id chunk and uncompress it
    if (hasEncoding(dimensionColumnChunk.encoders, Encoding.INVERTED_INDEX)) {
      byte[] dataInv = new byte[dimensionColumnChunk.rowid_page_length];
      rawData.position(copySourcePoint);
      rawData.get(dataInv);
      invertedIndexes = CarbonUtil
          .getUnCompressColumnIndex(dimensionColumnChunk.rowid_page_length, dataInv,
              numberComressor, 0);
      copySourcePoint += dimensionColumnChunk.rowid_page_length;
      // get the reverse index
      invertedIndexesReverse = getInvertedReverseIndex(invertedIndexes);
    }
    // if rle is applied then read the rle block chunk and then uncompress
    //then actual data based on rle block
    if (hasEncoding(dimensionColumnChunk.encoders, Encoding.RLE)) {
      byte[] dataRle = new byte[dimensionColumnChunk.rle_page_length];
      rawData.position(copySourcePoint);
      rawData.get(dataRle);
      rlePage = numberComressor.unCompress(dataRle, 0, dimensionColumnChunk.rle_page_length);
      // uncompress the data with rle indexes
      dataPage = UnBlockIndexer.uncompressData(dataPage, rlePage, eachColumnValueSize[blockIndex]);
    }
    // fill chunk attributes
    DimensionColumnPage columnDataChunk = null;

    if (dimensionColumnChunk.isRowMajor()) {
      // to store fixed length column chunk values
      columnDataChunk = new ColumnGroupDimensionColumnPage(
          dataPage, eachColumnValueSize[blockIndex], numberOfRows);
    }
    // if no dictionary column then first create a no dictionary column chunk
    // and set to data chunk instance
    else if (!hasEncoding(dimensionColumnChunk.encoders, Encoding.DICTIONARY)) {
      columnDataChunk =
          new VariableLengthDimensionColumnPage(dataPage, invertedIndexes, invertedIndexesReverse,
              numberOfRows);
    } else {
      // to store fixed length column chunk values
      columnDataChunk =
          new FixedLengthDimensionColumnPage(dataPage, invertedIndexes, invertedIndexesReverse,
              numberOfRows, eachColumnValueSize[blockIndex]);
    }
    return columnDataChunk;
  }
}
