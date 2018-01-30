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
package org.apache.carbondata.core.datastore.chunk.reader.dimension.v1;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.ColumnGroupDimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.FixedLengthDimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.VariableLengthDimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.reader.dimension.AbstractChunkReader;
import org.apache.carbondata.core.datastore.columnar.UnBlockIndexer;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.datachunk.DataChunk;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * Compressed dimension chunk reader class
 */
public class CompressedDimensionChunkFileBasedReaderV1 extends AbstractChunkReader {

  /**
   * data chunk list which holds the information
   * about the data block metadata
   */
  private final List<DataChunk> dimensionColumnChunk;

  /**
   * Constructor to get minimum parameter to create instance of this class
   *
   * @param blockletInfo        blocklet info
   * @param eachColumnValueSize size of the each column value
   * @param filePath            file from which data will be read
   */
  public CompressedDimensionChunkFileBasedReaderV1(final BlockletInfo blockletInfo,
      final int[] eachColumnValueSize, final String filePath) {
    super(eachColumnValueSize, filePath, blockletInfo.getNumberOfRows());
    this.dimensionColumnChunk = blockletInfo.getDimensionColumnChunk();
  }

  /**
   * Below method will be used to read the raw chunk based on block indexes
   *
   * @param fileReader   file reader to read the blocks from file
   * @param columnIndexRange blocks to be read
   * @return dimension column chunks
   */
  @Override public DimensionRawColumnChunk[] readRawDimensionChunks(FileReader fileReader,
      int[][] columnIndexRange) throws IOException {
    DimensionRawColumnChunk[] dataChunks = new DimensionRawColumnChunk[dimensionColumnChunk.size()];
    for (int i = 0; i < columnIndexRange.length; i++) {
      for (int j = columnIndexRange[i][0]; j <= columnIndexRange[i][1]; j++) {
        dataChunks[j] = readRawDimensionChunk(fileReader, j);
      }
    }
    return dataChunks;
  }

  /**
   * Below method will be used to read the raw chunk based on block index
   *
   * @param fileReader file reader to read the blocks from file
   * @param columnIndex column to be read
   * @return dimension column chunk
   */
  @Override public DimensionRawColumnChunk readRawDimensionChunk(FileReader fileReader,
      int columnIndex) throws IOException {
    DataChunk dataChunk = dimensionColumnChunk.get(columnIndex);
    ByteBuffer buffer = null;
    synchronized (fileReader) {
      buffer = fileReader
          .readByteBuffer(filePath, dataChunk.getDataPageOffset(), dataChunk.getDataPageLength());
    }
    DimensionRawColumnChunk rawColumnChunk = new DimensionRawColumnChunk(columnIndex, buffer, 0,
        dataChunk.getDataPageLength(), this);
    rawColumnChunk.setFileReader(fileReader);
    rawColumnChunk.setPagesCount(1);
    rawColumnChunk.setRowCount(new int[] { numberOfRows });
    return rawColumnChunk;
  }

  @Override public DimensionColumnPage decodeColumnPage(
      DimensionRawColumnChunk dimensionRawColumnChunk, int pageNumber) throws IOException {
    int blockIndex = dimensionRawColumnChunk.getColumnIndex();
    byte[] dataPage = null;
    int[] invertedIndexes = null;
    int[] invertedIndexesReverse = null;
    int[] rlePage = null;
    FileReader fileReader = dimensionRawColumnChunk.getFileReader();

    ByteBuffer rawData = dimensionRawColumnChunk.getRawData();
    dataPage = COMPRESSOR.unCompressByte(rawData.array(), (int) dimensionRawColumnChunk.getOffSet(),
        dimensionRawColumnChunk.getLength());

    // if row id block is present then read the row id chunk and uncompress it
    DataChunk dataChunk = dimensionColumnChunk.get(blockIndex);
    if (CarbonUtil.hasEncoding(dataChunk.getEncodingList(),
        Encoding.INVERTED_INDEX)) {
      byte[] columnIndexData;
      synchronized (fileReader) {
        columnIndexData = fileReader
            .readByteArray(filePath, dataChunk.getRowIdPageOffset(),
                dataChunk.getRowIdPageLength());
      }
      invertedIndexes = CarbonUtil
          .getUnCompressColumnIndex(dataChunk.getRowIdPageLength(),
              columnIndexData, numberComressor, 0);
      // get the reverse index
      invertedIndexesReverse = getInvertedReverseIndex(invertedIndexes);
    }
    // if rle is applied then read the rle block chunk and then uncompress
    //then actual data based on rle block
    if (CarbonUtil
        .hasEncoding(dataChunk.getEncodingList(), Encoding.RLE)) {
      // read and uncompress the rle block
      byte[] key;
      synchronized (fileReader) {
        key = fileReader
            .readByteArray(filePath, dataChunk.getRlePageOffset(),
                dataChunk.getRlePageLength());
      }
      rlePage = numberComressor
          .unCompress(key, 0, dataChunk.getRlePageLength());
      // uncompress the data with rle indexes
      dataPage = UnBlockIndexer.uncompressData(dataPage, rlePage, eachColumnValueSize[blockIndex]);
      rlePage = null;
    }
    // fill chunk attributes
    DimensionColumnPage columnDataChunk = null;
    if (dataChunk.isRowMajor()) {
      // to store fixed length column chunk values
      columnDataChunk = new ColumnGroupDimensionColumnPage(
          dataPage, eachColumnValueSize[blockIndex], numberOfRows);
    }
    // if no dictionary column then first create a no dictionary column chunk
    // and set to data chunk instance
    else if (!CarbonUtil
        .hasEncoding(dataChunk.getEncodingList(), Encoding.DICTIONARY)) {
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
