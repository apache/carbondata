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
import java.util.List;

import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.ColumnGroupDimensionDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.VariableLengthDimensionDataChunk;
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
   * Below method will be used to read the chunk based on block indexes
   *
   * @param fileReader   file reader to read the blocks from file
   * @param blockIndexes blocks to be read
   * @return dimension column chunks
   */
  @Override public DimensionColumnDataChunk[] readDimensionChunks(FileHolder fileReader,
      int[][] blockIndexes) throws IOException {
    // read the column chunk based on block index and add
    DimensionColumnDataChunk[] dataChunks =
        new DimensionColumnDataChunk[dimensionColumnChunk.size()];
    for (int i = 0; i < blockIndexes.length; i++) {
      for (int j = blockIndexes[i][0]; j <= blockIndexes[i][1]; j++) {
        dataChunks[j] = readDimensionChunk(fileReader, j);
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
  @Override public DimensionColumnDataChunk readDimensionChunk(FileHolder fileReader,
      int blockIndex) throws IOException {
    byte[] dataPage = null;
    int[] invertedIndexes = null;
    int[] invertedIndexesReverse = null;
    int[] rlePage = null;

    // first read the data and uncompressed it
    dataPage = COMPRESSOR.unCompressByte(fileReader
        .readByteArray(filePath, dimensionColumnChunk.get(blockIndex).getDataPageOffset(),
            dimensionColumnChunk.get(blockIndex).getDataPageLength()));
    // if row id block is present then read the row id chunk and uncompress it
    if (CarbonUtil.hasEncoding(dimensionColumnChunk.get(blockIndex).getEncodingList(),
        Encoding.INVERTED_INDEX)) {
      invertedIndexes = CarbonUtil
          .getUnCompressColumnIndex(dimensionColumnChunk.get(blockIndex).getRowIdPageLength(),
              fileReader.readByteArray(filePath,
                  dimensionColumnChunk.get(blockIndex).getRowIdPageOffset(),
                  dimensionColumnChunk.get(blockIndex).getRowIdPageLength()), numberComressor, 0);
      // get the reverse index
      invertedIndexesReverse = getInvertedReverseIndex(invertedIndexes);
    }
    // if rle is applied then read the rle block chunk and then uncompress
    //then actual data based on rle block
    if (CarbonUtil
        .hasEncoding(dimensionColumnChunk.get(blockIndex).getEncodingList(), Encoding.RLE)) {
      // read and uncompress the rle block
      rlePage = numberComressor.unCompress(fileReader
              .readByteArray(filePath, dimensionColumnChunk.get(blockIndex).getRlePageOffset(),
                  dimensionColumnChunk.get(blockIndex).getRlePageLength()), 0,
          dimensionColumnChunk.get(blockIndex).getRlePageLength());
      // uncompress the data with rle indexes
      dataPage = UnBlockIndexer.uncompressData(dataPage, rlePage, eachColumnValueSize[blockIndex]);
      rlePage = null;
    }
    // fill chunk attributes
    DimensionColumnDataChunk columnDataChunk = null;
    if (dimensionColumnChunk.get(blockIndex).isRowMajor()) {
      // to store fixed length column chunk values
      columnDataChunk = new ColumnGroupDimensionDataChunk(dataPage, eachColumnValueSize[blockIndex],
          numberOfRows);
    }
    // if no dictionary column then first create a no dictionary column chunk
    // and set to data chunk instance
    else if (!CarbonUtil
        .hasEncoding(dimensionColumnChunk.get(blockIndex).getEncodingList(), Encoding.DICTIONARY)) {
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

}
