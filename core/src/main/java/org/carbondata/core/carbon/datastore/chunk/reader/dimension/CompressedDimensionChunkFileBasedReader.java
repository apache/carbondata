/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.core.carbon.datastore.chunk.reader.dimension;

import java.util.List;

import org.carbondata.core.carbon.datastore.chunk.DimensionChunkAttributes;
import org.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.carbondata.core.carbon.datastore.chunk.impl.ColumnGroupDimensionDataChunk;
import org.carbondata.core.carbon.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.carbondata.core.carbon.datastore.chunk.impl.VariableLengthDimensionDataChunk;
import org.carbondata.core.carbon.metadata.blocklet.datachunk.DataChunk;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.columnar.UnBlockIndexer;
import org.carbondata.core.util.CarbonUtil;

/**
 * Compressed dimension chunk reader class
 */
public class CompressedDimensionChunkFileBasedReader extends AbstractChunkReader {

  /**
   * Constructor to get minimum parameter to create instance of this class
   *
   * @param dimensionColumnChunk dimension chunk metadata
   * @param eachColumnValueSize  size of the each column value
   * @param filePath             file from which data will be read
   */
  public CompressedDimensionChunkFileBasedReader(List<DataChunk> dimensionColumnChunk,
      int[] eachColumnValueSize, String filePath) {
    super(dimensionColumnChunk, eachColumnValueSize, filePath);
  }

  /**
   * Below method will be used to read the chunk based on block indexes
   *
   * @param fileReader   file reader to read the blocks from file
   * @param blockIndexes blocks to be read
   * @return dimension column chunks
   */
  @Override public DimensionColumnDataChunk[] readDimensionChunks(FileHolder fileReader,
      int... blockIndexes) {
    // read the column chunk based on block index and add
    DimensionColumnDataChunk[] dataChunks =
        new DimensionColumnDataChunk[dimensionColumnChunk.size()];
    for (int i = 0; i < blockIndexes.length; i++) {
      dataChunks[blockIndexes[i]] = readDimensionChunk(fileReader, blockIndexes[i]);
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
      int blockIndex) {
    byte[] dataPage = null;
    int[] invertedIndexes = null;
    int[] invertedIndexesReverse = null;
    int[] rlePage = null;

    // first read the data and uncompressed it
    dataPage = COMPRESSOR.unCompress(fileReader
        .readByteArray(filePath, dimensionColumnChunk.get(blockIndex).getDataPageOffset(),
            dimensionColumnChunk.get(blockIndex).getDataPageLength()));
    // if row id block is present then read the row id chunk and uncompress it
    if (CarbonUtil.hasEncoding(dimensionColumnChunk.get(blockIndex).getEncodingList(),
        Encoding.INVERTED_INDEX)) {
      invertedIndexes = CarbonUtil
          .getUnCompressColumnIndex(dimensionColumnChunk.get(blockIndex).getRowIdPageLength(),
              fileReader.readByteArray(filePath,
                  dimensionColumnChunk.get(blockIndex).getRowIdPageOffset(),
                  dimensionColumnChunk.get(blockIndex).getRowIdPageLength()), numberComressor);
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
              dimensionColumnChunk.get(blockIndex).getRlePageLength()));
      // uncompress the data with rle indexes
      dataPage = UnBlockIndexer.uncompressData(dataPage, rlePage, eachColumnValueSize[blockIndex]);
      rlePage = null;
    }
    // fill chunk attributes
    DimensionChunkAttributes chunkAttributes = new DimensionChunkAttributes();
    chunkAttributes.setEachRowSize(eachColumnValueSize[blockIndex]);
    chunkAttributes.setInvertedIndexes(invertedIndexes);
    chunkAttributes.setInvertedIndexesReverse(invertedIndexesReverse);
    DimensionColumnDataChunk columnDataChunk = null;

    if (dimensionColumnChunk.get(blockIndex).isRowMajor()) {
      // to store fixed length column chunk values
      columnDataChunk = new ColumnGroupDimensionDataChunk(dataPage, chunkAttributes);
    }
    // if no dictionary column then first create a no dictionary column chunk
    // and set to data chunk instance
    else if (!CarbonUtil
        .hasEncoding(dimensionColumnChunk.get(blockIndex).getEncodingList(), Encoding.DICTIONARY)) {
      columnDataChunk =
          new VariableLengthDimensionDataChunk(getNoDictionaryDataChunk(dataPage), chunkAttributes);
      chunkAttributes.setNoDictionary(true);
    } else {
      // to store fixed length column chunk values
      columnDataChunk = new FixedLengthDimensionDataChunk(dataPage, chunkAttributes);
    }
    return columnDataChunk;
  }

}
