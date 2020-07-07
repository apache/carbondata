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

package org.apache.carbondata.core.datastore.chunk.reader.dimension;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.ReusableDataBuffer;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.reader.DimensionColumnChunkReader;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;

/**
 * Class which will have all the common properties and behavior among all type
 * of reader
 */
public abstract class AbstractDimensionChunkReader implements DimensionColumnChunkReader {

  /**
   * compressor will be used to uncompress the data
   */
  protected Compressor compressor;

  /**
   * full qualified path of the data file from
   * which data will be read
   */
  protected String filePath;

  /**
   * dimension chunks offset
   */
  protected List<Long> dimensionChunksOffset;
  /**
   * dimension chunks length
   */
  protected List<Integer> dimensionChunksLength;

  /**
   * Constructor to get minimum parameter to create
   * instance of this class
   * @param filePath             file from which data will be read
   */
  public AbstractDimensionChunkReader(final BlockletInfo blockletInfo, final String filePath) {
    this.filePath = filePath;
    dimensionChunksOffset = blockletInfo.getDimensionChunkOffsets();
    dimensionChunksLength = blockletInfo.getDimensionChunksLength();
  }

  @Override
  public void decodeColumnPageAndFillVector(DimensionRawColumnChunk dimensionRawColumnChunk,
      int pageNumber, ColumnVectorInfo vectorInfo, ReusableDataBuffer reusableDataBuffer)
      throws IOException {
    throw new UnsupportedOperationException(
        "This operation is not supported in this reader " + this.getClass().getName());
  }

  /**
   * Below method will be used to read the chunk based on block indexes
   * Reading logic of below method is:
   * Except last column all the column chunk can be read in group
   * if not last column then read data of all the column present in block index
   * together then process it.
   * For last column read is separately and process
   *
   * @param fileReader      file reader to read the blocks from file
   * @param columnIndexRange column index range to be read
   * @return dimension column chunks
   */
  @Override
  public DimensionRawColumnChunk[] readRawDimensionChunks(final FileReader fileReader,
      final int[][] columnIndexRange) throws IOException {
    // read the column chunk based on block index and add
    DimensionRawColumnChunk[] dataChunks =
        new DimensionRawColumnChunk[dimensionChunksOffset.size()];
    // if blocklet index is empty then return empty data chunk
    if (columnIndexRange.length == 0) {
      return dataChunks;
    }
    DimensionRawColumnChunk[] groupChunk = null;
    int index = 0;
    // iterate till block indexes -1 as block index will be in sorted order, so to avoid
    // the last column reading in group
    for (int i = 0; i < columnIndexRange.length - 1; i++) {
      index = 0;
      groupChunk =
          readRawDimensionChunksInGroup(fileReader, columnIndexRange[i][0], columnIndexRange[i][1]);
      for (int j = columnIndexRange[i][0]; j <= columnIndexRange[i][1]; j++) {
        dataChunks[j] = groupChunk[index++];
      }
    }
    // check last index is present in block index, if it is present then read separately
    if (columnIndexRange[columnIndexRange.length - 1][0] == dimensionChunksOffset.size() - 1) {
      dataChunks[columnIndexRange[columnIndexRange.length - 1][0]] =
          readRawDimensionChunk(fileReader, columnIndexRange[columnIndexRange.length - 1][0]);
    }
    // otherwise read the data in group
    else {
      groupChunk = readRawDimensionChunksInGroup(
          fileReader, columnIndexRange[columnIndexRange.length - 1][0],
          columnIndexRange[columnIndexRange.length - 1][1]);
      index = 0;
      for (int j = columnIndexRange[columnIndexRange.length - 1][0];
           j <= columnIndexRange[columnIndexRange.length - 1][1]; j++) {
        dataChunks[j] = groupChunk[index++];
      }
    }
    return dataChunks;
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
  protected abstract DimensionRawColumnChunk[] readRawDimensionChunksInGroup(FileReader fileReader,
      int startColumnBlockletIndex, int endColumnBlockletIndex) throws IOException;
}
