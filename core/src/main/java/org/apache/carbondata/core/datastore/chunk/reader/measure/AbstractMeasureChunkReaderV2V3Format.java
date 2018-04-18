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
package org.apache.carbondata.core.datastore.chunk.reader.measure;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;

/**
 * Abstract class for V2, V3 format measure column reader
 */
public abstract class AbstractMeasureChunkReaderV2V3Format extends AbstractMeasureChunkReader {

  /**
   * measure column chunks offset
   */
  protected List<Long> measureColumnChunkOffsets;

  /**
   * measure column chunks length
   */
  protected List<Integer> measureColumnChunkLength;

  public AbstractMeasureChunkReaderV2V3Format(final BlockletInfo blockletInfo,
      final String filePath) {
    super(filePath, blockletInfo.getNumberOfRows());
    this.measureColumnChunkOffsets = blockletInfo.getMeasureChunkOffsets();
    this.measureColumnChunkLength = blockletInfo.getMeasureChunksLength();
  }

  /**
   * Below method will be used to read the chunk based on block indexes
   * Reading logic of below method is: Except last column all the column chunk
   * can be read in group if not last column then read data of all the column
   * present in block index together then process it. For last column read is
   * separately and process
   *
   * @param fileReader   file reader to read the blocks from file
   * @param columnIndexRange blocks range to be read, columnIndexGroup[i] is one group, inside the
   *                         group, columnIndexGroup[i][0] is start column index,
   *                         and columnIndexGroup[i][1] is end column index
   * @return measure column chunks
   * @throws IOException
   */
  public MeasureRawColumnChunk[] readRawMeasureChunks(FileReader fileReader,
      int[][] columnIndexRange) throws IOException {
    // read the column chunk based on block index and add
    MeasureRawColumnChunk[] dataChunks =
        new MeasureRawColumnChunk[measureColumnChunkOffsets.size()];
    if (columnIndexRange.length == 0) {
      return dataChunks;
    }
    MeasureRawColumnChunk[] groupChunk = null;
    int index = 0;
    for (int i = 0; i < columnIndexRange.length - 1; i++) {
      index = 0;
      groupChunk = readRawMeasureChunksInGroup(
          fileReader, columnIndexRange[i][0], columnIndexRange[i][1]);
      for (int j = columnIndexRange[i][0]; j <= columnIndexRange[i][1]; j++) {
        dataChunks[j] = groupChunk[index++];
      }
    }
    if (columnIndexRange[columnIndexRange.length - 1][0] == measureColumnChunkOffsets.size() - 1) {
      dataChunks[columnIndexRange[columnIndexRange.length - 1][0]] =
          readRawMeasureChunk(fileReader, columnIndexRange[columnIndexRange.length - 1][0]);
    } else {
      groupChunk = readRawMeasureChunksInGroup(
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
   * Below method will be used to convert the thrift presence meta to wrapper
   * presence meta
   *
   * @param presentMetadataThrift
   * @return wrapper presence meta
   */
  protected BitSet getNullBitSet(
      org.apache.carbondata.format.PresenceMeta presentMetadataThrift) {
    Compressor compressor = CompressorFactory.getInstance().getCompressor();
    return BitSet.valueOf(
        compressor.unCompressByte(presentMetadataThrift.getPresent_bit_stream()));
  }

  /**
   * Below method will be used to read measure chunk data in group.
   * This method will be useful to avoid multiple IO while reading the
   * data from
   *
   * @param fileReader               file reader to read the data
   * @param startColumnIndex first column index to be read
   * @param endColumnIndex   end column index to be read
   * @return measure raw chunkArray
   * @throws IOException
   */
  protected abstract MeasureRawColumnChunk[] readRawMeasureChunksInGroup(FileReader fileReader,
      int startColumnIndex, int endColumnIndex) throws IOException;

}
