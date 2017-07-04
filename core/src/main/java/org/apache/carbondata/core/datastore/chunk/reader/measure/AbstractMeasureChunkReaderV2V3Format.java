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

import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageCodec;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.datachunk.PresenceMeta;
import org.apache.carbondata.format.DataChunk2;

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
   * @param blockIndexes blocks range to be read
   * @return measure column chunks
   * @throws IOException
   */
  public MeasureRawColumnChunk[] readRawMeasureChunks(FileHolder fileReader, int[][] blockIndexes)
      throws IOException {
    // read the column chunk based on block index and add
    MeasureRawColumnChunk[] dataChunks =
        new MeasureRawColumnChunk[measureColumnChunkOffsets.size()];
    if (blockIndexes.length == 0) {
      return dataChunks;
    }
    MeasureRawColumnChunk[] groupChunk = null;
    int index = 0;
    for (int i = 0; i < blockIndexes.length - 1; i++) {
      index = 0;
      groupChunk = readRawMeasureChunksInGroup(fileReader, blockIndexes[i][0], blockIndexes[i][1]);
      for (int j = blockIndexes[i][0]; j <= blockIndexes[i][1]; j++) {
        dataChunks[j] = groupChunk[index++];
      }
    }
    if (blockIndexes[blockIndexes.length - 1][0] == measureColumnChunkOffsets.size() - 1) {
      dataChunks[blockIndexes[blockIndexes.length - 1][0]] =
          readRawMeasureChunk(fileReader, blockIndexes[blockIndexes.length - 1][0]);
    } else {
      groupChunk = readRawMeasureChunksInGroup(fileReader, blockIndexes[blockIndexes.length - 1][0],
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
   * Below method will be used to convert the thrift presence meta to wrapper
   * presence meta
   *
   * @param presentMetadataThrift
   * @return wrapper presence meta
   */
  protected PresenceMeta getPresenceMeta(
      org.apache.carbondata.format.PresenceMeta presentMetadataThrift) {
    PresenceMeta presenceMeta = new PresenceMeta();
    presenceMeta.setRepresentNullValues(presentMetadataThrift.isRepresents_presence());
    presenceMeta.setBitSet(BitSet.valueOf(CompressorFactory.getInstance().getCompressor()
        .unCompressByte(presentMetadataThrift.getPresent_bit_stream())));
    return presenceMeta;
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
  protected abstract MeasureRawColumnChunk[] readRawMeasureChunksInGroup(FileHolder fileReader,
      int startColumnBlockletIndex, int endColumnBlockletIndex) throws IOException;


  protected ColumnPage decodeMeasure(MeasureRawColumnChunk measureRawColumnChunk,
      DataChunk2 measureColumnChunk, int copyPoint) throws MemoryException {
    // for measure, it should have only one ValueEncoderMeta
    assert (measureColumnChunk.getEncoder_meta().size() == 1);
    byte[] encodedMeta = measureColumnChunk.getEncoder_meta().get(0).array();
    ValueEncoderMeta meta = ValueEncoderMeta.deserialize(encodedMeta);
    ColumnPageCodec codec = strategy.createCodec(meta);
    byte[] rawData = measureRawColumnChunk.getRawData().array();
    return codec.decode(rawData, copyPoint, measureColumnChunk.data_page_length);
  }
}
