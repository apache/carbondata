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
package org.apache.carbondata.core.carbon.datastore.impl.btree;

import org.apache.carbondata.core.carbon.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.reader.DimensionColumnChunkReader;
import org.apache.carbondata.core.carbon.datastore.chunk.reader.MeasureColumnChunkReader;
import org.apache.carbondata.core.carbon.datastore.chunk.reader.dimension.CompressedDimensionChunkFileBasedReader;
import org.apache.carbondata.core.carbon.datastore.chunk.reader.measure.CompressedMeasureChunkFileBasedReader;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * Leaf node class of a Blocklet btree
 */
public class BlockletBTreeLeafNode extends AbstractBTreeLeafNode {

  /**
   * reader for dimension chunk
   */
  private DimensionColumnChunkReader dimensionChunksReader;

  /**
   * reader of measure chunk
   */
  private MeasureColumnChunkReader measureColumnChunkReader;

  /**
   * Create a leaf node
   *
   * @param builderInfos builder infos which have required metadata to create a leaf node
   * @param leafIndex    leaf node index
   * @param nodeNumber   node number of the node
   *                     this will be used during query execution when we can
   *                     give some leaf node of a btree to one executor some to other
   */
  public BlockletBTreeLeafNode(BTreeBuilderInfo builderInfos, int leafIndex, long nodeNumber) {
    // get a lead node min max
    BlockletMinMaxIndex minMaxIndex =
        builderInfos.getFooterList().get(0).getBlockletList().get(leafIndex)
            .getBlockletIndex().getMinMaxIndex();
    // max key of the columns
    maxKeyOfColumns = minMaxIndex.getMaxValues();
    // min keys of the columns
    minKeyOfColumns = minMaxIndex.getMinValues();
    // number of keys present in the leaf
    numberOfKeys = builderInfos.getFooterList().get(0).getBlockletList().get(leafIndex)
        .getNumberOfRows();
    // create a instance of dimension chunk
    dimensionChunksReader = new CompressedDimensionChunkFileBasedReader(
        builderInfos.getFooterList().get(0).getBlockletList().get(leafIndex)
            .getDimensionColumnChunk(), builderInfos.getDimensionColumnValueSize(),
        builderInfos.getFooterList().get(0).getBlockInfo().getTableBlockInfo().getFilePath());
    // get the value compression model which was used to compress the measure values
    ValueCompressionModel valueCompressionModel = CarbonUtil.getValueCompressionModel(
        builderInfos.getFooterList().get(0).getBlockletList().get(leafIndex)
            .getMeasureColumnChunk());
    // create a instance of measure column chunk reader
    measureColumnChunkReader = new CompressedMeasureChunkFileBasedReader(
        builderInfos.getFooterList().get(0).getBlockletList().get(leafIndex)
            .getMeasureColumnChunk(), valueCompressionModel,
            builderInfos.getFooterList().get(0).getBlockInfo().getTableBlockInfo().getFilePath());
    this.nodeNumber = nodeNumber;
  }

  /**
   * Below method will be used to get the dimension chunks
   *
   * @param fileReader   file reader to read the chunks from file
   * @param blockIndexes indexes of the blocks need to be read
   * @return dimension data chunks
   */
  @Override public DimensionColumnDataChunk[] getDimensionChunks(FileHolder fileReader,
      int[] blockIndexes) {
    return dimensionChunksReader.readDimensionChunks(fileReader, blockIndexes);
  }

  /**
   * Below method will be used to get the dimension chunk
   *
   * @param fileReader file reader to read the chunk from file
   * @param blockIndex block index to be read
   * @return dimension data chunk
   */
  @Override public DimensionColumnDataChunk getDimensionChunk(FileHolder fileReader,
      int blockIndex) {
    return dimensionChunksReader.readDimensionChunk(fileReader, blockIndex);
  }

  /**
   * Below method will be used to get the measure chunk
   *
   * @param fileReader   file reader to read the chunk from file
   * @param blockIndexes block indexes to be read from file
   * @return measure column data chunk
   */
  @Override public MeasureColumnDataChunk[] getMeasureChunks(FileHolder fileReader,
      int[] blockIndexes) {
    return measureColumnChunkReader.readMeasureChunks(fileReader, blockIndexes);
  }

  /**
   * Below method will be used to read the measure chunk
   *
   * @param fileReader file read to read the file chunk
   * @param blockIndex block index to be read from file
   * @return measure data chunk
   */
  @Override public MeasureColumnDataChunk getMeasureChunk(FileHolder fileReader, int blockIndex) {
    return measureColumnChunkReader.readMeasureChunk(fileReader, blockIndex);
  }
}
