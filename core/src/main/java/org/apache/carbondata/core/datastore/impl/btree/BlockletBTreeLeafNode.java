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
package org.apache.carbondata.core.datastore.impl.btree;

import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.reader.CarbonDataReaderFactory;
import org.apache.carbondata.core.datastore.chunk.reader.DimensionColumnChunkReader;
import org.apache.carbondata.core.datastore.chunk.reader.MeasureColumnChunkReader;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;

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
   * number of pages in blocklet
   */
  private int numberOfPages;

  private int[] pageRowCount;

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
        builderInfos.getFooterList().get(0).getBlockletList().get(leafIndex).getBlockletIndex()
            .getMinMaxIndex();
    // max key of the columns
    maxKeyOfColumns = minMaxIndex.getMaxValues();
    // min keys of the columns
    minKeyOfColumns = minMaxIndex.getMinValues();
    // number of keys present in the leaf
    numberOfKeys =
        builderInfos.getFooterList().get(0).getBlockletList().get(leafIndex).getNumberOfRows();
    // create a instance of dimension chunk
    dimensionChunksReader = CarbonDataReaderFactory.getInstance()
        .getDimensionColumnChunkReader(builderInfos.getFooterList().get(0).getVersionId(),
            builderInfos.getFooterList().get(0).getBlockletList().get(leafIndex),
            builderInfos.getDimensionColumnValueSize(),
            builderInfos.getFooterList().get(0).getBlockInfo().getTableBlockInfo().getFilePath());
    // create a instance of measure column chunk reader
    measureColumnChunkReader = CarbonDataReaderFactory.getInstance()
        .getMeasureColumnChunkReader(builderInfos.getFooterList().get(0).getVersionId(),
            builderInfos.getFooterList().get(0).getBlockletList().get(leafIndex),
            builderInfos.getFooterList().get(0).getBlockInfo().getTableBlockInfo().getFilePath());
    this.nodeNumber = nodeNumber;
    this.numberOfPages =
        builderInfos.getFooterList().get(0).getBlockletList().get(leafIndex).getNumberOfPages();
    this.pageRowCount = new int[numberOfPages];
    int numberOfPagesCompletelyFilled =
        numberOfKeys / CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT;
    int lastPageRowCount =
        numberOfKeys % CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT;
    for (int i = 0; i < numberOfPagesCompletelyFilled; i++) {
      pageRowCount[i] = CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT;
    }
    if (lastPageRowCount > 0) {
      pageRowCount[pageRowCount.length - 1] = lastPageRowCount;
    }
  }

  @Override public String blockletId() {
    return "0";
  }

  /**
   * Below method will be used to get the dimension chunks
   *
   * @param fileReader   file reader to read the chunks from file
   * @param blockIndexes indexes of the blocks need to be read
   * @return dimension data chunks
   */
  @Override public DimensionRawColumnChunk[] getDimensionChunks(FileHolder fileReader,
      int[][] blockIndexes) throws IOException {
    return dimensionChunksReader.readRawDimensionChunks(fileReader, blockIndexes);
  }

  /**
   * Below method will be used to get the dimension chunk
   *
   * @param fileReader file reader to read the chunk from file
   * @param blockIndex block index to be read
   * @return dimension data chunk
   */
  @Override public DimensionRawColumnChunk getDimensionChunk(FileHolder fileReader, int blockIndex)
      throws IOException {
    return dimensionChunksReader.readRawDimensionChunk(fileReader, blockIndex);
  }

  /**
   * Below method will be used to get the measure chunk
   *
   * @param fileReader   file reader to read the chunk from file
   * @param blockIndexes block indexes to be read from file
   * @return measure column data chunk
   */
  @Override public MeasureRawColumnChunk[] getMeasureChunks(FileHolder fileReader,
      int[][] blockIndexes) throws IOException {
    return measureColumnChunkReader.readRawMeasureChunks(fileReader, blockIndexes);
  }

  /**
   * Below method will be used to read the measure chunk
   *
   * @param fileReader file read to read the file chunk
   * @param blockIndex block index to be read from file
   * @return measure data chunk
   */
  @Override public MeasureRawColumnChunk getMeasureChunk(FileHolder fileReader, int blockIndex)
      throws IOException {
    return measureColumnChunkReader.readRawMeasureChunk(fileReader, blockIndex);
  }

  /**
   * @return the number of pages in blocklet
   */
  @Override public int numberOfPages() {
    return numberOfPages;
  }

  @Override public int getPageRowCount(int pageNumber) {
    return this.pageRowCount[pageNumber];
  }
}
