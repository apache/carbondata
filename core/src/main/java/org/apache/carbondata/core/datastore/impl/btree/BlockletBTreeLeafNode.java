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
import org.apache.carbondata.core.datastore.FileReader;
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
   * reader for dimension chunk of page level
   */
  private DimensionColumnChunkReader dimensionChunksPageLevelReader;

  /**
   * reader of measure chunk of page level
   */
  private MeasureColumnChunkReader measureColumnChunkPageLevelReader;

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
  BlockletBTreeLeafNode(BTreeBuilderInfo builderInfos, int leafIndex, long nodeNumber) {
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
            builderInfos.getFooterList().get(0).getBlockInfo().getTableBlockInfo().getFilePath(),
            false);
    // create a instance of measure column chunk reader
    measureColumnChunkReader = CarbonDataReaderFactory.getInstance()
        .getMeasureColumnChunkReader(builderInfos.getFooterList().get(0).getVersionId(),
            builderInfos.getFooterList().get(0).getBlockletList().get(leafIndex),
            builderInfos.getFooterList().get(0).getBlockInfo().getTableBlockInfo().getFilePath(),
            false);
    // create a instance of dimension chunk
    dimensionChunksPageLevelReader = CarbonDataReaderFactory.getInstance()
        .getDimensionColumnChunkReader(builderInfos.getFooterList().get(0).getVersionId(),
            builderInfos.getFooterList().get(0).getBlockletList().get(leafIndex),
            builderInfos.getDimensionColumnValueSize(),
            builderInfos.getFooterList().get(0).getBlockInfo().getTableBlockInfo().getFilePath(),
            true);
    // create a instance of measure column chunk reader
    measureColumnChunkPageLevelReader = CarbonDataReaderFactory.getInstance()
        .getMeasureColumnChunkReader(builderInfos.getFooterList().get(0).getVersionId(),
            builderInfos.getFooterList().get(0).getBlockletList().get(leafIndex),
            builderInfos.getFooterList().get(0).getBlockInfo().getTableBlockInfo().getFilePath(),
            true);

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

  @Override public short blockletIndex() {
    return 0;
  }

  /**
   * Below method will be used to get the dimension chunks
   *
   * @param fileReader   file reader to read the chunks from file
   * @param columnIndexRange indexes of the blocks need to be read
   * @return dimension data chunks
   */
  @Override public DimensionRawColumnChunk[] readDimensionChunks(FileReader fileReader,
      int[][] columnIndexRange) throws IOException {
    if (fileReader.isReadPageByPage()) {
      return dimensionChunksPageLevelReader.readRawDimensionChunks(fileReader, columnIndexRange);
    } else {
      return dimensionChunksReader.readRawDimensionChunks(fileReader, columnIndexRange);
    }
  }

  /**
   * Below method will be used to get the dimension chunk
   *
   * @param fileReader file reader to read the chunk from file
   * @param columnIndex block index to be read
   * @return dimension data chunk
   */
  @Override public DimensionRawColumnChunk readDimensionChunk(
      FileReader fileReader, int columnIndex) throws IOException {
    if (fileReader.isReadPageByPage()) {
      return dimensionChunksPageLevelReader.readRawDimensionChunk(fileReader, columnIndex);
    } else {
      return dimensionChunksReader.readRawDimensionChunk(fileReader, columnIndex);
    }
  }

  /**
   * Below method will be used to get the measure chunk
   *
   * @param fileReader   file reader to read the chunk from file
   * @param columnIndexRange block indexes to be read from file
   * @return measure column data chunk
   */
  @Override public MeasureRawColumnChunk[] readMeasureChunks(FileReader fileReader,
      int[][] columnIndexRange) throws IOException {
    if (fileReader.isReadPageByPage()) {
      return measureColumnChunkPageLevelReader.readRawMeasureChunks(fileReader, columnIndexRange);
    } else {
      return measureColumnChunkReader.readRawMeasureChunks(fileReader, columnIndexRange);
    }
  }

  /**
   * Below method will be used to read the measure chunk
   *
   * @param fileReader file read to read the file chunk
   * @param columnIndex block index to be read from file
   * @return measure data chunk
   */
  @Override public MeasureRawColumnChunk readMeasureChunk(FileReader fileReader, int columnIndex)
      throws IOException {
    if (fileReader.isReadPageByPage()) {
      return measureColumnChunkPageLevelReader.readRawMeasureChunk(fileReader, columnIndex);
    } else {
      return measureColumnChunkReader.readRawMeasureChunk(fileReader, columnIndex);
    }
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
