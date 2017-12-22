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
package org.apache.carbondata.core.indexstore.blockletindex;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.cache.update.BlockletLevelDeleteDeltaDataCache;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.reader.CarbonDataReaderFactory;
import org.apache.carbondata.core.datastore.chunk.reader.DimensionColumnChunkReader;
import org.apache.carbondata.core.datastore.chunk.reader.MeasureColumnChunkReader;
import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;

/**
 * wrapper for blocklet data map data
 */
public class BlockletDataRefNodeWrapper implements DataRefNode {

  private List<TableBlockInfo> blockInfos;

  private int index;

  private int[] dimensionLens;

  private BlockletLevelDeleteDeltaDataCache deleteDeltaDataCache;

  public BlockletDataRefNodeWrapper(List<TableBlockInfo> blockInfos, int index,
      int[] dimensionLens) {
    this.blockInfos = blockInfos;
    // Update row count and page count to blocklet info
    for (TableBlockInfo blockInfo : blockInfos) {
      BlockletDetailInfo detailInfo = blockInfo.getDetailInfo();
      detailInfo.getBlockletInfo().setNumberOfRows(detailInfo.getRowCount());
      detailInfo.getBlockletInfo().setNumberOfPages(detailInfo.getPagesCount());
      detailInfo.setBlockletId(blockInfo.getDetailInfo().getBlockletId());
      int[] pageRowCount = new int[detailInfo.getPagesCount()];
      int numberOfPagesCompletelyFilled = detailInfo.getRowCount()
          / CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT;
      int lastPageRowCount = detailInfo.getRowCount()
          % CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT;
      for (int i = 0; i < numberOfPagesCompletelyFilled; i++) {
        pageRowCount[i] =
            CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT;
      }
      if (lastPageRowCount > 0) {
        pageRowCount[pageRowCount.length - 1] = lastPageRowCount;
      }
      detailInfo.getBlockletInfo().setNumberOfRowsPerPage(pageRowCount);
    }
    this.index = index;
    this.dimensionLens = dimensionLens;
  }

  @Override public DataRefNode getNextDataRefNode() {
    if (index + 1 < blockInfos.size()) {
      return new BlockletDataRefNodeWrapper(blockInfos, index + 1, dimensionLens);
    }
    return null;
  }

  @Override public int nodeSize() {
    return blockInfos.get(index).getDetailInfo().getRowCount();
  }

  @Override public long nodeNumber() {
    return index;
  }

  @Override public String blockletId() {
    return blockInfos.get(index).getDetailInfo().getBlockletId().toString();
  }

  @Override public byte[][] getColumnsMaxValue() {
    return null;
  }

  @Override public byte[][] getColumnsMinValue() {
    return null;
  }

  @Override
  public DimensionRawColumnChunk[] getDimensionChunks(FileHolder fileReader, int[][] blockIndexes)
      throws IOException {
    DimensionColumnChunkReader dimensionChunksReader = getDimensionColumnChunkReader();
    return dimensionChunksReader.readRawDimensionChunks(fileReader, blockIndexes);
  }

  @Override
  public DimensionRawColumnChunk getDimensionChunk(FileHolder fileReader, int blockIndexes)
      throws IOException {
    DimensionColumnChunkReader dimensionChunksReader = getDimensionColumnChunkReader();
    return dimensionChunksReader.readRawDimensionChunk(fileReader, blockIndexes);
  }

  @Override
  public MeasureRawColumnChunk[] getMeasureChunks(FileHolder fileReader, int[][] blockIndexes)
      throws IOException {
    MeasureColumnChunkReader measureColumnChunkReader = getMeasureColumnChunkReader();
    return measureColumnChunkReader.readRawMeasureChunks(fileReader, blockIndexes);
  }

  @Override public MeasureRawColumnChunk getMeasureChunk(FileHolder fileReader, int blockIndex)
      throws IOException {
    MeasureColumnChunkReader measureColumnChunkReader = getMeasureColumnChunkReader();
    return measureColumnChunkReader.readRawMeasureChunk(fileReader, blockIndex);
  }

  private DimensionColumnChunkReader getDimensionColumnChunkReader() throws IOException {
    ColumnarFormatVersion version =
        ColumnarFormatVersion.valueOf(blockInfos.get(index).getDetailInfo().getVersionNumber());
    DimensionColumnChunkReader dimensionColumnChunkReader = CarbonDataReaderFactory.getInstance()
        .getDimensionColumnChunkReader(version,
            blockInfos.get(index).getDetailInfo().getBlockletInfo(), dimensionLens,
            blockInfos.get(index).getFilePath());
    return dimensionColumnChunkReader;
  }

  private MeasureColumnChunkReader getMeasureColumnChunkReader() throws IOException {
    ColumnarFormatVersion version =
        ColumnarFormatVersion.valueOf(blockInfos.get(index).getDetailInfo().getVersionNumber());
    return CarbonDataReaderFactory.getInstance().getMeasureColumnChunkReader(version,
        blockInfos.get(index).getDetailInfo().getBlockletInfo(),
        blockInfos.get(index).getFilePath());
  }

  @Override
  public void setDeleteDeltaDataCache(BlockletLevelDeleteDeltaDataCache deleteDeltaDataCache) {
    this.deleteDeltaDataCache = deleteDeltaDataCache;
  }

  @Override public BlockletLevelDeleteDeltaDataCache getDeleteDeltaDataCache() {
    return deleteDeltaDataCache;
  }

  @Override public int numberOfPages() {
    return blockInfos.get(index).getDetailInfo().getPagesCount();
  }

  @Override public int getPageRowCount(int pageNumber) {
    return blockInfos.get(index).getDetailInfo().getBlockletInfo()
        .getNumberOfRowsPerPage()[pageNumber];
  }

  public int numberOfNodes() {
    return blockInfos.size();
  }
}
