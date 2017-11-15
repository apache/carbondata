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

import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datamap.dev.BlockletSerializer;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.reader.CarbonDataReaderFactory;
import org.apache.carbondata.core.datastore.chunk.reader.DimensionColumnChunkReader;
import org.apache.carbondata.core.datastore.chunk.reader.MeasureColumnChunkReader;
import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.core.indexstore.FineGrainBlocklet;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex;

/**
 * wrapper for blocklet data map data
 */
public class BlockletDataRefNode implements DataRefNode {

  private List<TableBlockInfo> blockInfos;

  private int index;

  private int[] dimensionLens;

  private BlockletSerializer blockletSerializer;

  BlockletDataRefNode(List<TableBlockInfo> blockInfos, int index, int[] dimensionLens) {
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
    this.blockletSerializer = new BlockletSerializer();
  }

  @Override public DataRefNode getNextDataRefNode() {
    if (index + 1 < blockInfos.size()) {
      return new BlockletDataRefNode(blockInfos, index + 1, dimensionLens);
    }
    return null;
  }

  @Override public int numRows() {
    return blockInfos.get(index).getDetailInfo().getRowCount();
  }

  @Override public long nodeIndex() {
    return index;
  }

  @Override public short blockletIndex() {
    return blockInfos.get(index).getDetailInfo().getBlockletId();
  }

  @Override
  public byte[][] getColumnsMaxValue() {
    BlockletIndex blockletIndex =
        blockInfos.get(index).getDetailInfo().getBlockletInfo().getBlockletIndex();
    // In case of blocklet distribution this will be null
    if (null != blockletIndex) {
      return blockletIndex.getMinMaxIndex().getMaxValues();
    }
    return null;
  }

  @Override
  public byte[][] getColumnsMinValue() {
    BlockletIndex blockletIndex =
        blockInfos.get(index).getDetailInfo().getBlockletInfo().getBlockletIndex();
    // In case of blocklet distribution this will be null
    if (null != blockletIndex) {
      return blockletIndex.getMinMaxIndex().getMinValues();
    }
    return null;
  }

  @Override
  public DimensionRawColumnChunk[] readDimensionChunks(FileReader fileReader, int[][] blockIndexes)
      throws IOException {
    DimensionColumnChunkReader dimensionChunksReader = getDimensionColumnChunkReader(fileReader);
    return dimensionChunksReader.readRawDimensionChunks(fileReader, blockIndexes);
  }

  @Override
  public DimensionRawColumnChunk readDimensionChunk(FileReader fileReader, int columnIndex)
      throws IOException {
    DimensionColumnChunkReader dimensionChunksReader = getDimensionColumnChunkReader(fileReader);
    return dimensionChunksReader.readRawDimensionChunk(fileReader, columnIndex);
  }

  @Override
  public MeasureRawColumnChunk[] readMeasureChunks(FileReader fileReader, int[][] columnIndexRange)
      throws IOException {
    MeasureColumnChunkReader measureColumnChunkReader = getMeasureColumnChunkReader(fileReader);
    return measureColumnChunkReader.readRawMeasureChunks(fileReader, columnIndexRange);
  }

  @Override public MeasureRawColumnChunk readMeasureChunk(FileReader fileReader, int columnIndex)
      throws IOException {
    MeasureColumnChunkReader measureColumnChunkReader = getMeasureColumnChunkReader(fileReader);
    return measureColumnChunkReader.readRawMeasureChunk(fileReader, columnIndex);
  }

  private DimensionColumnChunkReader getDimensionColumnChunkReader(FileReader fileReader) {
    ColumnarFormatVersion version =
        ColumnarFormatVersion.valueOf(blockInfos.get(index).getDetailInfo().getVersionNumber());
    if (fileReader.isReadPageByPage()) {
      return CarbonDataReaderFactory.getInstance().getDimensionColumnChunkReader(version,
          blockInfos.get(index).getDetailInfo().getBlockletInfo(), dimensionLens,
          blockInfos.get(index).getFilePath(), true);
    } else {
      return CarbonDataReaderFactory.getInstance().getDimensionColumnChunkReader(version,
          blockInfos.get(index).getDetailInfo().getBlockletInfo(), dimensionLens,
          blockInfos.get(index).getFilePath(), false);
    }
  }

  private MeasureColumnChunkReader getMeasureColumnChunkReader(FileReader fileReader) {
    ColumnarFormatVersion version =
        ColumnarFormatVersion.valueOf(blockInfos.get(index).getDetailInfo().getVersionNumber());
    if (fileReader.isReadPageByPage()) {
      return CarbonDataReaderFactory.getInstance().getMeasureColumnChunkReader(version,
          blockInfos.get(index).getDetailInfo().getBlockletInfo(),
          blockInfos.get(index).getFilePath(), true);
    } else {
      return CarbonDataReaderFactory.getInstance().getMeasureColumnChunkReader(version,
          blockInfos.get(index).getDetailInfo().getBlockletInfo(),
          blockInfos.get(index).getFilePath(), false);
    }
  }

  @Override
  public int numberOfPages() {
    return blockInfos.get(index).getDetailInfo().getPagesCount();
  }

  @Override
  public BitSetGroup getIndexedData() {
    String dataMapWriterPath = blockInfos.get(index).getDataMapWriterPath();
    if (dataMapWriterPath != null) {
      try {
        FineGrainBlocklet blocklet = blockletSerializer.deserializeBlocklet(dataMapWriterPath);
        return blocklet.getBitSetGroup(numberOfPages());
      } catch (IOException e) {
        return null;
      }
    } else {
      return null;
    }
  }

  @Override
  public int getPageRowCount(int pageNumber) {
    return blockInfos.get(index).getDetailInfo().getBlockletInfo()
        .getNumberOfRowsPerPage()[pageNumber];
  }

  public int numberOfNodes() {
    return blockInfos.size();
  }

  public List<TableBlockInfo> getBlockInfos() {
    return blockInfos;
  }
}
