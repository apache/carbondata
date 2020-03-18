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
import java.util.Optional;

import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datamap.dev.BlockletSerializer;
import org.apache.carbondata.core.datamap.dev.fgdatamap.FineGrainBlocklet;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.chunk.AbstractRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.reader.CarbonDataReaderFactory;
import org.apache.carbondata.core.datastore.chunk.reader.DimensionColumnChunkReader;
import org.apache.carbondata.core.datastore.chunk.reader.MeasureColumnChunkReader;
import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.core.indexstore.columncache.ColumnChunkCache;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.util.BitSetGroup;

/**
 * wrapper for blocklet data map data
 */
public class BlockletDataRefNode implements DataRefNode {

  private String tableId;

  private List<TableBlockInfo> blockInfos;

  private int index;

  private BlockletSerializer blockletSerializer;

  BlockletDataRefNode(List<TableBlockInfo> blockInfos, int index) {
    this.blockInfos = blockInfos;
    this.tableId = blockInfos.get(index).getTableId();
    // Update row count and page count to blocklet info
    for (TableBlockInfo blockInfo : blockInfos) {
      BlockletDetailInfo detailInfo = blockInfo.getDetailInfo();
      detailInfo.getBlockletInfo().setNumberOfRows(detailInfo.getRowCount());
      detailInfo.getBlockletInfo().setNumberOfPages(detailInfo.getPagesCount());
      detailInfo.setBlockletId(blockInfo.getDetailInfo().getBlockletId());
      int[] pageRowCount = new int[detailInfo.getPagesCount()];
      int numberOfPagesCompletelyFilled = detailInfo.getRowCount() /
          CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT;

      // no. of rows to a page is 120000 in V2 and 32000 in V3, same is handled to get the number
      // of pages filled
      int lastPageRowCount = detailInfo.getRowCount()
          % CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT;
      int fullyFilledRowsCount =
          CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT;
      for (int i = 0; i < numberOfPagesCompletelyFilled; i++) {
        pageRowCount[i] = fullyFilledRowsCount;
      }
      if (lastPageRowCount > 0) {
        pageRowCount[pageRowCount.length - 1] = lastPageRowCount;
      }
      // V3 old store to V3 new store compatibility. V3 new store will get this info in thrift.
      // so don't overwrite it with hardcoded values.
      if (detailInfo.getBlockletInfo().getNumberOfRowsPerPage() == null) {
        detailInfo.getBlockletInfo().setNumberOfRowsPerPage(pageRowCount);
      }
    }
    this.index = index;
    this.blockletSerializer = new BlockletSerializer();
  }

  @Override
  public DataRefNode getNextDataRefNode() {
    if (index + 1 < blockInfos.size()) {
      return new BlockletDataRefNode(blockInfos, index + 1);
    }
    return null;
  }

  @Override
  public int numRows() {
    return blockInfos.get(index).getDetailInfo().getRowCount();
  }

  @Override
  public short blockletIndex() {
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
  public boolean[] minMaxFlagArray() {
    BlockletIndex blockletIndex =
        blockInfos.get(index).getDetailInfo().getBlockletInfo().getBlockletIndex();
    boolean[] isMinMaxSet = null;
    if (null != blockletIndex) {
      isMinMaxSet = blockletIndex.getMinMaxIndex().getIsMinMaxSet();
    }
    return isMinMaxSet;
  }

  @Override
  public DimensionRawColumnChunk[] readDimensionChunks(FileReader fileReader, int[][] columnIndexes)
      throws IOException {
    TableBlockInfo blockInfo = blockInfos.get(index);
    if (ColumnChunkCache.isEnabledForTable(tableId)) {
      DimensionRawColumnChunk[] chunks = new DimensionRawColumnChunk[
          blockInfo.getDetailInfo().getBlockletInfo().getDimensionChunkOffsets().size()];
      for (int[] columnIndex : columnIndexes) {
        int columnIndexStart = columnIndex[0];
        int columnIndexEnd = columnIndex[1];
        for (int j = columnIndexStart; j <= columnIndexEnd; j++) {
          ColumnChunkCache.CacheKey key = new ColumnChunkCache.CacheKey(
              blockInfo.getFilePath(),
              blockInfo.getDetailInfo().getBlockletInfo().getDimensionChunkOffsets().get(j));
          Optional<AbstractRawColumnChunk> chunkOp = ColumnChunkCache.get(tableId, key);
          if (chunkOp.isPresent()) {
            chunks[j] = (DimensionRawColumnChunk) chunkOp.get();
          } else {
            chunks[j] = readDimensionChunkWithoutCache(fileReader, j);
            ColumnChunkCache.put(tableId, key, chunks[j]);
          }
        }
      }
      return chunks;
    } else {
      DimensionColumnChunkReader dimensionChunksReader = getDimensionColumnChunkReader(fileReader);
      return dimensionChunksReader.readRawDimensionChunks(fileReader, columnIndexes);
    }
  }

  @Override
  public DimensionRawColumnChunk readDimensionChunk(FileReader fileReader, int columnIndex)
      throws IOException {
    TableBlockInfo blockInfo = blockInfos.get(index);
    ColumnChunkCache.CacheKey key = new ColumnChunkCache.CacheKey(
        blockInfo.getFilePath(),
        blockInfo.getDetailInfo().getBlockletInfo().getDimensionChunkOffsets().get(columnIndex));
    Optional<AbstractRawColumnChunk> columnChunkOp = ColumnChunkCache.get(tableId, key);
    if (columnChunkOp.isPresent()) {
      return (DimensionRawColumnChunk) columnChunkOp.get();
    }
    DimensionRawColumnChunk chunk = readDimensionChunkWithoutCache(fileReader, columnIndex);
    ColumnChunkCache.put(tableId, key, chunk);
    return chunk;
  }

  private DimensionRawColumnChunk readDimensionChunkWithoutCache(
      FileReader fileReader, int columnIndex) throws IOException {
    DimensionColumnChunkReader reader = getDimensionColumnChunkReader(fileReader);
    return reader.readRawDimensionChunk(fileReader, columnIndex);
  }

  private MeasureRawColumnChunk readMeasureChunkWithoutCache(
      FileReader fileReader, int columnIndex) throws IOException {
    MeasureColumnChunkReader measureColumnChunkReader = getMeasureColumnChunkReader(fileReader);
    return measureColumnChunkReader.readRawMeasureChunk(fileReader, columnIndex);
  }

  @Override
  public MeasureRawColumnChunk[] readMeasureChunks(FileReader fileReader, int[][] columnIndexes)
      throws IOException {
    TableBlockInfo blockInfo = blockInfos.get(index);
    if (ColumnChunkCache.isEnabledForTable(tableId)) {
      MeasureRawColumnChunk[] chunks = new MeasureRawColumnChunk[
          blockInfo.getDetailInfo().getBlockletInfo().getMeasureChunkOffsets().size()];
      for (int[] columnIndex : columnIndexes) {
        int columnIndexStart = columnIndex[0];
        int columnIndexEnd = columnIndex[1];
        for (int j = columnIndexStart; j <= columnIndexEnd; j++) {
          ColumnChunkCache.CacheKey key = new ColumnChunkCache.CacheKey(
              blockInfo.getFilePath(),
              blockInfo.getDetailInfo().getBlockletInfo().getMeasureChunkOffsets().get(j));
          Optional<AbstractRawColumnChunk> chunkOp = ColumnChunkCache.get(tableId, key);
          if (chunkOp.isPresent()) {
            chunks[j] = (MeasureRawColumnChunk) chunkOp.get();
          } else {
            chunks[j] = readMeasureChunkWithoutCache(fileReader, j);
            ColumnChunkCache.put(tableId, key, chunks[j]);
          }
        }
      }
      return chunks;
    } else {
      MeasureColumnChunkReader measureColumnChunkReader = getMeasureColumnChunkReader(fileReader);
      return measureColumnChunkReader.readRawMeasureChunks(fileReader, columnIndexes);
    }
  }

  @Override
  public MeasureRawColumnChunk readMeasureChunk(FileReader fileReader, int columnIndex)
      throws IOException {
    TableBlockInfo blockInfo = blockInfos.get(index);
    ColumnChunkCache.CacheKey key = new ColumnChunkCache.CacheKey(
        blockInfo.getFilePath(),
        blockInfo.getDetailInfo().getBlockletInfo().getMeasureChunkOffsets().get(columnIndex));
    Optional<AbstractRawColumnChunk> columnChunkOp = ColumnChunkCache.get(tableId, key);
    if (columnChunkOp.isPresent()) {
      return (MeasureRawColumnChunk) columnChunkOp.get();
    }
    MeasureRawColumnChunk chunk = readMeasureChunkWithoutCache(fileReader, columnIndex);
    ColumnChunkCache.put(tableId, key, chunk);
    return chunk;
  }

  private DimensionColumnChunkReader getDimensionColumnChunkReader(FileReader fileReader) {
    ColumnarFormatVersion version =
        ColumnarFormatVersion.valueOf(blockInfos.get(index).getDetailInfo().getVersionNumber());
    if (fileReader.isReadPageByPage()) {
      return CarbonDataReaderFactory.getInstance().getDimensionColumnChunkReader(version,
          blockInfos.get(index).getDetailInfo().getBlockletInfo(),
          blockInfos.get(index).getFilePath(), true);
    } else {
      return CarbonDataReaderFactory.getInstance().getDimensionColumnChunkReader(version,
          blockInfos.get(index).getDetailInfo().getBlockletInfo(),
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
