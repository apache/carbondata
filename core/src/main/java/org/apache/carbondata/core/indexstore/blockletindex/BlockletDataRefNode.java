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
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.reader.CarbonDataReaderFactory;
import org.apache.carbondata.core.datastore.chunk.reader.DimensionColumnChunkReader;
import org.apache.carbondata.core.datastore.chunk.reader.MeasureColumnChunkReader;
import org.apache.carbondata.core.index.dev.BlockletSerializer;
import org.apache.carbondata.core.index.dev.fgindex.FineGrainBlocklet;
import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.util.BitSetGroup;

/**
 * wrapper for blocklet index data
 */
public class BlockletDataRefNode implements DataRefNode {

  private List<TableBlockInfo> blockInfos;

  private int index;

  private BlockletSerializer blockletSerializer;

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
  BlockletDataRefNode(List<TableBlockInfo> blockInfos, int index) {
    this.blockInfos = blockInfos;
    // Update row count and page count to blocklet info
    for (TableBlockInfo blockInfo : blockInfos) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1537
      BlockletDetailInfo detailInfo = blockInfo.getDetailInfo();
      detailInfo.getBlockletInfo().setNumberOfRows(detailInfo.getRowCount());
      detailInfo.getBlockletInfo().setNumberOfPages(detailInfo.getPagesCount());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1731
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1728
      detailInfo.setBlockletId(blockInfo.getDetailInfo().getBlockletId());
      int[] pageRowCount = new int[detailInfo.getPagesCount()];
      int numberOfPagesCompletelyFilled = detailInfo.getRowCount() /
          CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3650

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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3001
      if (detailInfo.getBlockletInfo().getNumberOfRowsPerPage() == null) {
        detailInfo.getBlockletInfo().setNumberOfRowsPerPage(pageRowCount);
      }
    }
    this.index = index;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1544
    this.blockletSerializer = new BlockletSerializer();
  }

  @Override
  public DataRefNode getNextDataRefNode() {
    if (index + 1 < blockInfos.size()) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2020
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2942
    BlockletIndex blockletIndex =
        blockInfos.get(index).getDetailInfo().getBlockletInfo().getBlockletIndex();
    boolean[] isMinMaxSet = null;
    if (null != blockletIndex) {
      isMinMaxSet = blockletIndex.getMinMaxIndex().getIsMinMaxSet();
    }
    return isMinMaxSet;
  }

  @Override
  public DimensionRawColumnChunk[] readDimensionChunks(FileReader fileReader, int[][] blockIndexes)
      throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1224
    DimensionColumnChunkReader dimensionChunksReader = getDimensionColumnChunkReader(fileReader);
    return dimensionChunksReader.readRawDimensionChunks(fileReader, blockIndexes);
  }

  @Override
  public DimensionRawColumnChunk readDimensionChunk(FileReader fileReader, int columnIndex)
      throws IOException {
    DimensionColumnChunkReader dimensionChunksReader = getDimensionColumnChunkReader(fileReader);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2099
    return dimensionChunksReader.readRawDimensionChunk(fileReader, columnIndex);
  }

  @Override
  public MeasureRawColumnChunk[] readMeasureChunks(FileReader fileReader, int[][] columnIndexRange)
      throws IOException {
    MeasureColumnChunkReader measureColumnChunkReader = getMeasureColumnChunkReader(fileReader);
    MeasureRawColumnChunk[] measureRawColumnChunks =
        measureColumnChunkReader.readRawMeasureChunks(fileReader, columnIndexRange);
    return measureRawColumnChunks;
  }

  @Override
  public MeasureRawColumnChunk readMeasureChunk(FileReader fileReader, int columnIndex)
      throws IOException {
    MeasureColumnChunkReader measureColumnChunkReader = getMeasureColumnChunkReader(fileReader);
    MeasureRawColumnChunk measureRawColumnChunk =
        measureColumnChunkReader.readRawMeasureChunk(fileReader, columnIndex);
    return measureRawColumnChunk;
  }

  private DimensionColumnChunkReader getDimensionColumnChunkReader(FileReader fileReader) {
    ColumnarFormatVersion version =
        ColumnarFormatVersion.valueOf(blockInfos.get(index).getDetailInfo().getVersionNumber());
    if (fileReader.isReadPageByPage()) {
      return CarbonDataReaderFactory.getInstance().getDimensionColumnChunkReader(version,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    String indexWriterPath = blockInfos.get(index).getIndexWriterPath();
    if (indexWriterPath != null) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1544
      try {
        FineGrainBlocklet blocklet = blockletSerializer.deserializeBlocklet(indexWriterPath);
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1854
    return blockInfos.get(index).getDetailInfo().getBlockletInfo()
        .getNumberOfRowsPerPage()[pageNumber];
  }

  public int numberOfNodes() {
    return blockInfos.size();
  }

  public List<TableBlockInfo> getBlockInfos() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1934
    return blockInfos;
  }
}
