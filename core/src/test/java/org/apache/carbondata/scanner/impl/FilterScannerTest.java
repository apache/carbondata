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
package org.apache.carbondata.scanner.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.core.carbon.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.carbon.datastore.DataRefNode;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionChunkAttributes;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.ColumnGroupDimensionDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.apache.carbondata.core.carbon.datastore.impl.btree.BlockBTreeLeafNode;
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatistic;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatisticsConstants;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatisticsModel;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatisticsRecorder;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatisticsRecorderImpl;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.impl.DFSFileHolderImpl;
import org.apache.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.scan.filter.GenericQueryType;
import org.apache.carbondata.scan.filter.executer.AndFilterExecuterImpl;
import org.apache.carbondata.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.scan.model.QueryDimension;
import org.apache.carbondata.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.scan.result.AbstractScannedResult;
import org.apache.carbondata.scan.scanner.impl.FilterScanner;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class FilterScannerTest {

  private static FilterScanner filterScanner;
  private static BlockletIndex blockletIndex;
  private static BlockletMinMaxIndex blockletMinMaxIndex;
  private static BTreeBuilderInfo bTreeBuilderInfo;
  private static DataFileFooter dataFileFooter;

  @BeforeClass public static void setUp() {
    BlockExecutionInfo blockExecutionInfo = new BlockExecutionInfo();
    FilterExecuter filterExecutor = new AndFilterExecuterImpl(null, null);
    blockExecutionInfo.setFilterExecuterTree(filterExecutor);
    blockExecutionInfo.setFixedLengthKeySize(1);
    blockExecutionInfo.setNoDictionaryBlockIndexes(new int[] { 1, 2 });
    blockExecutionInfo.setDictionaryColumnBlockIndex(new int[] { 1 });
    blockExecutionInfo.setColumnGroupToKeyStructureInfo(new HashMap<Integer, KeyStructureInfo>());
    blockExecutionInfo.setComplexDimensionInfoMap(new HashMap<Integer, GenericQueryType>());
    blockExecutionInfo.setComplexColumnParentBlockIndexes(new int[] { 1 });
    blockExecutionInfo.setQueryDimensions(new QueryDimension[] { new QueryDimension("Col1") });
    blockExecutionInfo.setAllSelectedDimensionBlocksIndexes(new int[][] { { 0, 0 } });
    blockExecutionInfo.setAllSelectedMeasureBlocksIndexes(new int[][] { { 0, 0 } });
    blockExecutionInfo.setTotalNumberOfMeasureBlock(1);
    blockExecutionInfo.setTotalNumberDimensionBlock(1);
    QueryStatisticsModel queryStatisticsModel = new QueryStatisticsModel();
    QueryStatistic queryStatistic = new QueryStatistic();
    queryStatistic.addCountStatistic(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM, 1);
    Map<String, QueryStatistic> statisticsTypeAndObjMap = new HashMap<>();
    statisticsTypeAndObjMap.put(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM, queryStatistic);
    statisticsTypeAndObjMap.put(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM, queryStatistic);
    queryStatisticsModel.setStatisticsTypeAndObjMap(statisticsTypeAndObjMap);
    QueryStatisticsRecorder queryStatisticsRecorder = new QueryStatisticsRecorderImpl(System.nanoTime() + "");
    queryStatisticsModel.setRecorder(queryStatisticsRecorder);
    filterScanner = new FilterScanner(blockExecutionInfo, queryStatisticsModel);
    blockletIndex = new BlockletIndex();
    blockletMinMaxIndex = new BlockletMinMaxIndex();
    blockletMinMaxIndex.setMinValues(new byte[][] { { 1, 2 } });
    blockletMinMaxIndex.setMaxValues(new byte[][] { { 10, 12 } });
    blockletIndex.setMinMaxIndex(blockletMinMaxIndex);
    dataFileFooter = new DataFileFooter();
    dataFileFooter.setBlockletIndex(blockletIndex);
    bTreeBuilderInfo = new BTreeBuilderInfo(Arrays.asList(dataFileFooter), new int[] { 1 });
  }

  @Test public void testToScanBlockletWithEmptyBitSet()
      throws IOException, FilterUnsupportedException {
    new MockUp<AndFilterExecuterImpl>() {
      @SuppressWarnings("unused") @Mock
      public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
        return new BitSet();
      }
    };
    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(1, 1);
    DataRefNode dataRefNode = new BlockBTreeLeafNode(bTreeBuilderInfo, 0, 1);
    blocksChunkHolder.setDataBlock(dataRefNode);
    AbstractScannedResult abstractScannedResult = filterScanner.scanBlocklet(blocksChunkHolder);
    assertEquals(0, abstractScannedResult.numberOfOutputRows());
  }

  @Test public void testToScanBlockletWithNonEmptyBitSet()
      throws IOException, FilterUnsupportedException {
    new MockUp<AndFilterExecuterImpl>() {
      @SuppressWarnings("unused") @Mock
      public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
        BitSet bitSet = new BitSet();
        bitSet.set(1);
        bitSet.set(2);
        bitSet.set(1);
        return bitSet;
      }

      @SuppressWarnings("unused") @Mock
      public BitSet applyFilter(BlocksChunkHolder blockChunkHolder)
          throws FilterUnsupportedException {
        BitSet bitSet = new BitSet();
        bitSet.set(1);
        bitSet.set(2);
        bitSet.set(1);
        return bitSet;
      }
    };
    DataRefNode dataRefNode = new MockUp<DataRefNode>() {
      @Mock @SuppressWarnings("unused") DimensionColumnDataChunk[] getDimensionChunks(
          FileHolder fileReader, int[][] blockIndexes) {
        DimensionColumnDataChunk[] dimensionChunkAttributes =
            { new ColumnGroupDimensionDataChunk(null, null) };
        return dimensionChunkAttributes;
      }

      @Mock @SuppressWarnings("unused") MeasureColumnDataChunk[] getMeasureChunks(
          FileHolder fileReader, int[][] blockIndexes) {

        MeasureColumnDataChunk[] measureColumnDataChunks = { new MeasureColumnDataChunk() };
        return measureColumnDataChunks;
      }
    }.getMockInstance();

    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(1, 1);
    blocksChunkHolder.setDataBlock(dataRefNode);
    DimensionChunkAttributes dimensionChunkAttributes = new DimensionChunkAttributes();
    DimensionColumnDataChunk dimensionColumnDataChunk =
        new FixedLengthDimensionDataChunk(new byte[] { 0, 1 }, dimensionChunkAttributes);
    blocksChunkHolder.setDimensionDataChunk(new DimensionColumnDataChunk[]

        { dimensionColumnDataChunk });
    MeasureColumnDataChunk measureColumnDataChunk = new MeasureColumnDataChunk();
    blocksChunkHolder.setMeasureDataChunk(new MeasureColumnDataChunk[]

        { measureColumnDataChunk });
    FileHolder fileHolder = new DFSFileHolderImpl();
    blocksChunkHolder.setFileReader(fileHolder);
    AbstractScannedResult abstractScannedResult = filterScanner.scanBlocklet(blocksChunkHolder);

    assertEquals(2, abstractScannedResult.numberOfOutputRows());
  }

  @Test(expected = FilterUnsupportedException.class) public void testToScanBlockletWithException()
      throws IOException, FilterUnsupportedException {
    new MockUp<AndFilterExecuterImpl>() {
      @SuppressWarnings("unused") @Mock
      public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
        BitSet bitSet = new BitSet();
        bitSet.set(1);
        bitSet.set(2);
        bitSet.set(1);
        return bitSet;
      }

      @SuppressWarnings("unused") @Mock
      public BitSet applyFilter(BlocksChunkHolder blockChunkHolder)
          throws FilterUnsupportedException {
        throw new FilterUnsupportedException("Filter unsupported");
      }
    };
    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(1, 1);
    BTreeBuilderInfo bTreeBuilderInfo =
        new BTreeBuilderInfo(Arrays.asList(dataFileFooter), new int[] { 1 });
    DataRefNode dataRefNode = new BlockBTreeLeafNode(bTreeBuilderInfo, 0, 1);
    blocksChunkHolder.setDataBlock(dataRefNode);
    filterScanner.scanBlocklet(blocksChunkHolder);
  }

}
