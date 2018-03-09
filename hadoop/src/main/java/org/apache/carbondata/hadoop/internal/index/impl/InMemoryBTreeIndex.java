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

package org.apache.carbondata.hadoop.internal.index.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.DataRefNodeFinder;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.datastore.SegmentTaskIndexStore;
import org.apache.carbondata.core.datastore.TableSegmentUniqueIdentifier;
import org.apache.carbondata.core.datastore.block.AbstractIndex;
import org.apache.carbondata.core.datastore.block.BlockletInfos;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.SegmentTaskIndexWrapper;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.apache.carbondata.core.datastore.impl.btree.BlockBTreeLeafNode;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.scan.filter.FilterExpressionProcessor;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.hadoop.CacheClient;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.internal.index.Block;
import org.apache.carbondata.hadoop.internal.index.Index;
import org.apache.carbondata.hadoop.internal.segment.Segment;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

class InMemoryBTreeIndex implements Index {

  private static final Log LOG = LogFactory.getLog(InMemoryBTreeIndex.class);
  private Segment segment;

  InMemoryBTreeIndex(Segment segment) {
    this.segment = segment;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public List<Block> filter(JobContext job, FilterResolverIntf filter)
      throws IOException {

    List<Block> result = new LinkedList<>();

    FilterExpressionProcessor filterExpressionProcessor = new FilterExpressionProcessor();

    AbsoluteTableIdentifier identifier = AbsoluteTableIdentifier.from(segment.getPath(), "", "");

    //for this segment fetch blocks matching filter in BTree
    List<DataRefNode> dataRefNodes =
        getDataBlocksOfSegment(job, filterExpressionProcessor, identifier, filter);
    for (DataRefNode dataRefNode : dataRefNodes) {
      BlockBTreeLeafNode leafNode = (BlockBTreeLeafNode) dataRefNode;
      TableBlockInfo tableBlockInfo = leafNode.getTableBlockInfo();
      result.add(new CarbonInputSplit(segment.getId(),
          tableBlockInfo.getDetailInfo().getBlockletId().toString(),
          new Path(tableBlockInfo.getFilePath()), tableBlockInfo.getBlockOffset(),
          tableBlockInfo.getBlockLength(), tableBlockInfo.getLocations(),
          tableBlockInfo.getBlockletInfos().getNoOfBlockLets(), tableBlockInfo.getVersion(), null));
    }
    return result;
  }

  private Map<SegmentTaskIndexStore.TaskBucketHolder, AbstractIndex> getSegmentAbstractIndexs(
      JobContext job, AbsoluteTableIdentifier identifier) throws IOException {
    Map<SegmentTaskIndexStore.TaskBucketHolder, AbstractIndex> segmentIndexMap = null;
    CacheClient cacheClient = new CacheClient();
    TableSegmentUniqueIdentifier segmentUniqueIdentifier =
        new TableSegmentUniqueIdentifier(identifier, segment.getId());
    try {
      SegmentTaskIndexWrapper segmentTaskIndexWrapper =
          cacheClient.getSegmentAccessClient().getIfPresent(segmentUniqueIdentifier);
      if (null != segmentTaskIndexWrapper) {
        segmentIndexMap = segmentTaskIndexWrapper.getTaskIdToTableSegmentMap();
      }
      // if segment tree is not loaded, load the segment tree
      if (segmentIndexMap == null) {
        List<TableBlockInfo> tableBlockInfoList = getTableBlockInfo(job);
        Map<String, List<TableBlockInfo>> segmentToTableBlocksInfos = new HashMap<>();
        segmentToTableBlocksInfos.put(segment.getId(), tableBlockInfoList);
        segmentUniqueIdentifier.setSegmentToTableBlocksInfos(segmentToTableBlocksInfos);
        // TODO: loadAndGetTaskIdToSegmentsMap can be optimized, use tableBlockInfoList as input
        // get Btree blocks for given segment
        segmentTaskIndexWrapper = cacheClient.getSegmentAccessClient().get(segmentUniqueIdentifier);
        segmentIndexMap = segmentTaskIndexWrapper.getTaskIdToTableSegmentMap();
      }
    } finally {
      cacheClient.close();
    }
    return segmentIndexMap;
  }

  /**
   * Below method will be used to get the table block info
   *
   * @param job                     job context
   * @return list of table block
   * @throws IOException
   */
  private List<TableBlockInfo> getTableBlockInfo(JobContext job) throws IOException {
    List<TableBlockInfo> tableBlockInfoList = new ArrayList<>();

    // identify table blocks from all file locations of given segment
    for (InputSplit inputSplit : segment.getAllSplits(job)) {
      CarbonInputSplit carbonInputSplit = (CarbonInputSplit) inputSplit;
      BlockletInfos blockletInfos = new BlockletInfos(carbonInputSplit.getNumberOfBlocklets(), 0,
          carbonInputSplit.getNumberOfBlocklets());
      tableBlockInfoList.add(
          new TableBlockInfo(carbonInputSplit.getPath().toString(),
              carbonInputSplit.getBlockletId(),carbonInputSplit.getStart(), segment.getId(),
              carbonInputSplit.getLocations(), carbonInputSplit.getLength(),
              blockletInfos, carbonInputSplit.getVersion(),
              carbonInputSplit.getDeleteDeltaFiles()));
    }
    return tableBlockInfoList;
  }

  /**
   * get data blocks of given segment
   */
  private List<DataRefNode> getDataBlocksOfSegment(JobContext job,
      FilterExpressionProcessor filterExpressionProcessor, AbsoluteTableIdentifier identifier,
      FilterResolverIntf resolver) throws IOException {

    QueryStatisticsRecorder recorder = CarbonTimeStatisticsFactory.createDriverRecorder();
    QueryStatistic statistic = new QueryStatistic();
    Map<SegmentTaskIndexStore.TaskBucketHolder, AbstractIndex> segmentIndexMap =
        getSegmentAbstractIndexs(job, identifier);

    List<DataRefNode> resultFilterredBlocks = new LinkedList<DataRefNode>();

    // build result
    for (AbstractIndex abstractIndex : segmentIndexMap.values()) {

      List<DataRefNode> filterredBlocks = null;
      // if no filter is given get all blocks from Btree Index
      if (null == resolver) {
        filterredBlocks = getDataBlocksOfIndex(abstractIndex);
      } else {
        // apply filter and get matching blocks
        filterredBlocks = filterExpressionProcessor.getFilterredBlocks(
            abstractIndex.getDataRefNode(),
            resolver,
            abstractIndex);
      }
      resultFilterredBlocks.addAll(filterredBlocks);
    }
    statistic.addStatistics(QueryStatisticsConstants.LOAD_BLOCKS_DRIVER,
        System.currentTimeMillis());
    recorder.recordStatistics(statistic);
    recorder.logStatistics();
    return resultFilterredBlocks;
  }

  /**
   * get data blocks of given btree
   */
  private List<DataRefNode> getDataBlocksOfIndex(AbstractIndex abstractIndex) {
    List<DataRefNode> blocks = new LinkedList<DataRefNode>();
    SegmentProperties segmentProperties = abstractIndex.getSegmentProperties();

    try {
      IndexKey startIndexKey = FilterUtil.prepareDefaultStartIndexKey(segmentProperties);
      IndexKey endIndexKey = FilterUtil.prepareDefaultEndIndexKey(segmentProperties);

      // Add all blocks of btree into result
      DataRefNodeFinder blockFinder =
          new BTreeDataRefNodeFinder(segmentProperties.getEachDimColumnValueSize(),
              segmentProperties.getNumberOfSortColumns(),
              segmentProperties.getNumberOfNoDictSortColumns());
      DataRefNode startBlock =
          blockFinder.findFirstDataBlock(abstractIndex.getDataRefNode(), startIndexKey);
      DataRefNode endBlock =
          blockFinder.findLastDataBlock(abstractIndex.getDataRefNode(), endIndexKey);
      while (startBlock != endBlock) {
        blocks.add(startBlock);
        startBlock = startBlock.getNextDataRefNode();
      }
      blocks.add(endBlock);

    } catch (KeyGenException e) {
      LOG.error("Could not generate start key", e);
    }
    return blocks;
  }

}
