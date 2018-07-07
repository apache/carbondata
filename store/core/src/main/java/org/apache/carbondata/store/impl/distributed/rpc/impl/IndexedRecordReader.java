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

package org.apache.carbondata.store.impl.distributed.rpc.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.datamap.DataMapChooser;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.expr.DataMapDistributableWrapper;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapper;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.readcommitter.TableStatusReadCommittedScope;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.iterator.ChunkRowIterator;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.CarbonRecordReader;
import org.apache.carbondata.hadoop.readsupport.impl.CarbonRowReadSupport;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This is a special RecordReader that leverages FGDataMap before reading carbondata file
 * and return CarbonRow object
 */
public class IndexedRecordReader extends CarbonRecordReader<CarbonRow> {

  private static final LogService LOG =
      LogServiceFactory.getLogService(RequestHandler.class.getName());

  private int queryId;
  private CarbonTable table;

  public IndexedRecordReader(int queryId, CarbonTable table, QueryModel queryModel) {
    super(queryModel, new CarbonRowReadSupport());
    this.queryId = queryId;
    this.table = table;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
    CarbonMultiBlockSplit mbSplit = (CarbonMultiBlockSplit) inputSplit;
    List<CarbonInputSplit> splits =  mbSplit.getAllSplits();
    List<TableBlockInfo> list = CarbonInputSplit.createBlocks(mbSplit.getAllSplits());
    queryModel.setTableBlockInfos(list);

    // prune the block with FGDataMap is there is one based on the filter condition
    DataMapExprWrapper fgDataMap = chooseFGDataMap(table,
        queryModel.getFilterExpressionResolverTree());
    if (fgDataMap != null) {
      queryModel = prune(table, queryModel, mbSplit, fgDataMap);
    } else {
      List<TableBlockInfo> tableBlockInfoList = CarbonInputSplit.createBlocks(splits);
      queryModel.setTableBlockInfos(tableBlockInfoList);
    }

    readSupport.initialize(queryModel.getProjectionColumns(), queryModel.getTable());
    try {
      carbonIterator = new ChunkRowIterator(queryExecutor.execute(queryModel));
    } catch (QueryExecutionException e) {
      throw new InterruptedException(e.getMessage());
    }
  }

  private DataMapExprWrapper chooseFGDataMap(
      CarbonTable table,
      FilterResolverIntf filterInterface) {
    DataMapChooser chooser = null;
    try {
      chooser = new DataMapChooser(table);
      return chooser.chooseFGDataMap(filterInterface);
    } catch (IOException e) {
      LOG.error(e);
      return null;
    }
  }

  /**
   * If there is FGDataMap defined for this table and filter condition in the query,
   * prune the splits by the DataMap and set the pruned split into the QueryModel and return
   */
  private QueryModel prune(CarbonTable table, QueryModel queryModel,
      CarbonMultiBlockSplit mbSplit, DataMapExprWrapper datamap) throws IOException {
    Objects.requireNonNull(datamap);
    List<Segment> segments = new LinkedList<>();
    HashMap<String, Integer> uniqueSegments = new HashMap<>();
    LoadMetadataDetails[] loadMetadataDetails =
        SegmentStatusManager.readLoadMetadata(
            CarbonTablePath.getMetadataPath(table.getTablePath()));
    for (CarbonInputSplit split : mbSplit.getAllSplits()) {
      String segmentId = Segment.getSegment(split.getSegmentId(), loadMetadataDetails).toString();
      if (uniqueSegments.get(segmentId) == null) {
        segments.add(Segment.toSegment(segmentId,
            new TableStatusReadCommittedScope(table.getAbsoluteTableIdentifier(),
                loadMetadataDetails)));
        uniqueSegments.put(segmentId, 1);
      } else {
        uniqueSegments.put(segmentId, uniqueSegments.get(segmentId) + 1);
      }
    }

    List<DataMapDistributableWrapper> distributables = datamap.toDistributable(segments);
    List<ExtendedBlocklet> prunnedBlocklets = new LinkedList<ExtendedBlocklet>();
    for (int i = 0; i < distributables.size(); i++) {
      DataMapDistributable dataMapDistributable = distributables.get(i).getDistributable();
      prunnedBlocklets.addAll(datamap.prune(dataMapDistributable, null));
    }

    HashMap<String, ExtendedBlocklet> pathToRead = new HashMap<>();
    for (ExtendedBlocklet prunedBlocklet : prunnedBlocklets) {
      pathToRead.put(prunedBlocklet.getFilePath(), prunedBlocklet);
    }

    List<TableBlockInfo> blocks = queryModel.getTableBlockInfos();
    List<TableBlockInfo> blockToRead = new LinkedList<>();
    for (TableBlockInfo block : blocks) {
      if (pathToRead.keySet().contains(block.getFilePath())) {
        // If not set this, it won't create FineGrainBlocklet object in
        // org.apache.carbondata.core.indexstore.blockletindex.BlockletDataRefNode.getIndexedData
        block.setDataMapWriterPath(pathToRead.get(block.getFilePath()).getDataMapWriterPath());
        blockToRead.add(block);
      }
    }
    LOG.info(String.format("[QueryId:%d] pruned using FG DataMap, pruned blocks: %d", queryId,
        blockToRead.size()));
    queryModel.setTableBlockInfos(blockToRead);
    return queryModel;
  }

  @Override public void close() throws IOException {
    logStatistics(rowCount, queryModel.getStatisticsRecorder());
    // clear dictionary cache
    Map<String, Dictionary> columnToDictionaryMapping = queryModel.getColumnToDictionaryMapping();
    if (null != columnToDictionaryMapping) {
      for (Map.Entry<String, Dictionary> entry : columnToDictionaryMapping.entrySet()) {
        CarbonUtil.clearDictionaryCache(entry.getValue());
      }
    }

    // close read support
    readSupport.close();
    try {
      queryExecutor.finish();
    } catch (QueryExecutionException e) {
      throw new IOException(e);
    }
  }
}
