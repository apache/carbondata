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

package org.apache.carbondata.store.worker;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.DataMapChooser;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.expr.DataMapDistributableWrapper;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapper;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.readcommitter.TableStatusReadCommittedScope;
import org.apache.carbondata.core.scan.executor.impl.SearchModeDetailQueryExecutor;
import org.apache.carbondata.core.scan.executor.impl.SearchModeVectorDetailQueryExecutor;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.QueryModelBuilder;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentManager;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentsHolder;
import org.apache.carbondata.core.util.CarbonTaskInfo;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.CarbonRecordReader;
import org.apache.carbondata.hadoop.readsupport.impl.CarbonRowReadSupport;

import org.apache.spark.search.SearchRequest;
import org.apache.spark.search.SearchResult;
import org.apache.spark.search.ShutdownRequest;
import org.apache.spark.search.ShutdownResponse;

/**
 * Thread runnable for handling SearchRequest from master.
 */
@InterfaceAudience.Internal
public class SearchRequestHandler {

  private static final LogService LOG =
      LogServiceFactory.getLogService(SearchRequestHandler.class.getName());

  public SearchResult handleSearch(SearchRequest request) {
    try {
      LOG.info(String.format("[SearchId:%d] receive search request", request.searchId()));
      List<CarbonRow> rows = handleRequest(request);
      LOG.info(String.format("[SearchId:%d] sending success response", request.searchId()));
      return createSuccessResponse(request, rows);
    } catch (IOException | InterruptedException e) {
      LOG.error(e);
      LOG.info(String.format("[SearchId:%d] sending failure response", request.searchId()));
      return createFailureResponse(request, e);
    }
  }

  public ShutdownResponse handleShutdown(ShutdownRequest request) {
    LOG.info("Shutting down worker...");
    SearchModeDetailQueryExecutor.shutdownThreadPool();
    SearchModeVectorDetailQueryExecutor.shutdownThreadPool();
    LOG.info("Worker shutted down");
    return new ShutdownResponse(Status.SUCCESS.ordinal(), "");
  }

  private DataMapExprWrapper chooseFGDataMap(
          CarbonTable table,
          FilterResolverIntf filterInterface) {
    DataMapChooser chooser = null;
    try {
      chooser = new DataMapChooser(table);
      return chooser.chooseFGDataMap(filterInterface);
    } catch (IOException e) {
      LOG.audit(e.getMessage());
      return null;
    }
  }

  /**
   * Builds {@link QueryModel} and read data from files
   */
  private List<CarbonRow> handleRequest(SearchRequest request)
      throws IOException, InterruptedException {
    CarbonTaskInfo carbonTaskInfo = new CarbonTaskInfo();
    carbonTaskInfo.setTaskId(System.nanoTime());
    ThreadLocalTaskInfo.setCarbonTaskInfo(carbonTaskInfo);
    TableInfo tableInfo = request.tableInfo();
    CarbonTable table = CarbonTable.buildFromTableInfo(tableInfo);
    QueryModel queryModel = createQueryModel(table, request);

    // in search mode, plain reader is better since it requires less memory
    queryModel.setVectorReader(false);

    CarbonMultiBlockSplit mbSplit = request.split().value();
    List<TableBlockInfo> list = CarbonInputSplit.createBlocks(mbSplit.getAllSplits());
    queryModel.setTableBlockInfos(list);
    long limit = request.limit();
    long rowCount = 0;

    LOG.info(String.format("[SearchId:%d] %s, number of block: %d",
        request.searchId(), queryModel.toString(), mbSplit.getAllSplits().size()));
    DataMapExprWrapper fgDataMap = chooseFGDataMap(table,
            queryModel.getFilterExpressionResolverTree());

    // If there is DataMap selected in Master, prune the split by it
    if (fgDataMap != null) {
      queryModel = prune(request.searchId(), table, queryModel, mbSplit, fgDataMap);
    }

    // In search mode, reader will read multiple blocks by using a thread pool
    CarbonRecordReader<CarbonRow> reader =
        new CarbonRecordReader<>(queryModel, new CarbonRowReadSupport());

    // read all rows by the reader
    List<CarbonRow> rows = new LinkedList<>();
    try {
      reader.initialize(mbSplit, null);

      // loop to read required number of rows.
      // By default, if user does not specify the limit value, limit is Long.MaxValue
      while (reader.nextKeyValue() && rowCount < limit) {
        rows.add(reader.getCurrentValue());
        rowCount++;
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      reader.close();
    }
    LOG.info(String.format("[SearchId:%d] scan completed, return %d rows",
        request.searchId(), rows.size()));
    return rows;
  }

  /**
   * If there is FGDataMap defined for this table and filter condition in the query,
   * prune the splits by the DataMap and set the pruned split into the QueryModel and return
   */
  private QueryModel prune(int queryId, CarbonTable table, QueryModel queryModel,
      CarbonMultiBlockSplit mbSplit, DataMapExprWrapper datamap) throws IOException {
    Objects.requireNonNull(datamap);
    List<Segment> segments = new LinkedList<>();
    HashMap<String, Integer> uniqueSegments = new HashMap<>();
    SegmentsHolder segmentsHolder =
        new SegmentManager().getAllSegments(table.getAbsoluteTableIdentifier());
    for (CarbonInputSplit split : mbSplit.getAllSplits()) {
      String segmentId =
          Segment.getSegment(split.getSegmentId(), segmentsHolder.getValidSegments()).toString();
      if (uniqueSegments.get(segmentId) == null) {
        segments.add(Segment.toSegment(segmentId,
            new TableStatusReadCommittedScope(table.getAbsoluteTableIdentifier(),
                segmentsHolder)));
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
      pathToRead.put(prunedBlocklet.getFilePath().replace('\\', '/'), prunedBlocklet);
    }

    List<TableBlockInfo> blocks = queryModel.getTableBlockInfos();
    List<TableBlockInfo> blockToRead = new LinkedList<>();
    for (TableBlockInfo block : blocks) {
      if (pathToRead.keySet().contains(block.getFilePath())) {
        // If not set this, it will can't create FineGrainBlocklet object in
        // org.apache.carbondata.core.indexstore.blockletindex.BlockletDataRefNode.getIndexedData
        block.setDataMapWriterPath(pathToRead.get(block.getFilePath()).getDataMapWriterPath());
        blockToRead.add(block);
      }
    }
    LOG.info(String.format("[SearchId:%d] pruned using FG DataMap, pruned blocks: %d", queryId,
        blockToRead.size()));
    queryModel.setTableBlockInfos(blockToRead);
    queryModel.setFG(true);
    return queryModel;
  }

  private QueryModel createQueryModel(CarbonTable table, SearchRequest request) {
    String[] projectColumns = request.projectColumns();
    Expression filter = null;
    if (request.filterExpression() != null) {
      filter = request.filterExpression();
    }
    return new QueryModelBuilder(table)
        .projectColumns(projectColumns)
        .filterExpression(filter)
        .build();
  }

  /**
   * create a failure response
   */
  private SearchResult createFailureResponse(SearchRequest request, Throwable throwable) {
    return new SearchResult(request.searchId(), Status.FAILURE.ordinal(), throwable.getMessage(),
        new Object[0][]);
  }

  /**
   * create a success response with result rows
   */
  private SearchResult createSuccessResponse(SearchRequest request, List<CarbonRow> rows) {
    Iterator<CarbonRow> itor = rows.iterator();
    Object[][] output = new Object[rows.size()][];
    int i = 0;
    while (itor.hasNext()) {
      output[i++] = itor.next().getData();
    }
    return new SearchResult(request.searchId(), Status.SUCCESS.ordinal(), "", output);
  }

}
