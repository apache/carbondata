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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapper;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.readcommitter.LatestFilesReadCommittedScope;
import org.apache.carbondata.core.scan.executor.impl.SearchModeDetailQueryExecutor;
import org.apache.carbondata.core.scan.executor.impl.SearchModeVectorDetailQueryExecutor;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.QueryModelBuilder;
import org.apache.carbondata.core.util.CarbonTaskInfo;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;
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
    long limit = request.limit();
    long rowCount = 0;

    LOG.info(String
        .format("[SearchId:%d] %s, number of block: %d", request.searchId(), queryModel.toString(),
            mbSplit.getAllSplits().size()));

    // If there is DataMap selected in Master, prune the split by it
    if (request.dataMap() != null) {
      queryModel = prune(request.searchId(), table, queryModel, mbSplit, request.dataMap().get());
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
    LOG.info(String
        .format("[SearchId:%d] scan completed, return %d rows", request.searchId(), rows.size()));
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
    for (CarbonInputSplit split : mbSplit.getAllSplits()) {
      segments.add(
          Segment.toSegment(split.getSegmentId(),
              new LatestFilesReadCommittedScope(table.getTablePath())));
    }
    List<ExtendedBlocklet> prunnedBlocklets = datamap.prune(segments, null);

    List<String> pathToRead = new LinkedList<>();
    for (ExtendedBlocklet prunnedBlocklet : prunnedBlocklets) {
      pathToRead.add(prunnedBlocklet.getPath());
    }

    List<TableBlockInfo> blocks = queryModel.getTableBlockInfos();
    List<TableBlockInfo> blockToRead = new LinkedList<>();
    for (TableBlockInfo block : blocks) {
      if (pathToRead.contains(block.getFilePath())) {
        blockToRead.add(block);
      }
    }
    LOG.info(String.format("[SearchId:%d] pruned using FG DataMap, pruned blocks: %d", queryId,
        blockToRead.size()));
    queryModel.setTableBlockInfos(blockToRead);
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
