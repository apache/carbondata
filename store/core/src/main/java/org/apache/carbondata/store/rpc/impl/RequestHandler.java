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

package org.apache.carbondata.store.rpc.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.scan.executor.impl.SearchModeDetailQueryExecutor;
import org.apache.carbondata.core.scan.executor.impl.SearchModeVectorDetailQueryExecutor;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.QueryModelBuilder;
import org.apache.carbondata.core.util.CarbonTaskInfo;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.CarbonRecordReader;
import org.apache.carbondata.store.rpc.model.QueryRequest;
import org.apache.carbondata.store.rpc.model.QueryResponse;
import org.apache.carbondata.store.rpc.model.ShutdownRequest;
import org.apache.carbondata.store.rpc.model.ShutdownResponse;

/**
 * It handles request from master.
 */
@InterfaceAudience.Internal
class RequestHandler {

  private static final LogService LOG =
      LogServiceFactory.getLogService(RequestHandler.class.getName());

  QueryResponse handleSearch(QueryRequest request) {
    try {
      LOG.info(String.format("[QueryId:%d] receive search request", request.getRequestId()));
      List<CarbonRow> rows = handleRequest(request);
      LOG.info(String.format("[QueryId:%d] sending success response", request.getRequestId()));
      return createSuccessResponse(request, rows);
    } catch (IOException e) {
      LOG.error(e);
      LOG.info(String.format("[QueryId:%d] sending failure response", request.getRequestId()));
      return createFailureResponse(request, e);
    }
  }

  ShutdownResponse handleShutdown(ShutdownRequest request) {
    LOG.info("Shutting down worker...");
    SearchModeDetailQueryExecutor.shutdownThreadPool();
    SearchModeVectorDetailQueryExecutor.shutdownThreadPool();
    LOG.info("Worker shut down");
    return new ShutdownResponse(Status.SUCCESS.ordinal(), "");
  }

  /**
   * Builds {@link QueryModel} and read data from files
   */
  private List<CarbonRow> handleRequest(QueryRequest request) throws IOException {
    CarbonTaskInfo carbonTaskInfo = new CarbonTaskInfo();
    carbonTaskInfo.setTaskId(System.nanoTime());
    ThreadLocalTaskInfo.setCarbonTaskInfo(carbonTaskInfo);
    CarbonMultiBlockSplit mbSplit = request.getSplit();
    long limit = request.getLimit();
    TableInfo tableInfo = request.getTableInfo();
    CarbonTable table = CarbonTable.buildFromTableInfo(tableInfo);
    QueryModel queryModel = createQueryModel(table, request);

    LOG.info(String.format("[QueryId:%d] %s, number of block: %d",
        request.getRequestId(), queryModel.toString(), mbSplit.getAllSplits().size()));

    // read all rows by the reader
    List<CarbonRow> rows = new LinkedList<>();
    try (CarbonRecordReader<CarbonRow> reader =
        new IndexedRecordReader(request.getRequestId(), table, queryModel)) {
      reader.initialize(mbSplit, null);

      // loop to read required number of rows.
      // By default, if user does not specify the limit value, limit is Long.MaxValue
      long rowCount = 0;
      while (reader.nextKeyValue() && rowCount < limit) {
        rows.add(reader.getCurrentValue());
        rowCount++;
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    LOG.info(String.format("[QueryId:%d] scan completed, return %d rows",
        request.getRequestId(), rows.size()));
    return rows;
  }



  private QueryModel createQueryModel(CarbonTable table, QueryRequest request) {
    String[] projectColumns = request.getProjectColumns();
    Expression filter = null;
    if (request.getFilterExpression() != null) {
      filter = request.getFilterExpression();
    }
    return new QueryModelBuilder(table)
        .projectColumns(projectColumns)
        .filterExpression(filter)
        .build();
  }

  /**
   * create a failure response
   */
  private QueryResponse createFailureResponse(QueryRequest request, Throwable throwable) {
    return new QueryResponse(request.getRequestId(), Status.FAILURE.ordinal(),
        throwable.getMessage(), new Object[0][]);
  }

  /**
   * create a success response with result rows
   */
  private QueryResponse createSuccessResponse(QueryRequest request, List<CarbonRow> rows) {
    Iterator<CarbonRow> itor = rows.iterator();
    Object[][] output = new Object[rows.size()][];
    int i = 0;
    while (itor.hasNext()) {
      output[i++] = itor.next().getData();
    }
    return new QueryResponse(request.getRequestId(), Status.SUCCESS.ordinal(), "", output);
  }

}
