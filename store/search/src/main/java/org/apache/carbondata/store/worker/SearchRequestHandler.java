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

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.DataMapChooser;
import org.apache.carbondata.core.datamap.DataMapLevel;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapper;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.readcommitter.LatestFilesReadCommittedScope;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.QueryModelBuilder;
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
      List<CarbonRow> rows = handleRequest(request);
      return createSuccessResponse(request, rows);
    } catch (IOException | InterruptedException e) {
      LOG.error(e);
      return createFailureResponse(request, e);
    }
  }

  public ShutdownResponse handleShutdown(ShutdownRequest request) {
    return new ShutdownResponse(Status.SUCCESS.ordinal(), "");
  }

  /**
   * Builds {@link QueryModel} and read data from files
   */
  private List<CarbonRow> handleRequest(SearchRequest request)
      throws IOException, InterruptedException {
    TableInfo tableInfo = request.tableInfo();
    CarbonTable table = CarbonTable.buildFromTableInfo(tableInfo);
    QueryModel queryModel = createQueryModel(table, request);

    // in search mode, plain reader is better since it requires less memory
    queryModel.setVectorReader(false);
    CarbonMultiBlockSplit mbSplit = request.split().value();
    long limit = request.limit();
    long rowCount = 0;

    // If there is FGDataMap, prune the split by applying FGDataMap
    queryModel = tryPruneByFGDataMap(table, queryModel, mbSplit);

    // In search mode, reader will read multiple blocks by using a thread pool
    CarbonRecordReader<CarbonRow> reader =
        new CarbonRecordReader<>(queryModel, new CarbonRowReadSupport());
    reader.initialize(mbSplit, null);

    // read all rows by the reader
    List<CarbonRow> rows = new LinkedList<>();
    try {
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
    return rows;
  }

  /**
   * If there is FGDataMap defined for this table and filter condition in the query,
   * prune the splits by the DataMap and set the pruned split into the QueryModel and return
   */
  private QueryModel tryPruneByFGDataMap(
      CarbonTable table, QueryModel queryModel, CarbonMultiBlockSplit mbSplit) throws IOException {
    DataMapExprWrapper wrapper =
        DataMapChooser.get().choose(table, queryModel.getFilterExpressionResolverTree());

    if (wrapper.getDataMapType() == DataMapLevel.FG) {
      List<Segment> segments = new LinkedList<>();
      for (CarbonInputSplit split : mbSplit.getAllSplits()) {
        segments.add(Segment.toSegment(
            split.getSegmentId(), new LatestFilesReadCommittedScope(table.getTablePath())));
      }
      List<ExtendedBlocklet> prunnedBlocklets = wrapper.prune(segments, null);

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
      queryModel.setTableBlockInfos(blockToRead);
    }
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
    return new SearchResult(request.queryId(), Status.FAILURE.ordinal(), throwable.getMessage(),
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
    return new SearchResult(request.queryId(), Status.SUCCESS.ordinal(), "", output);
  }

}
