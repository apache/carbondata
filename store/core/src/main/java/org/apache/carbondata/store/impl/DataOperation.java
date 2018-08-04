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

package org.apache.carbondata.store.impl;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.QueryModelBuilder;
import org.apache.carbondata.core.util.CarbonTaskInfo;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.CarbonRecordReader;
import org.apache.carbondata.store.impl.service.model.ScanRequest;

public class DataOperation {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataOperation.class.getCanonicalName());

  /**
   * Scan data and return matched rows. This should be invoked in worker side.
   * @param tableInfo carbon table
   * @param scan scan parameter
   * @return matched rows
   * @throws IOException if IO error occurs
   */
  public static List<CarbonRow> scan(TableInfo tableInfo, ScanRequest scan) throws IOException {
    CarbonTable table = CarbonTable.buildFromTableInfo(tableInfo);
    CarbonTaskInfo carbonTaskInfo = new CarbonTaskInfo();
    carbonTaskInfo.setTaskId(System.nanoTime());
    ThreadLocalTaskInfo.setCarbonTaskInfo(carbonTaskInfo);

    CarbonMultiBlockSplit mbSplit = scan.getSplit();
    long limit = scan.getLimit();
    QueryModel queryModel = createQueryModel(table, scan);

    LOGGER.info(String.format("[QueryId:%d] %s, number of block: %d", scan.getRequestId(),
        queryModel.toString(), mbSplit.getAllSplits().size()));

    // read all rows by the reader
    List<CarbonRow> rows = new LinkedList<>();
    try (CarbonRecordReader<CarbonRow> reader = new IndexedRecordReader(scan.getRequestId(),
        table, queryModel)) {
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
    LOGGER.info(String.format("[QueryId:%d] scan completed, return %d rows",
        scan.getRequestId(), rows.size()));
    return rows;
  }

  private static QueryModel createQueryModel(CarbonTable table, ScanRequest scan) {
    String[] projectColumns = scan.getProjectColumns();
    Expression filter = null;
    if (scan.getFilterExpression() != null) {
      filter = scan.getFilterExpression();
    }
    return new QueryModelBuilder(table)
        .projectColumns(projectColumns)
        .filterExpression(filter)
        .build();
  }
}
