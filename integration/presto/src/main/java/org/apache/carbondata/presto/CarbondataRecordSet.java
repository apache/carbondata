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

package org.apache.carbondata.presto;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.iterator.AbstractDetailQueryResultIterator;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.presto.impl.CarbonLocalInputSplit;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import org.apache.hadoop.mapred.TaskAttemptContext;

import static org.apache.carbondata.presto.Types.checkType;

public class CarbondataRecordSet implements RecordSet {

  private QueryModel queryModel;
  private CarbondataSplit split;
  private List<CarbondataColumnHandle> columns;
  private QueryExecutor queryExecutor;

  private CarbonDictionaryDecodeReadSupport readSupport;

  public CarbondataRecordSet(CarbonTable carbonTable, ConnectorSession session,
      ConnectorSplit split, List<CarbondataColumnHandle> columns, QueryModel queryModel,
      TaskAttemptContext taskAttemptContext) {
    this.split = checkType(split, CarbondataSplit.class, "connectorSplit");
    this.queryModel = queryModel;
    this.columns = columns;
    this.readSupport = new CarbonDictionaryDecodeReadSupport();
  }

  @Override public List<Type> getColumnTypes() {
    return columns.stream().map(a -> a.getColumnType()).collect(Collectors.toList());
  }

  /**
   * get data blocks via Carbondata QueryModel API.
   */
  @Override public RecordCursor cursor() {
    CarbonLocalInputSplit carbonLocalInputSplit = split.getLocalInputSplit();
    List<CarbonInputSplit> splitList = new ArrayList<>(1);
    splitList.add(CarbonLocalInputSplit.convertSplit(carbonLocalInputSplit));
    List<TableBlockInfo> tableBlockInfoList = CarbonInputSplit.createBlocks(splitList);
    queryModel.setTableBlockInfos(tableBlockInfoList);
    queryExecutor = QueryExecutorFactory.getQueryExecutor(queryModel);
    try {

      readSupport
          .initialize(queryModel.getProjectionColumns(), queryModel.getTable());
      CarbonIterator iterator = queryExecutor.execute(queryModel);
      PrestoCarbonVectorizedRecordReader vectorReader =
          new PrestoCarbonVectorizedRecordReader(queryExecutor, queryModel,
              (AbstractDetailQueryResultIterator) iterator);
      return new CarbondataRecordCursor(readSupport, vectorReader, columns, split);
    } catch (QueryExecutionException e) {
      throw new RuntimeException(e.getMessage(), e);
    } catch (Exception ex) {
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }

}
