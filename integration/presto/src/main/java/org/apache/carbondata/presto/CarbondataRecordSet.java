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
import org.apache.carbondata.core.datastore.block.BlockletInfos;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.BatchResult;
import org.apache.carbondata.core.scan.result.iterator.ChunkRowIterator;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import scala.Tuple3;

import static org.apache.carbondata.presto.Types.checkType;

//import org.apache.carbondata.hadoop.readsupport.impl.DictionaryDecodedReadSupportImpl;

public class CarbondataRecordSet implements RecordSet {

  private CarbonTable carbonTable;
  private TupleDomain<ColumnHandle> originalConstraint;
  private Expression carbonConstraint;
  private List<CarbondataColumnConstraint> rebuildConstraints;
  private QueryModel queryModel;
  private CarbondataSplit split;
  private List<CarbondataColumnHandle> columns;
  private QueryExecutor queryExecutor;

  private CarbonDictionaryDecodeReaderSupport readSupport;

  public CarbondataRecordSet(CarbonTable carbonTable, ConnectorSession session,
      ConnectorSplit split, List<CarbondataColumnHandle> columns, QueryModel queryModel) {
    this.carbonTable = carbonTable;
    this.split = checkType(split, CarbondataSplit.class, "connectorSplit");
    this.originalConstraint = this.split.getConstraints();
    this.rebuildConstraints = this.split.getRebuildConstraints();
    this.queryModel = queryModel;
    this.columns = columns;
    this.readSupport = new CarbonDictionaryDecodeReaderSupport();
  }

  //todo support later
  private Expression parseConstraint2Expression(TupleDomain<ColumnHandle> constraints) {
    return null;
  }

  @Override public List<Type> getColumnTypes() {
    return columns.stream().map(a -> a.getColumnType()).collect(Collectors.toList());
  }

  /**
   * get data blocks via Carbondata QueryModel API
   */
  @Override public RecordCursor cursor() {
    List<TableBlockInfo> tableBlockInfoList = new ArrayList<TableBlockInfo>();

    tableBlockInfoList.add(new TableBlockInfo(split.getLocalInputSplit().getPath().toString(),
        split.getLocalInputSplit().getStart(), split.getLocalInputSplit().getSegmentId(),
        split.getLocalInputSplit().getLocations().toArray(new String[0]),
        split.getLocalInputSplit().getLength(), new BlockletInfos(),
        //blockletInfos,
        ColumnarFormatVersion.valueOf(split.getLocalInputSplit().getVersion()), null));
    queryModel.setTableBlockInfos(tableBlockInfoList);

    queryExecutor = QueryExecutorFactory.getQueryExecutor(queryModel);

    try {

      Tuple3[] dict = readSupport
          .initialize(queryModel.getProjectionColumns(), queryModel.getAbsoluteTableIdentifier());
      CarbonIterator<Object[]> carbonIterator =
          new ChunkRowIterator((CarbonIterator<BatchResult>) queryExecutor.execute(queryModel));
      RecordCursor rc =
          new CarbondataRecordCursor(readSupport, carbonIterator, columns, split, dict);
      return rc;
    } catch (QueryExecutionException e) {
      throw new RuntimeException(e.getMessage(), e);
    } catch (Exception ex) {
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }
}

