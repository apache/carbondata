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

import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.iterator.AbstractDetailQueryResultIterator;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.CarbonProjection;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.presto.impl.CarbonLocalMultiBlockSplit;
import org.apache.carbondata.presto.impl.CarbonTableCacheModel;
import org.apache.carbondata.presto.impl.CarbonTableReader;

import static org.apache.carbondata.presto.Types.checkType;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;


/**
 * Provider Class for Carbondata Page Source class.
 */
public class CarbondataPageSourceProvider implements ConnectorPageSourceProvider {

  private String connectorId;
  private CarbonTableReader carbonTableReader;
  private String queryId ;

  @Inject public CarbondataPageSourceProvider(CarbondataConnectorId connectorId,
      CarbonTableReader carbonTableReader) {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    this.carbonTableReader = requireNonNull(carbonTableReader, "carbonTableReader is null");
  }

  @Override
  public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns) {
    this.queryId = ((CarbondataSplit)split).getQueryId();
    CarbonDictionaryDecodeReadSupport readSupport = new CarbonDictionaryDecodeReadSupport();
    PrestoCarbonVectorizedRecordReader carbonRecordReader =
        createReader(split, columns, readSupport);
    return new CarbondataPageSource(carbonRecordReader, columns);
  }

  /**
   * @param split
   * @param columns
   * @param readSupport
   * @return
   */
  private PrestoCarbonVectorizedRecordReader createReader(ConnectorSplit split,
      List<? extends ColumnHandle> columns, CarbonDictionaryDecodeReadSupport readSupport) {

    CarbondataSplit carbondataSplit =
        checkType(split, CarbondataSplit.class, "split is not class CarbondataSplit");
    checkArgument(carbondataSplit.getConnectorId().equals(connectorId),
        "split is not for this connector");
    QueryModel queryModel = createQueryModel(carbondataSplit, columns);
    QueryExecutor queryExecutor =
        QueryExecutorFactory.getQueryExecutor(queryModel, new Configuration());
    try {
      CarbonIterator iterator = queryExecutor.execute(queryModel);
      readSupport.initialize(queryModel.getProjectionColumns(), queryModel.getTable());
      PrestoCarbonVectorizedRecordReader reader =
          new PrestoCarbonVectorizedRecordReader(queryExecutor, queryModel,
              (AbstractDetailQueryResultIterator) iterator, readSupport);
      reader.setTaskId(carbondataSplit.getIndex());
      return reader;
    } catch (IOException e) {
      throw new RuntimeException("Unable to get the Query Model ", e);
    } catch (QueryExecutionException e) {
      throw new RuntimeException(e.getMessage(), e);
    } catch (Exception ex) {
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }

  /**
   * @param carbondataSplit
   * @param columns
   * @return
   */
  private QueryModel createQueryModel(CarbondataSplit carbondataSplit,
      List<? extends ColumnHandle> columns) {

    try {
      CarbonProjection carbonProjection = getCarbonProjection(columns);
      CarbonTable carbonTable = getCarbonTable(carbondataSplit);

      Configuration conf = new Configuration();
      conf.set(CarbonTableInputFormat.INPUT_SEGMENT_NUMBERS, "");
      String carbonTablePath = carbonTable.getAbsoluteTableIdentifier().getTablePath();

      conf.set(CarbonTableInputFormat.INPUT_DIR, carbonTablePath);
      conf.set("query.id", queryId);
      JobConf jobConf = new JobConf(conf);
      CarbonTableInputFormat carbonTableInputFormat = createInputFormat(jobConf, carbonTable,
          PrestoFilterUtil.parseFilterExpression(carbondataSplit.getConstraints()),
          carbonProjection);
      TaskAttemptContextImpl hadoopAttemptContext =
          new TaskAttemptContextImpl(jobConf, new TaskAttemptID("", 1, TaskType.MAP, 0, 0));
      CarbonMultiBlockSplit carbonInputSplit =
          CarbonLocalMultiBlockSplit.convertSplit(carbondataSplit.getLocalInputSplit());
      QueryModel queryModel =
          carbonTableInputFormat.createQueryModel(carbonInputSplit, hadoopAttemptContext);
      queryModel.setQueryId(queryId);
      queryModel.setVectorReader(true);
      queryModel.setStatisticsRecorder(
          CarbonTimeStatisticsFactory.createExecutorRecorder(queryModel.getQueryId()));

      List<TableBlockInfo> tableBlockInfoList =
          CarbonInputSplit.createBlocks(carbonInputSplit.getAllSplits());
      queryModel.setTableBlockInfos(tableBlockInfoList);
      return queryModel;
    } catch (IOException e) {
      throw new RuntimeException("Unable to get the Query Model ", e);
    }
  }

  /**
   * @param conf
   * @param carbonTable
   * @param filterExpression
   * @param projection
   * @return
   */
  private CarbonTableInputFormat<Object> createInputFormat(Configuration conf,
      CarbonTable carbonTable, Expression filterExpression, CarbonProjection projection) {

    AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();
    CarbonTableInputFormat format = new CarbonTableInputFormat<Object>();
    try {
      CarbonTableInputFormat
          .setTablePath(conf, identifier.appendWithLocalPrefix(identifier.getTablePath()));
      CarbonTableInputFormat
          .setDatabaseName(conf, identifier.getCarbonTableIdentifier().getDatabaseName());
      CarbonTableInputFormat
          .setTableName(conf, identifier.getCarbonTableIdentifier().getTableName());
    } catch (Exception e) {
      throw new RuntimeException("Unable to create the CarbonTableInputFormat", e);
    }
    CarbonTableInputFormat.setFilterPredicates(conf, filterExpression);
    CarbonTableInputFormat.setColumnProjection(conf, projection);

    return format;
  }

  /**
   * @param columns
   * @return
   */
  private CarbonProjection getCarbonProjection(List<? extends ColumnHandle> columns) {
    CarbonProjection carbonProjection = new CarbonProjection();
    // Convert all columns handles
    ImmutableList.Builder<CarbondataColumnHandle> handles = ImmutableList.builder();
    for (ColumnHandle handle : columns) {
      handles.add(checkType(handle, CarbondataColumnHandle.class, "handle"));
      carbonProjection.addColumn(((CarbondataColumnHandle) handle).getColumnName());
    }
    return carbonProjection;
  }

  /**
   * @param carbonSplit
   * @return
   */
  private CarbonTable getCarbonTable(CarbondataSplit carbonSplit) {
    CarbonTableCacheModel tableCacheModel =
        carbonTableReader.getCarbonCache(carbonSplit.getSchemaTableName());
    checkNotNull(tableCacheModel, "tableCacheModel should not be null");
    checkNotNull(tableCacheModel.carbonTable, "tableCacheModel.carbonTable should not be null");
    checkNotNull(tableCacheModel.carbonTable.getTableInfo(),
        "tableCacheModel.carbonTable.tableInfo should not be null");
    return tableCacheModel.carbonTable;
  }

}
