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
import java.util.Set;

import static java.util.Objects.requireNonNull;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
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

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePageSourceFactory;
import com.facebook.presto.hive.HivePageSourceProvider;
import com.facebook.presto.hive.HiveRecordCursorProvider;
import com.facebook.presto.hive.HiveSplit;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provider Class for Carbondata Page Source class.
 */
public class CarbondataPageSourceProvider extends HivePageSourceProvider {

  private CarbonTableReader carbonTableReader;
  private String queryId ;
  private HdfsEnvironment hdfsEnvironment;

  @Inject public CarbondataPageSourceProvider(
      HiveClientConfig hiveClientConfig,
      HdfsEnvironment hdfsEnvironment,
      Set<HiveRecordCursorProvider> cursorProviders,
      Set<HivePageSourceFactory> pageSourceFactories,
      TypeManager typeManager,
      CarbonTableReader carbonTableReader) {
    super(hiveClientConfig, hdfsEnvironment, cursorProviders, pageSourceFactories, typeManager);
    this.carbonTableReader = requireNonNull(carbonTableReader, "carbonTableReader is null");
    this.hdfsEnvironment = hdfsEnvironment;
  }

  @Override
  public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns) {
    HiveSplit carbonSplit =
        checkType(split, HiveSplit.class, "split is not class HiveSplit");
    this.queryId = carbonSplit.getSchema().getProperty("queryId");
    if (this.queryId == null) {
      // Fall back to hive pagesource.
      return super.createPageSource(transactionHandle, session, split, columns);
    }
    Configuration configuration = this.hdfsEnvironment.getConfiguration(
        new HdfsEnvironment.HdfsContext(session, carbonSplit.getDatabase(), carbonSplit.getTable()),
        new Path(carbonSplit.getSchema().getProperty("tablePath")));
    configuration = carbonTableReader.updateS3Properties(configuration);
    CarbonDictionaryDecodeReadSupport readSupport = new CarbonDictionaryDecodeReadSupport();
    PrestoCarbonVectorizedRecordReader carbonRecordReader =
        createReader(carbonSplit, columns, readSupport, configuration);
    return new CarbondataPageSource(carbonRecordReader, columns);
  }

  /**
   * Create vector reader using the split.
   */
  private PrestoCarbonVectorizedRecordReader createReader(HiveSplit carbonSplit,
      List<? extends ColumnHandle> columns, CarbonDictionaryDecodeReadSupport readSupport,
      Configuration conf) {
    QueryModel queryModel = createQueryModel(carbonSplit, columns, conf);
    if (carbonTableReader.config.getPushRowFilter() == null ||
        carbonTableReader.config.getPushRowFilter().equalsIgnoreCase("false")) {
      queryModel.setDirectVectorFill(true);
      queryModel.setPreFetchData(false);
    }
    QueryExecutor queryExecutor =
        QueryExecutorFactory.getQueryExecutor(queryModel, new Configuration());
    try {
      CarbonIterator iterator = queryExecutor.execute(queryModel);
      readSupport.initialize(queryModel.getProjectionColumns(), queryModel.getTable());
      PrestoCarbonVectorizedRecordReader reader =
          new PrestoCarbonVectorizedRecordReader(queryExecutor, queryModel,
              (AbstractDetailQueryResultIterator) iterator, readSupport);
      reader.setTaskId(Long.parseLong(carbonSplit.getSchema().getProperty("index")));
      return reader;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create reader ", e);
    }
  }

  /**
   * @param carbondataSplit
   * @param columns
   * @return
   */
  private QueryModel createQueryModel(HiveSplit carbondataSplit,
      List<? extends ColumnHandle> columns, Configuration conf) {

    try {
      CarbonProjection carbonProjection = getCarbonProjection(columns);
      CarbonTable carbonTable = getCarbonTable(carbondataSplit, conf);
      conf.set(CarbonTableInputFormat.INPUT_SEGMENT_NUMBERS, "");
      String carbonTablePath = carbonTable.getAbsoluteTableIdentifier().getTablePath();
      CarbonTableInputFormat
          .setTransactionalTable(conf, carbonTable.getTableInfo().isTransactionalTable());
      CarbonTableInputFormat.setTableInfo(conf, carbonTable.getTableInfo());
      conf.set(CarbonTableInputFormat.INPUT_DIR, carbonTablePath);
      conf.set("query.id", queryId);
      JobConf jobConf = new JobConf(conf);
      CarbonTableInputFormat carbonTableInputFormat = createInputFormat(jobConf, carbonTable,
          PrestoFilterUtil.parseFilterExpression(carbondataSplit.getEffectivePredicate()),
          carbonProjection);
      TaskAttemptContextImpl hadoopAttemptContext =
          new TaskAttemptContextImpl(jobConf, new TaskAttemptID("", 1, TaskType.MAP, 0, 0));
      CarbonMultiBlockSplit carbonInputSplit = CarbonLocalMultiBlockSplit
          .convertSplit(carbondataSplit.getSchema().getProperty("carbonSplit"));
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
    ImmutableList.Builder<HiveColumnHandle> handles = ImmutableList.builder();
    for (ColumnHandle handle : columns) {
      handles.add(checkType(handle, HiveColumnHandle.class, "handle"));
      carbonProjection.addColumn(((HiveColumnHandle) handle).getName());
    }
    return carbonProjection;
  }

  /**
   * @param carbonSplit
   * @return
   */
  private CarbonTable getCarbonTable(HiveSplit carbonSplit, Configuration configuration) {
    CarbonTableCacheModel tableCacheModel = carbonTableReader
        .getCarbonCache(new SchemaTableName(carbonSplit.getDatabase(), carbonSplit.getTable()),
            carbonSplit.getSchema().getProperty("tablePath"), configuration);
    checkNotNull(tableCacheModel, "tableCacheModel should not be null");
    checkNotNull(tableCacheModel.getCarbonTable(),
        "tableCacheModel.carbonTable should not be null");
    checkNotNull(tableCacheModel.getCarbonTable().getTableInfo(),
        "tableCacheModel.carbonTable.tableInfo should not be null");
    return tableCacheModel.getCarbonTable();
  }

}
