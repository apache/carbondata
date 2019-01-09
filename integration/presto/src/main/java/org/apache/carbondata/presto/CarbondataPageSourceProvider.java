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

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.presto.impl.CarbonTableCacheModel;
import org.apache.carbondata.presto.impl.CarbonTableReader;

import static org.apache.carbondata.presto.Types.checkType;

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
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
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

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
    CarbonTable carbonTable = getCarbonTable(carbonSplit, configuration);
    boolean isDirectVectorFill = carbonTableReader.config.getPushRowFilter() == null ||
        carbonTableReader.config.getPushRowFilter().equalsIgnoreCase("false");
    return new CarbondataPageSource(
        carbonTable, queryId, carbonSplit, columns, configuration, isDirectVectorFill);
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
