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

package org.apache.carbondata.trino.page;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.trino.impl.CarbonTableCacheModel;
import org.apache.carbondata.trino.impl.CarbonTableReader;

import static org.apache.carbondata.trino.Types.checkType;

import com.google.inject.Inject;
import io.trino.plugin.hive.GenericHiveRecordCursorProvider;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.HivePageSourceProvider;
import io.trino.plugin.hive.HiveRecordCursorProvider;
import io.trino.plugin.hive.HiveSplit;
import io.trino.plugin.hive.orc.OrcFileWriterFactory;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provider Class for Carbondata Page Source class.
 */
public class CarbonDataPageSourceProvider extends HivePageSourceProvider {

  private final CarbonTableReader carbonTableReader;
  private final HdfsEnvironment hdfsEnvironment;
  private String queryId;

  @Inject
  public CarbonDataPageSourceProvider(TypeManager typeManager, HdfsEnvironment hdfsEnvironment,
      HiveConfig hiveConfig, Set<HivePageSourceFactory> pageSourceFactories,
      Set<HiveRecordCursorProvider> cursorProviders,
      GenericHiveRecordCursorProvider genericCursorProvider,
      OrcFileWriterFactory orcFileWriterFactory, CarbonTableReader carbonTableReader) {
    super(typeManager, hdfsEnvironment, hiveConfig, pageSourceFactories, cursorProviders,
        genericCursorProvider, orcFileWriterFactory);
    this.carbonTableReader = requireNonNull(carbonTableReader, "carbonTableReader is null");
    this.hdfsEnvironment = hdfsEnvironment;
  }

  @Override
  public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction,
      ConnectorSession session, ConnectorSplit split, ConnectorTableHandle tableHandle,
      List<ColumnHandle> columns, DynamicFilter dynamicFilter) {
    HiveSplit carbonSplit = checkType(split, HiveSplit.class, "split is not class HiveSplit");
    this.queryId = carbonSplit.getSchema().getProperty("queryId");
    if (this.queryId == null) {
      // Fall back to hive pagesource.
      return super.createPageSource(transaction, session, split, tableHandle, columns,
          dynamicFilter);
    }
    // TODO: check and use dynamicFilter in CarbonDataPageSource
    Configuration configuration =
        this.hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session),
            new Path(carbonSplit.getSchema().getProperty("tablePath")));
    configuration = carbonTableReader.updateS3Properties(configuration);
    for (Map.Entry<Object, Object> entry : carbonSplit.getSchema().entrySet()) {
      configuration.set(entry.getKey().toString(), entry.getValue().toString());
    }
    CarbonTable carbonTable = getCarbonTable(carbonSplit, configuration);
    boolean isDirectVectorFill = carbonTableReader.config.getPushRowFilter() == null
        || carbonTableReader.config.getPushRowFilter().equalsIgnoreCase("false");
    return new CarbonDataPageSource(carbonTable, queryId, carbonSplit, columns, tableHandle,
        configuration, isDirectVectorFill);
  }

  /**
   * @param carbonSplit
   * @return
   */
  private CarbonTable getCarbonTable(HiveSplit carbonSplit, Configuration configuration) {
    CarbonTableCacheModel tableCacheModel = carbonTableReader.getCarbonCache(
        new SchemaTableName(carbonSplit.getDatabase(), carbonSplit.getTable()),
        carbonSplit.getSchema().getProperty("tablePath"), configuration);
    checkNotNull(tableCacheModel, "tableCacheModel should not be null");
    checkNotNull(tableCacheModel.getCarbonTable(),
        "tableCacheModel.carbonTable should not be null");
    checkNotNull(tableCacheModel.getCarbonTable().getTableInfo(),
        "tableCacheModel.carbonTable.tableInfo should not be null");
    return tableCacheModel.getCarbonTable();
  }

}
