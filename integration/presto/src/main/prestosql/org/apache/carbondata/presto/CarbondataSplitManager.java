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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.presto.impl.CarbonLocalMultiBlockSplit;
import org.apache.carbondata.presto.impl.CarbonTableCacheModel;
import org.apache.carbondata.presto.impl.CarbonTableReader;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.CoercionPolicy;
import io.prestosql.plugin.hive.DirectoryLister;
import io.prestosql.plugin.hive.ForHive;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HivePartitionManager;
import io.prestosql.plugin.hive.HiveSplit;
import io.prestosql.plugin.hive.HiveSplitManager;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.plugin.hive.HiveTransactionHandle;
import io.prestosql.plugin.hive.NamenodeStats;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.VersionEmbedder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Build Carbontable splits
 * filtering irrelevant blocks
 */
public class CarbondataSplitManager extends HiveSplitManager {

  private final CarbonTableReader carbonTableReader;
  private final Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider;
  private final HdfsEnvironment hdfsEnvironment;

  @Inject public CarbondataSplitManager(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3737
      HiveConfig hiveConfig,
      Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider,
      HivePartitionManager partitionManager,
      NamenodeStats namenodeStats,
      HdfsEnvironment hdfsEnvironment,
      DirectoryLister directoryLister,
      @ForHive ExecutorService executorService,
      VersionEmbedder versionEmbedder,
      CoercionPolicy coercionPolicy,
      CarbonTableReader reader) {
    super(hiveConfig, metastoreProvider, partitionManager, namenodeStats, hdfsEnvironment,
        directoryLister, executorService, versionEmbedder, coercionPolicy);
    this.carbonTableReader = requireNonNull(reader, "client is null");
    this.metastoreProvider = requireNonNull(metastoreProvider, "metastore is null");
    this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
  }

  @Override
  public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorTableHandle tableHandle,
      SplitSchedulingStrategy splitSchedulingStrategy) {

    HiveTableHandle hiveTable = (HiveTableHandle) tableHandle;
    SchemaTableName schemaTableName = hiveTable.getSchemaTableName();

    // get table metadata
    SemiTransactionalHiveMetastore metastore =
        metastoreProvider.apply((HiveTransactionHandle) transactionHandle);
    Table table =
        metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName())
            .orElseThrow(() -> new TableNotFoundException(schemaTableName));
    if (!table.getStorage().getStorageFormat().getInputFormat().contains("carbon")) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3737
      return super.getSplits(transactionHandle, session, tableHandle, splitSchedulingStrategy);
    }
    // for hive metastore, get table location from catalog table's tablePath
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3786
    String location = table.getStorage().getSerdeParameters().get("tablePath");
    if (StringUtils.isEmpty(location))  {
      // file metastore case tablePath can be null, so get from location
      location = table.getStorage().getLocation();
    }
    String queryId = System.nanoTime() + "";
    QueryStatistic statistic = new QueryStatistic();
    QueryStatisticsRecorder statisticRecorder = CarbonTimeStatisticsFactory.createDriverRecorder();
    statistic.addStatistics(QueryStatisticsConstants.BLOCK_ALLOCATION, System.currentTimeMillis());
    statisticRecorder.recordStatisticsForDriver(statistic, queryId);
    statistic = new QueryStatistic();

    carbonTableReader.setQueryId(queryId);
    TupleDomain<HiveColumnHandle> predicate =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3737
        (TupleDomain<HiveColumnHandle>) hiveTable.getCompactEffectivePredicate();
    Configuration configuration = this.hdfsEnvironment.getConfiguration(
        new HdfsEnvironment.HdfsContext(session, schemaTableName.getSchemaName(),
            schemaTableName.getTableName()), new Path(location));
    configuration = carbonTableReader.updateS3Properties(configuration);
    // set the hadoop configuration to thread local, so that FileFactory can use it.
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3282
    ThreadLocalSessionInfo.setConfigurationToCurrentThread(configuration);
    CarbonTableCacheModel cache =
        carbonTableReader.getCarbonCache(schemaTableName, location, configuration);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3218
    Expression filters = PrestoFilterUtil.parseFilterExpression(predicate);
    try {

      List<CarbonLocalMultiBlockSplit> splits =
          carbonTableReader.getInputSplits(cache, filters, predicate, configuration);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3737

      ImmutableList.Builder<ConnectorSplit> cSplits = ImmutableList.builder();
      long index = 0;
      for (CarbonLocalMultiBlockSplit split : splits) {
        index++;
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : table.getStorage().getSerdeParameters().entrySet()) {
          properties.setProperty(entry.getKey(), entry.getValue());
        }
        properties.setProperty("tablePath", cache.getCarbonTable().getTablePath());
        properties.setProperty("carbonSplit", split.getJsonString());
        properties.setProperty("queryId", queryId);
        properties.setProperty("index", String.valueOf(index));
        cSplits.add(new HiveSplit(schemaTableName.getSchemaName(), schemaTableName.getTableName(),
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3737
            schemaTableName.getTableName(), cache.getCarbonTable().getTablePath(), 0, 0, 0,
            properties, new ArrayList(), getHostAddresses(split.getLocations()),
            OptionalInt.empty(), false, new HashMap<>(),
            Optional.empty(), false));
      }

      statisticRecorder.logStatisticsAsTableDriver();

      statistic
          .addStatistics(QueryStatisticsConstants.BLOCK_IDENTIFICATION, System.currentTimeMillis());
      statisticRecorder.recordStatisticsForDriver(statistic, queryId);
      statisticRecorder.logStatisticsAsTableDriver();
      return new FixedSplitSource(cSplits.build());
    } catch (Exception ex) {
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }

  private static List<HostAddress> getHostAddresses(String[] hosts) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3194
    return Arrays.stream(hosts).map(HostAddress::fromString).collect(toImmutableList());
  }

}
