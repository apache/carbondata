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

package org.apache.carbondata.trino.split;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.trino.TrinoFilterUtil;
import org.apache.carbondata.trino.impl.CarbonTableCacheModel;
import org.apache.carbondata.trino.impl.CarbonTableReader;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.DirectoryLister;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePartition;
import io.trino.plugin.hive.HivePartitionManager;
import io.trino.plugin.hive.HiveSplit;
import io.trino.plugin.hive.HiveSplitManager;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.NamenodeStats;
import io.trino.plugin.hive.TableToPartitionMapping;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.HostAddress;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Build CarbonTable splits
 * filtering irrelevant blocks
 */
public class CarbonDataSplitManager extends HiveSplitManager {

  private final CarbonTableReader carbonTableReader;
  private final Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider;
  private final HdfsEnvironment hdfsEnvironment;

  @Inject
  public CarbonDataSplitManager(HiveConfig hiveConfig,
      Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider,
      HivePartitionManager partitionManager, NamenodeStats namenodeStats,
      HdfsEnvironment hdfsEnvironment, DirectoryLister directoryLister,
      ExecutorService executorService, VersionEmbedder versionEmbedder, TypeManager typeManager,
      CarbonTableReader reader) {
    super(hiveConfig, metastoreProvider, partitionManager, namenodeStats, hdfsEnvironment,
        directoryLister, executorService, versionEmbedder, typeManager);
    this.carbonTableReader = requireNonNull(reader, "client is null");
    this.metastoreProvider = requireNonNull(metastoreProvider, "metastore is null");
    this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");

  }

  private static List<HostAddress> getHostAddresses(String[] hosts) {
    return Arrays.stream(hosts).map(HostAddress::fromString).collect(toImmutableList());
  }

  @Override
  public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorTableHandle tableHandle,
      SplitSchedulingStrategy splitSchedulingStrategy, DynamicFilter dynamicFilter) {
    HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
    SchemaTableName schemaTableName = hiveTableHandle.getSchemaTableName();
    carbonTableReader.setPrestoQueryId(session.getQueryId());
    // get table metadata
    SemiTransactionalHiveMetastore metastore =
        metastoreProvider.apply((HiveTransactionHandle) transactionHandle);
    Table table = metastore.getTable(new HiveIdentity(session), schemaTableName.getSchemaName(),
            schemaTableName.getTableName())
        .orElseThrow(() -> new TableNotFoundException(schemaTableName));
    if (!table.getStorage().getStorageFormat().getInputFormat().contains("carbon")) {
      return super.getSplits(transactionHandle, session, tableHandle, splitSchedulingStrategy,
          dynamicFilter);
    }
    // for hive metastore, get table location from catalog table's tablePath
    String location = table.getStorage().getSerdeParameters().get("tablePath");
    if (StringUtils.isEmpty(location)) {
      // file metastore case tablePath can be null, so get from location
      location = table.getStorage().getLocation();
    }
    List<PartitionSpec> filteredPartitions = new ArrayList<>();
    if (hiveTableHandle.getPartitionColumns().size() > 0 && hiveTableHandle.getPartitions()
        .isPresent()) {
      List<String> colNames =
          hiveTableHandle.getPartitionColumns().stream().map(HiveColumnHandle::getName)
              .collect(Collectors.toList());
      for (HivePartition partition : hiveTableHandle.getPartitions().get()) {
        filteredPartitions.add(new PartitionSpec(colNames,
            location + CarbonCommonConstants.FILE_SEPARATOR + partition.getPartitionId()));
      }
    }
    String queryId = System.nanoTime() + "";
    QueryStatistic statistic = new QueryStatistic();
    QueryStatisticsRecorder statisticRecorder = CarbonTimeStatisticsFactory.createDriverRecorder();
    statistic.addStatistics(QueryStatisticsConstants.BLOCK_ALLOCATION, System.currentTimeMillis());
    statisticRecorder.recordStatisticsForDriver(statistic, queryId);
    statistic = new QueryStatistic();

    carbonTableReader.setQueryId(queryId);
    TupleDomain<HiveColumnHandle> predicate = hiveTableHandle.getCompactEffectivePredicate();
    Configuration configuration =
        this.hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session),
            new Path(location));
    configuration = carbonTableReader.updateS3Properties(configuration);
    for (Map.Entry<String, String> entry : table.getStorage().getSerdeParameters().entrySet()) {
      configuration.set(entry.getKey(), entry.getValue());
    }
    // set the hadoop configuration to thread local, so that FileFactory can
    // use it.
    ThreadLocalSessionInfo.setConfigurationToCurrentThread(configuration);
    CarbonTableCacheModel cache =
        carbonTableReader.getCarbonCache(schemaTableName, location, configuration);
    Expression filters = TrinoFilterUtil.parseFilterExpression(predicate);
    try {
      List<CarbonLocalMultiBlockSplit> splits =
          carbonTableReader.getInputSplits(cache, filters, filteredPartitions, configuration);
      ImmutableList.Builder<ConnectorSplit> cSplits = ImmutableList.builder();
      int index = 0;
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
        properties.setProperty("serialization.lib",
            table.getStorage().getStorageFormat().getSerDe());
        HiveSplit hiveSplit =
            new HiveSplit(schemaTableName.getSchemaName(), schemaTableName.getTableName(),
                schemaTableName.getTableName(), cache.getCarbonTable().getTablePath(), 0, 0, 0, 0,
                properties, new ArrayList<>(), getHostAddresses(split.getLocations()),
                OptionalInt.empty(), index, false, TableToPartitionMapping.empty(),
                Optional.empty(), Optional.empty(), false, Optional.empty());
        cSplits.add(hiveSplit);
      }
      statisticRecorder.logStatisticsAsTableDriver();

      statistic.addStatistics(QueryStatisticsConstants.BLOCK_IDENTIFICATION,
          System.currentTimeMillis());
      statisticRecorder.recordStatisticsForDriver(statistic, queryId);
      statisticRecorder.logStatisticsAsTableDriver();
      return new FixedSplitSource(cSplits.build());
    } catch (Exception ex) {
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }

}
