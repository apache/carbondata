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

import java.util.*;
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

import com.facebook.presto.hive.CoercionPolicy;
import com.facebook.presto.hive.DirectoryLister;
import com.facebook.presto.hive.ForHiveClient;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveSplit;
import com.facebook.presto.hive.HiveSplitManager;
import com.facebook.presto.hive.HiveTableLayoutHandle;
import com.facebook.presto.hive.HiveTransactionHandle;
import com.facebook.presto.hive.NamenodeStats;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
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

  @Inject public CarbondataSplitManager(HiveClientConfig hiveClientConfig,
      Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider,
      NamenodeStats namenodeStats, HdfsEnvironment hdfsEnvironment, DirectoryLister directoryLister,
      @ForHiveClient ExecutorService executorService, CoercionPolicy coercionPolicy,
      CarbonTableReader reader) {
    super(hiveClientConfig, metastoreProvider, namenodeStats, hdfsEnvironment, directoryLister,
        executorService, coercionPolicy);
    this.carbonTableReader = requireNonNull(reader, "client is null");
    this.metastoreProvider = requireNonNull(metastoreProvider, "metastore is null");
    this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
  }

  public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorTableLayoutHandle layoutHandle,
      SplitSchedulingStrategy splitSchedulingStrategy) {

    HiveTableLayoutHandle layout = (HiveTableLayoutHandle) layoutHandle;
    SchemaTableName schemaTableName = layout.getSchemaTableName();

    // get table metadata
    SemiTransactionalHiveMetastore metastore =
        metastoreProvider.apply((HiveTransactionHandle) transactionHandle);
    Table table =
        metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName())
            .orElseThrow(() -> new TableNotFoundException(schemaTableName));
    if (!table.getStorage().getStorageFormat().getInputFormat().contains("carbon")) {
      return super.getSplits(transactionHandle, session, layoutHandle, splitSchedulingStrategy);
    }
    String location = table.getStorage().getLocation();

    String queryId = System.nanoTime() + "";
    QueryStatistic statistic = new QueryStatistic();
    QueryStatisticsRecorder statisticRecorder = CarbonTimeStatisticsFactory.createDriverRecorder();
    statistic.addStatistics(QueryStatisticsConstants.BLOCK_ALLOCATION, System.currentTimeMillis());
    statisticRecorder.recordStatisticsForDriver(statistic, queryId);
    statistic = new QueryStatistic();

    carbonTableReader.setQueryId(queryId);
    TupleDomain<HiveColumnHandle> predicate =
        (TupleDomain<HiveColumnHandle>) layout.getCompactEffectivePredicate();
    Configuration configuration = this.hdfsEnvironment.getConfiguration(
        new HdfsEnvironment.HdfsContext(session, schemaTableName.getSchemaName(),
            schemaTableName.getTableName()), new Path(location));
    configuration = carbonTableReader.updateS3Properties(configuration);
    // set the hadoop configuration to thread local, so that FileFactory can use it.
    ThreadLocalSessionInfo.setConfigurationToCurrentThread(configuration);
    CarbonTableCacheModel cache =
        carbonTableReader.getCarbonCache(schemaTableName, location, configuration);
    Expression filters = PrestoFilterUtil.parseFilterExpression(predicate);
    try {

      List<CarbonLocalMultiBlockSplit> splits =
          carbonTableReader.getInputSplits2(cache, filters, predicate, configuration);

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
            schemaTableName.getTableName(), "", 0, 0, 0, properties, new ArrayList(),
            getHostAddresses(split.getLocations()), OptionalInt.empty(), false, predicate,
            new HashMap<>(), Optional.empty()));
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
    return Arrays.stream(hosts).map(HostAddress::fromString).collect(toImmutableList());
  }

}
