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

package org.apache.carbondata.trino.metadata;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Objects.requireNonNull;

import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.ForHiveTransactionHeartbeats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveMaterializedViewMetadataFactory;
import io.trino.plugin.hive.HiveMetadataFactory;
import io.trino.plugin.hive.HiveMetastoreClosure;
import io.trino.plugin.hive.HivePartitionManager;
import io.trino.plugin.hive.HiveRedirectionsProvider;
import io.trino.plugin.hive.LocationService;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.PartitionUpdate;
import io.trino.plugin.hive.TransactionalMetadata;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.security.AccessControlMetadataFactory;
import io.trino.plugin.hive.statistics.MetastoreHiveStatisticsProvider;
import io.trino.spi.type.TypeManager;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.memoizeMetastore;

public class CarbonMetaDataFactory extends HiveMetadataFactory {

  private final CatalogName catalogName;
  private final boolean skipDeletionForAlter;
  private final boolean skipTargetCleanupOnRollback;
  private final boolean createsOfNonManagedTablesEnabled;
  private final boolean translateHiveViews;
  private final boolean hideDeltaLakeTables;
  private final long perTransactionCacheMaximumSize;
  private final HiveMetastore metastore;
  private final HdfsEnvironment hdfsEnvironment;
  private final HivePartitionManager partitionManager;
  private final TypeManager typeManager;
  private final LocationService locationService;
  private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
  private final BoundedExecutor renameExecution;
  private final BoundedExecutor dropExecutor;
  private final Executor updateExecutor;
  private final String trinoVersion;
  private final HiveRedirectionsProvider hiveRedirectionsProvider;
  private final HiveMaterializedViewMetadataFactory hiveMaterializedViewMetadataFactory;
  private final AccessControlMetadataFactory accessControlMetadataFactory;
  private final Optional<Duration> hiveTransactionHeartbeatInterval;
  private final ScheduledExecutorService heartbeatService;

  @Inject
  public CarbonMetaDataFactory(CatalogName catalogName, HiveConfig hiveConfig,
      MetastoreConfig metastoreConfig, HiveMetastore metastore, HdfsEnvironment hdfsEnvironment,
      HivePartitionManager partitionManager, ExecutorService executorService,
      @ForHiveTransactionHeartbeats ScheduledExecutorService heartbeatService,
      TypeManager typeManager, LocationService locationService,
      JsonCodec<PartitionUpdate> partitionUpdateCodec, NodeVersion nodeVersion,
      HiveRedirectionsProvider hiveRedirectionsProvider,
      HiveMaterializedViewMetadataFactory hiveMaterializedViewMetadataFactory,
      AccessControlMetadataFactory accessControlMetadataFactory) {
    this(catalogName, metastore, hdfsEnvironment, partitionManager,
        hiveConfig.getMaxConcurrentFileRenames(), hiveConfig.getMaxConcurrentMetastoreDrops(),
        hiveConfig.getMaxConcurrentMetastoreUpdates(), hiveConfig.isSkipDeletionForAlter(),
        hiveConfig.isSkipTargetCleanupOnRollback(), hiveConfig.getWritesToNonManagedTablesEnabled(),
        hiveConfig.getCreatesOfNonManagedTablesEnabled(), hiveConfig.isTranslateHiveViews(),
        hiveConfig.getPerTransactionMetastoreCacheMaximumSize(),
        hiveConfig.getHiveTransactionHeartbeatInterval(), metastoreConfig.isHideDeltaLakeTables(),
        typeManager, locationService, partitionUpdateCodec, executorService, heartbeatService,
        nodeVersion.toString(), hiveRedirectionsProvider, hiveMaterializedViewMetadataFactory,
        accessControlMetadataFactory);
  }

  public CarbonMetaDataFactory(CatalogName catalogName, HiveMetastore metastore,
      HdfsEnvironment hdfsEnvironment, HivePartitionManager partitionManager,
      int maxConcurrentFileRenames, int maxConcurrentMetastoreDrops,
      int maxConcurrentMetastoreUpdates, boolean skipDeletionForAlter,
      boolean skipTargetCleanupOnRollback, boolean writesToNonManagedTablesEnabled,
      boolean createsOfNonManagedTablesEnabled, boolean translateHiveViews,
      long perTransactionCacheMaximumSize, Optional<Duration> hiveTransactionHeartbeatInterval,
      boolean hideDeltaLakeTables, TypeManager typeManager, LocationService locationService,
      JsonCodec<PartitionUpdate> partitionUpdateCodec, ExecutorService executorService,
      ScheduledExecutorService heartbeatService, String trinoVersion,
      HiveRedirectionsProvider hiveRedirectionsProvider,
      HiveMaterializedViewMetadataFactory hiveMaterializedViewMetadataFactory,
      AccessControlMetadataFactory accessControlMetadataFactory) {
    super(catalogName, metastore, hdfsEnvironment, partitionManager, maxConcurrentFileRenames,
        maxConcurrentMetastoreDrops, maxConcurrentMetastoreUpdates, skipDeletionForAlter,
        skipTargetCleanupOnRollback, true, createsOfNonManagedTablesEnabled, translateHiveViews,
        perTransactionCacheMaximumSize, hiveTransactionHeartbeatInterval, hideDeltaLakeTables,
        typeManager, locationService, partitionUpdateCodec, executorService, heartbeatService,
        trinoVersion, hiveRedirectionsProvider, hiveMaterializedViewMetadataFactory,
        accessControlMetadataFactory);

    this.catalogName = requireNonNull(catalogName, "catalogName is null");
    this.skipDeletionForAlter = skipDeletionForAlter;
    this.skipTargetCleanupOnRollback = skipTargetCleanupOnRollback;
    this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
    this.translateHiveViews = translateHiveViews;
    this.hideDeltaLakeTables = hideDeltaLakeTables;
    this.perTransactionCacheMaximumSize = perTransactionCacheMaximumSize;

    this.metastore = requireNonNull(metastore, "metastore is null");
    this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
    this.typeManager = requireNonNull(typeManager, "typeManager is null");
    this.locationService = requireNonNull(locationService, "locationService is null");
    this.partitionUpdateCodec =
        requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is " + "null");
    this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
    this.hiveRedirectionsProvider =
        requireNonNull(hiveRedirectionsProvider, "hiveRedirectionsProvider is null");
    this.hiveMaterializedViewMetadataFactory = requireNonNull(hiveMaterializedViewMetadataFactory,
        "hiveMaterializedViewMetadataFactory is null");
    this.accessControlMetadataFactory =
        requireNonNull(accessControlMetadataFactory, "accessControlMetadataFactory is null");
    this.hiveTransactionHeartbeatInterval = requireNonNull(hiveTransactionHeartbeatInterval,
        "hiveTransactionHeartbeatInterval is null");

    renameExecution = new BoundedExecutor(executorService, maxConcurrentFileRenames);
    dropExecutor = new BoundedExecutor(executorService, maxConcurrentMetastoreDrops);
    if (maxConcurrentMetastoreUpdates == 1) {
      // this will serve as a kill switch in case we observe that parallel
      // updates causes
      // conflicts in metastore's DB side
      updateExecutor = directExecutor();
    } else {
      updateExecutor = new BoundedExecutor(executorService, maxConcurrentMetastoreUpdates);
    }
    this.heartbeatService = requireNonNull(heartbeatService, "heartbeatService is null");
  }

  @Override
  public TransactionalMetadata create() {
    SemiTransactionalHiveMetastore metastore = new SemiTransactionalHiveMetastore(hdfsEnvironment,
        // per-transaction cache
        new HiveMetastoreClosure(memoizeMetastore(this.metastore, perTransactionCacheMaximumSize)),
        renameExecution, dropExecutor, updateExecutor, skipDeletionForAlter,
        skipTargetCleanupOnRollback, hiveTransactionHeartbeatInterval, heartbeatService);

    return new CarbonDataMetaData(catalogName, metastore, hdfsEnvironment, partitionManager,
        createsOfNonManagedTablesEnabled, translateHiveViews, hideDeltaLakeTables, typeManager,
        locationService, partitionUpdateCodec, trinoVersion,
        new MetastoreHiveStatisticsProvider(metastore), hiveRedirectionsProvider,
        hiveMaterializedViewMetadataFactory.create(metastore),
        accessControlMetadataFactory.create(metastore));
  }
}
