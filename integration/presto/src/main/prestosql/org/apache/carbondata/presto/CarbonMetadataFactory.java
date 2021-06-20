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

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.plugin.base.CatalogName;
import io.prestosql.plugin.hive.ForHiveTransactionHeartbeats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveMetadataFactory;
import io.prestosql.plugin.hive.HiveMetastoreClosure;
import io.prestosql.plugin.hive.HivePartitionManager;
import io.prestosql.plugin.hive.LocationService;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.PartitionUpdate;
import io.prestosql.plugin.hive.TransactionalMetadata;
import io.prestosql.plugin.hive.TypeTranslator;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.security.AccessControlMetadataFactory;
import io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider;
import io.prestosql.spi.type.TypeManager;
import org.joda.time.DateTimeZone;

import static io.prestosql.plugin.hive.metastore.cache.CachingHiveMetastore.memoizeMetastore;

public class CarbonMetadataFactory extends HiveMetadataFactory {

  private static final Logger log = Logger.get(HiveMetadataFactory.class);
  private final boolean allowCorruptWritesForTesting;
  private final boolean skipDeletionForAlter;
  private final boolean skipTargetCleanupOnRollback;
  private final boolean writesToNonManagedTablesEnabled;
  private final boolean createsOfNonManagedTablesEnabled;
  private final long perTransactionCacheMaximumSize;
  private final HiveMetastore metastore;
  private final HdfsEnvironment hdfsEnvironment;
  private final HivePartitionManager partitionManager;
  private final DateTimeZone timeZone;
  private final TypeManager typeManager;
  private final LocationService locationService;
  private final BoundedExecutor renameExecution;
  private final TypeTranslator typeTranslator;
  private final String prestoVersion;
  private final AccessControlMetadataFactory accessControlMetadataFactory;
  private final JsonCodec partitionUpdateCodec;
  private final CatalogName catalogName;
  private final boolean translateHiveViews;
  private final BoundedExecutor dropExecutor;
  private final Optional<Duration> hiveTransactionHeartbeatInterval;
  private final ScheduledExecutorService heartbeatService;

  @Inject public CarbonMetadataFactory(
      CatalogName catalogName,
      HiveConfig hiveConfig,
      HiveMetastore metastore,
      HdfsEnvironment hdfsEnvironment,
      HivePartitionManager partitionManager,
      ExecutorService executorService,
      @ForHiveTransactionHeartbeats ScheduledExecutorService heartbeatService,
      TypeManager typeManager,
      LocationService locationService,
      JsonCodec<PartitionUpdate> partitionUpdateCodec,
      TypeTranslator typeTranslator,
      NodeVersion nodeVersion,
      AccessControlMetadataFactory accessControlMetadataFactory) {
    this(catalogName, metastore, hdfsEnvironment, partitionManager, hiveConfig.getDateTimeZone(),
        hiveConfig.getMaxConcurrentFileRenames(), hiveConfig.getMaxConcurrentMetastoreDrops(),
        hiveConfig.getAllowCorruptWritesForTesting(), hiveConfig.isSkipDeletionForAlter(),
        hiveConfig.isSkipTargetCleanupOnRollback(), hiveConfig.getWritesToNonManagedTablesEnabled(),
        hiveConfig.getCreatesOfNonManagedTablesEnabled(), hiveConfig.isTranslateHiveViews(),
        hiveConfig.getPerTransactionMetastoreCacheMaximumSize(),
        hiveConfig.getHiveTransactionHeartbeatInterval(), typeManager, locationService,
        partitionUpdateCodec, executorService, heartbeatService, typeTranslator,
        nodeVersion.toString(), accessControlMetadataFactory);
  }

  public CarbonMetadataFactory(CatalogName catalogName, HiveMetastore metastore,
      HdfsEnvironment hdfsEnvironment, HivePartitionManager partitionManager, DateTimeZone timeZone,
      int maxConcurrentFileRenames, int maxConcurrentMetastoreDrops,
      boolean allowCorruptWritesForTesting, boolean skipDeletionForAlter,
      boolean skipTargetCleanupOnRollback, boolean writesToNonManagedTablesEnabled,
      boolean createsOfNonManagedTablesEnabled, boolean translateHiveViews,
      long perTransactionCacheMaximumSize, Optional<Duration> hiveTransactionHeartbeatInterval,
      TypeManager typeManager, LocationService locationService,
      JsonCodec<PartitionUpdate> partitionUpdateCodec, ExecutorService executorService,
      ScheduledExecutorService heartbeatService, TypeTranslator typeTranslator,
      String prestoVersion, AccessControlMetadataFactory accessControlMetadataFactory) {
    super(catalogName, metastore, hdfsEnvironment, partitionManager, timeZone,
        maxConcurrentFileRenames, maxConcurrentMetastoreDrops, allowCorruptWritesForTesting,
        skipDeletionForAlter, skipTargetCleanupOnRollback, writesToNonManagedTablesEnabled,
        createsOfNonManagedTablesEnabled, translateHiveViews, perTransactionCacheMaximumSize,
        hiveTransactionHeartbeatInterval, typeManager, locationService, partitionUpdateCodec,
        executorService, heartbeatService, typeTranslator, prestoVersion,
        accessControlMetadataFactory);
    this.allowCorruptWritesForTesting = allowCorruptWritesForTesting;
    this.skipDeletionForAlter = skipDeletionForAlter;
    this.skipTargetCleanupOnRollback = skipTargetCleanupOnRollback;
    this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
    this.perTransactionCacheMaximumSize = perTransactionCacheMaximumSize;
    this.metastore = Objects.requireNonNull(metastore, "metastore is null");
    this.hdfsEnvironment = Objects.requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    this.partitionManager = Objects.requireNonNull(partitionManager, "partitionManager is null");
    this.timeZone = Objects.requireNonNull(timeZone, "timeZone is null");
    this.typeManager = Objects.requireNonNull(typeManager, "typeManager is null");
    this.locationService = Objects.requireNonNull(locationService, "locationService is null");
    this.partitionUpdateCodec =
        Objects.requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
    this.typeTranslator = Objects.requireNonNull(typeTranslator, "typeTranslator is null");
    this.prestoVersion = Objects.requireNonNull(prestoVersion, "prestoVersion is null");
    this.accessControlMetadataFactory = Objects
        .requireNonNull(accessControlMetadataFactory, "accessControlMetadataFactory is null");
    if (!allowCorruptWritesForTesting && !timeZone.equals(DateTimeZone.getDefault())) {
      log.warn(
          "Hive writes are disabled. "
              + "To write data to Hive, your JVM timezone must match the Hive storage timezone. "
              + "Add -Duser.timezone=%s to your JVM arguments",
          timeZone.getID());
    }
    this.renameExecution = new BoundedExecutor(executorService, maxConcurrentFileRenames);
    this.dropExecutor = new BoundedExecutor(executorService, maxConcurrentMetastoreDrops);
    this.catalogName = catalogName;
    this.translateHiveViews = translateHiveViews;
    this.hiveTransactionHeartbeatInterval = hiveTransactionHeartbeatInterval;
    this.heartbeatService = heartbeatService;
    this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
  }

  @Override
  public TransactionalMetadata create()
  {
    SemiTransactionalHiveMetastore metaStore = new SemiTransactionalHiveMetastore(
        hdfsEnvironment,
        new HiveMetastoreClosure(memoizeMetastore(this.metastore, perTransactionCacheMaximumSize)),
        renameExecution,
        dropExecutor,
        skipDeletionForAlter,
        skipTargetCleanupOnRollback,
        hiveTransactionHeartbeatInterval,
        heartbeatService);

    return new CarbonDataMetaData(
        catalogName,
        metaStore,
        hdfsEnvironment,
        partitionManager,
        timeZone,
        allowCorruptWritesForTesting,
        writesToNonManagedTablesEnabled,
        createsOfNonManagedTablesEnabled,
        translateHiveViews,
        typeManager,
        locationService,
        partitionUpdateCodec,
        typeTranslator,
        prestoVersion,
        new MetastoreHiveStatisticsProvider(metaStore),
        accessControlMetadataFactory.create(metaStore));
  }

}
