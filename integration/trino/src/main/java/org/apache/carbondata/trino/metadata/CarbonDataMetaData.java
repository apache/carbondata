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

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hive.MapredCarbonOutputCommitter;
import org.apache.carbondata.hive.util.HiveCarbonUtil;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.trino.CarbonDataInsertTableHandle;
import org.apache.carbondata.trino.impl.CarbonTableConfig;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveInsertTableHandle;
import io.trino.plugin.hive.HiveMaterializedViewMetadata;
import io.trino.plugin.hive.HiveMetadata;
import io.trino.plugin.hive.HivePartitionManager;
import io.trino.plugin.hive.HiveRedirectionsProvider;
import io.trino.plugin.hive.LocationService;
import io.trino.plugin.hive.PartitionUpdate;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.security.AccessControlMetadata;
import io.trino.plugin.hive.statistics.HiveStatisticsProvider;
import io.trino.plugin.hive.util.ConfigurationUtils;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.JobID;
import org.apache.log4j.Logger;

import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isTransactionalTable;

public class CarbonDataMetaData extends HiveMetadata {

  private static final Logger LOG =
      LogServiceFactory.getLogService(CarbonDataMetaData.class.getName());

  private final HdfsEnvironment hdfsEnvironment;
  private final SemiTransactionalHiveMetastore metastore;
  private MapredCarbonOutputCommitter carbonOutputCommitter;
  private JobContextImpl jobContext;

  public CarbonDataMetaData(CatalogName catalogName, SemiTransactionalHiveMetastore metastore,
      HdfsEnvironment hdfsEnvironment, HivePartitionManager partitionManager,
      boolean createsOfNonManagedTablesEnabled, boolean translateHiveViews,
      boolean hideDeltaLakeTables, TypeManager typeManager, LocationService locationService,
      JsonCodec<PartitionUpdate> partitionUpdateCodec, String trinoVersion,
      HiveStatisticsProvider hiveStatisticsProvider,
      HiveRedirectionsProvider hiveRedirectionsProvider,
      HiveMaterializedViewMetadata hiveMaterializedViewMetadata,
      AccessControlMetadata accessControlMetadata) {
    super(catalogName, metastore, hdfsEnvironment, partitionManager, true,
        createsOfNonManagedTablesEnabled, translateHiveViews, hideDeltaLakeTables, typeManager,
        locationService, partitionUpdateCodec, trinoVersion, hiveStatisticsProvider,
        hiveRedirectionsProvider, hiveMaterializedViewMetadata, accessControlMetadata);
    this.hdfsEnvironment = hdfsEnvironment;
    this.metastore = metastore;
  }

  @Override
  public CarbonDataInsertTableHandle beginInsert(ConnectorSession session,
      ConnectorTableHandle tableHandle) {
    HiveIdentity identity = new HiveIdentity(session);
    HiveInsertTableHandle hiveInsertTableHandle = super.beginInsert(session, tableHandle);
    SchemaTableName tableName = hiveInsertTableHandle.getSchemaTableName();
    Optional<Table> table =
        this.metastore.getTable(identity, tableName.getSchemaName(), tableName.getTableName());
    Path outputPath =
        new Path(hiveInsertTableHandle.getLocationHandle().getJsonSerializableTargetPath());
    JobConf jobConf = ConfigurationUtils.toJobConf(
        this.hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session),
            new Path(hiveInsertTableHandle.getLocationHandle().getJsonSerializableWritePath())));
    jobConf.set("location", outputPath.toString());
    Properties hiveSchema = MetastoreUtil.getHiveSchema(table.get());
    try {
      CarbonLoadModel carbonLoadModel = HiveCarbonUtil.getCarbonLoadModel(hiveSchema, jobConf);

      CarbonTableOutputFormat.setLoadModel(jobConf, carbonLoadModel);
    } catch (IOException ex) {
      LOG.error("Error while creating carbon load model", ex);
      throw new RuntimeException(ex);
    }
    try {
      carbonOutputCommitter = new MapredCarbonOutputCommitter();
      jobContext = new JobContextImpl(jobConf, new JobID());
      carbonOutputCommitter.setupJob(jobContext);
      ThreadLocalSessionInfo.setConfigurationToCurrentThread(jobConf);
    } catch (IOException e) {
      LOG.error("error setting the output committer", e);
      throw new RuntimeException("error setting the output committer");
    }

    AcidTransaction transaction = isTransactionalTable(table.get().getParameters()) ?
        metastore.beginInsert(session, table.get()) : NO_ACID_TRANSACTION;

    return new CarbonDataInsertTableHandle(
        hiveInsertTableHandle.getSchemaTableName().getSchemaName(),
        hiveInsertTableHandle.getTableName(), hiveInsertTableHandle.getInputColumns(),
        hiveInsertTableHandle.getPageSinkMetadata(), hiveInsertTableHandle.getLocationHandle(),
        hiveInsertTableHandle.getBucketProperty(), hiveInsertTableHandle.getTableStorageFormat(),
        hiveInsertTableHandle.getPartitionStorageFormat(), transaction,
        ImmutableMap.of(CarbonTableConfig.CARBON_TRINO_LOAD_MODEL,
            jobContext.getConfiguration().get(CarbonTableOutputFormat.LOAD_MODEL)));
  }

  @Override
  public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
      ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments,
      Collection<ComputedStatistics> computedStatistics) {
    Optional<ConnectorOutputMetadata> connectorOutputMetadata =
        super.finishInsert(session, insertHandle, fragments, computedStatistics);
    try {
      carbonOutputCommitter.commitJob(jobContext);
    } catch (IOException e) {
      LOG.error("Error occurred while committing the insert job.", e);
      throw new RuntimeException(e);
    }
    return connectorOutputMetadata;
  }
}
