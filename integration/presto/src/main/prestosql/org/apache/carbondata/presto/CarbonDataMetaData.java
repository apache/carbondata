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
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hive.MapredCarbonOutputCommitter;
import org.apache.carbondata.hive.util.HiveCarbonUtil;
import org.apache.carbondata.presto.impl.CarbonTableConfig;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveInsertTableHandle;
import io.prestosql.plugin.hive.HiveMetadata;
import io.prestosql.plugin.hive.HivePartitionManager;
import io.prestosql.plugin.hive.LocationService;
import io.prestosql.plugin.hive.PartitionUpdate;
import io.prestosql.plugin.hive.TypeTranslator;
import io.prestosql.plugin.hive.metastore.MetastoreUtil;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.security.AccessControlMetadata;
import io.prestosql.plugin.hive.statistics.HiveStatisticsProvider;
import io.prestosql.plugin.hive.util.ConfigurationUtils;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.type.TypeManager;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.JobID;

import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;

public class CarbonDataMetaData extends HiveMetadata {

  private static final Logger LOG =
      LogServiceFactory.getLogService(CarbonDataMetaData.class.getName());

  private HdfsEnvironment hdfsEnvironment;
  private SemiTransactionalHiveMetastore metastore;
  private MapredCarbonOutputCommitter carbonOutputCommitter;
  private JobContextImpl jobContext;

  public CarbonDataMetaData(SemiTransactionalHiveMetastore metastore,
      HdfsEnvironment hdfsEnvironment, HivePartitionManager partitionManager, DateTimeZone timeZone,
      boolean allowCorruptWritesForTesting, boolean writesToNonManagedTablesEnabled,
      boolean createsOfNonManagedTablesEnabled, TypeManager typeManager,
      LocationService locationService,
      io.airlift.json.JsonCodec<PartitionUpdate> partitionUpdateCodec,
      TypeTranslator typeTranslator, String prestoVersion,
      HiveStatisticsProvider hiveStatisticsProvider, AccessControlMetadata accessControlMetadata) {
    super(metastore, hdfsEnvironment, partitionManager, timeZone, allowCorruptWritesForTesting,
        true, createsOfNonManagedTablesEnabled, typeManager,
        locationService, partitionUpdateCodec, typeTranslator, prestoVersion,
        hiveStatisticsProvider, accessControlMetadata);
    this.hdfsEnvironment = hdfsEnvironment;
    this.metastore = metastore;
  }

  @Override
  public CarbonDataInsertTableHandle beginInsert(ConnectorSession session,
      ConnectorTableHandle tableHandle) {
    HiveInsertTableHandle hiveInsertTableHandle = super.beginInsert(session, tableHandle);
    SchemaTableName tableName = hiveInsertTableHandle.getSchemaTableName();
    Optional<Table> table =
        this.metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
    Path outputPath =
        new Path(hiveInsertTableHandle.getLocationHandle().getJsonSerializableTargetPath());
    JobConf jobConf = ConfigurationUtils.toJobConf(this.hdfsEnvironment
        .getConfiguration(
            new HdfsEnvironment.HdfsContext(session, hiveInsertTableHandle.getSchemaName(),
                hiveInsertTableHandle.getTableName()),
            new Path(hiveInsertTableHandle.getLocationHandle().getJsonSerializableWritePath())));
    jobConf.set("location", outputPath.toString());
    Properties hiveSchema = MetastoreUtil.getHiveSchema(table.get());
    try {
      CarbonLoadModel carbonLoadModel =
          HiveCarbonUtil.getCarbonLoadModel(hiveSchema, jobConf);

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
    return new CarbonDataInsertTableHandle(hiveInsertTableHandle.getSchemaTableName().getSchemaName(),
        hiveInsertTableHandle.getTableName(),
        hiveInsertTableHandle.getInputColumns(),
        hiveInsertTableHandle.getPageSinkMetadata(),
        hiveInsertTableHandle.getLocationHandle(),
        hiveInsertTableHandle.getBucketProperty(), hiveInsertTableHandle.getTableStorageFormat(),
        hiveInsertTableHandle.getPartitionStorageFormat(),
        ImmutableMap.of(CarbonTableConfig.CARBON_PRESTO_LOAD_MODEL,
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
