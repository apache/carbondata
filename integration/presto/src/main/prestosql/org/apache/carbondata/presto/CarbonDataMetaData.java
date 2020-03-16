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
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Optional;
import java.util.Properties;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.hadoop.api.CarbonOutputCommitter;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hive.util.HiveCarbonUtil;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;

public class CarbonDataMetaData extends HiveMetadata {

  private static final Logger LOG =
      LogServiceFactory.getLogService(CarbonDataMetaData.class.getName());

  private HdfsEnvironment hdfsEnvironment;
  private SemiTransactionalHiveMetastore metastore;
  private OutputCommitter carbonOutputCommitter;
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
        writesToNonManagedTablesEnabled, createsOfNonManagedTablesEnabled, typeManager,
        locationService, partitionUpdateCodec, typeTranslator, prestoVersion,
        hiveStatisticsProvider, accessControlMetadata);
    this.hdfsEnvironment = hdfsEnvironment;
    this.metastore = metastore;
  }

  @Override
  public HiveInsertTableHandle beginInsert(ConnectorSession session,
      ConnectorTableHandle tableHandle) {
    HiveInsertTableHandle hiveInsertTableHandle = super.beginInsert(session, tableHandle);
    SchemaTableName tableName = hiveInsertTableHandle.getSchemaTableName();
    Optional<Table> table =
        this.metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
    Path outputPath =
        new Path(hiveInsertTableHandle.getLocationHandle().getJsonSerializableTargetPath());
    Configuration initialConfiguration = ConfigurationUtils.toJobConf(this.hdfsEnvironment
        .getConfiguration(
            new HdfsEnvironment.HdfsContext(session, hiveInsertTableHandle.getSchemaName(),
                hiveInsertTableHandle.getTableName()),
            new Path(hiveInsertTableHandle.getLocationHandle().getJsonSerializableWritePath())));
    TaskAttemptID taskAttemptID = TaskAttemptID.forName(initialConfiguration.get("mapred.task.id"));
    Properties hiveSchema = MetastoreUtil.getHiveSchema(table.get());
    try {
      CarbonLoadModel carbonLoadModel =
          HiveCarbonUtil.getCarbonLoadModel(hiveSchema, initialConfiguration);

      CarbonTableOutputFormat.setLoadModel(initialConfiguration, carbonLoadModel);
    } catch (IOException ex) {
      LOG.error("Error while creating carbon load model", ex);
      throw new RuntimeException(ex);
    }
    if (taskAttemptID == null) {
      SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
      String jobTrackerId = formatter.format(new Date());
      taskAttemptID = new TaskAttemptID(jobTrackerId, 0, TaskType.MAP, 0, 0);
    }
    TaskAttemptContextImpl context =
        new TaskAttemptContextImpl(initialConfiguration, taskAttemptID);
    try {
      carbonOutputCommitter = new CarbonOutputCommitter(outputPath, context);
      jobContext = new JobContextImpl(initialConfiguration, new JobID());
      carbonOutputCommitter.setupJob(jobContext);
      ThreadLocalSessionInfo.setConfigurationToCurrentThread(context.getConfiguration());
    } catch (IOException e) {
      throw new RuntimeException("error setting the output committer");
    }
    return hiveInsertTableHandle;
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
