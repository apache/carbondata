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

package org.apache.carbondata.trino.writers;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;

import static java.util.Objects.requireNonNull;

import io.airlift.event.client.EventClient;
import io.airlift.units.DataSize;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveFileWriterFactory;
import io.trino.plugin.hive.HiveSessionProperties;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.HiveWriter;
import io.trino.plugin.hive.HiveWriterFactory;
import io.trino.plugin.hive.HiveWriterStats;
import io.trino.plugin.hive.LocationHandle;
import io.trino.plugin.hive.LocationService;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.HivePageSinkMetadataProvider;
import io.trino.plugin.hive.metastore.SortingColumn;
import io.trino.spi.NodeManager;
import io.trino.spi.Page;
import io.trino.spi.PageSorter;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;

public class CarbonDataWriterFactory extends HiveWriterFactory {

  private final Map<String, String> additionalJobConf;

  public CarbonDataWriterFactory(Set<HiveFileWriterFactory> fileWriterFactories, String schemaName,
      String tableName, boolean isCreateTable, AcidTransaction transaction,
      List<HiveColumnHandle> inputColumns, HiveStorageFormat tableStorageFormat,
      HiveStorageFormat partitionStorageFormat, Map<String, String> additionalTableParameters,
      OptionalInt bucketCount, List<SortingColumn> sortedBy, LocationHandle locationHandle,
      LocationService locationService, String queryId,
      HivePageSinkMetadataProvider pageSinkMetadataProvider, TypeManager typeManager,
      HdfsEnvironment hdfsEnvironment, PageSorter pageSorter, DataSize sortBufferSize,
      int maxOpenSortFiles, DateTimeZone parquetTimeZone, ConnectorSession session,
      NodeManager nodeManager, EventClient eventClient, HiveSessionProperties hiveSessionProperties,
      HiveWriterStats hiveWriterStats, Map<String, String> additionalJobConf) {
    super(fileWriterFactories, schemaName, tableName, isCreateTable, transaction, inputColumns,
        tableStorageFormat, partitionStorageFormat, additionalTableParameters, bucketCount,
        sortedBy, locationHandle, locationService, queryId, pageSinkMetadataProvider, typeManager,
        hdfsEnvironment, pageSorter, sortBufferSize, maxOpenSortFiles, parquetTimeZone, session,
        nodeManager, eventClient, hiveSessionProperties, hiveWriterStats);
    this.additionalJobConf = requireNonNull(additionalJobConf, "Additional jobConf is null");
  }

  @Override
  public HiveWriter createWriter(Page partitionColumns, int position, OptionalInt bucketNumber) {
    // set the additional conf like loadModel to send to worker
    JobConf jobConf = getSuperJobConf();
    additionalJobConf.forEach((k, v) -> jobConf.set(k, v));
    return super.createWriter(partitionColumns, position, bucketNumber);
  }

  private JobConf getSuperJobConf() {
    Object value;
    try {
      Field field = HiveWriterFactory.class.getDeclaredField("conf");
      field.setAccessible(true);
      value = field.get(this);
      field.setAccessible(false);

      if (value == null) {
        return null;
      } else if (JobConf.class.isAssignableFrom(value.getClass())) {
        return (JobConf) value;
      }
    } catch (NoSuchFieldException | IllegalAccessException ex) {
      throw new RuntimeException("JobConf field is not found");
    }
    return (JobConf) value;
  }
}
