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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.hive.HiveBucketProperty;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveInsertTableHandle;
import io.prestosql.plugin.hive.HiveStorageFormat;
import io.prestosql.plugin.hive.LocationHandle;
import io.prestosql.plugin.hive.metastore.HivePageSinkMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;

import static java.util.Objects.requireNonNull;

public class CarbonDataInsertTableHandle extends HiveInsertTableHandle implements
    ConnectorInsertTableHandle {

  private final Map<String, String> additionalConf;

  @JsonCreator public CarbonDataInsertTableHandle(
      @JsonProperty("schemaName") String schemaName,
      @JsonProperty("tableName") String tableName,
      @JsonProperty("inputColumns") List<HiveColumnHandle> inputColumns,
      @JsonProperty("pageSinkMetadata") HivePageSinkMetadata pageSinkMetadata,
      @JsonProperty("locationHandle") LocationHandle locationHandle,
      @JsonProperty("bucketProperty") Optional<HiveBucketProperty> bucketProperty,
      @JsonProperty("tableStorageFormat") HiveStorageFormat tableStorageFormat,
      @JsonProperty("partitionStorageFormat") HiveStorageFormat partitionStorageFormat,
      @JsonProperty("additionalConf") Map<String, String> additionalConf) {
    super(schemaName, tableName, inputColumns, pageSinkMetadata, locationHandle, bucketProperty,
        tableStorageFormat, partitionStorageFormat);
    this.additionalConf =
        ImmutableMap.copyOf(requireNonNull(additionalConf, "additionConf Map is null"));
  }

  @JsonProperty public Map<String, String> getAdditionalConf() {
    return additionalConf;
  }
}
