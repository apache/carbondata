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

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

import com.google.inject.Inject;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveFileWriterFactory;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.WriterKind;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.JobConf;

public class CarbonDataFileWriterFactory implements HiveFileWriterFactory {

  private final HdfsEnvironment hdfsEnvironment;
  private final TypeManager typeManager;
  private final NodeVersion nodeVersion;
  private final FileFormatDataSourceStats stats;

  @Inject
  public CarbonDataFileWriterFactory(HdfsEnvironment hdfsEnvironment, TypeManager typeManager,
      NodeVersion nodeVersion, FileFormatDataSourceStats stats) {
    this(typeManager, hdfsEnvironment, nodeVersion, stats);
  }

  public CarbonDataFileWriterFactory(TypeManager typeManager, HdfsEnvironment hdfsEnvironment,
      NodeVersion nodeVersion, FileFormatDataSourceStats stats) {
    this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    this.typeManager = requireNonNull(typeManager, "typeManager is null");
    this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
    this.stats = requireNonNull(stats, "stats is null");
  }

  @Override
  public Optional<FileWriter> createFileWriter(Path path, List<String> inputColumnNames,
      StorageFormat storageFormat, Properties schema, JobConf conf, ConnectorSession session,
      OptionalInt bucketNumber, AcidTransaction transaction, boolean useAcidSchema,
      WriterKind writerKind) {

    try {
      return Optional.of(
          new CarbonDataFileWriter(path, inputColumnNames, schema, conf, typeManager));
    } catch (SerDeException e) {
      throw new RuntimeException("Error while creating carbon file writer", e);
    }
  }
}
