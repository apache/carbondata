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

package org.apache.carbondata.trino;

import static java.util.Objects.requireNonNull;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.trino.impl.CarbonTableReader;
import org.apache.carbondata.trino.metadata.CarbonMetaDataFactory;
import org.apache.carbondata.trino.page.CarbonDataPageSinkProvider;
import org.apache.carbondata.trino.page.CarbonDataPageSourceProvider;
import org.apache.carbondata.trino.split.CarbonDataSplitManager;
import org.apache.carbondata.trino.writers.CarbonDataFileWriterFactory;
import org.apache.carbondata.trino.writers.CarbonDataLocationService;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.event.client.EventClient;
import io.trino.plugin.hive.CachingDirectoryLister;
import io.trino.plugin.hive.DefaultHiveMaterializedViewMetadataFactory;
import io.trino.plugin.hive.DirectoryLister;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.GenericHiveRecordCursorProvider;
import io.trino.plugin.hive.HiveAnalyzeProperties;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveEventClient;
import io.trino.plugin.hive.HiveFileWriterFactory;
import io.trino.plugin.hive.HiveLocationService;
import io.trino.plugin.hive.HiveMaterializedViewMetadataFactory;
import io.trino.plugin.hive.HiveMaterializedViewPropertiesProvider;
import io.trino.plugin.hive.HiveMetadataFactory;
import io.trino.plugin.hive.HiveModule;
import io.trino.plugin.hive.HiveNodePartitioningProvider;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.HivePartitionManager;
import io.trino.plugin.hive.HiveRecordCursorProvider;
import io.trino.plugin.hive.HiveRedirectionsProvider;
import io.trino.plugin.hive.HiveSessionProperties;
import io.trino.plugin.hive.HiveSplitManager;
import io.trino.plugin.hive.HiveTableProperties;
import io.trino.plugin.hive.HiveTransactionManager;
import io.trino.plugin.hive.HiveWriterStats;
import io.trino.plugin.hive.LocationService;
import io.trino.plugin.hive.NoneHiveRedirectionsProvider;
import io.trino.plugin.hive.PartitionUpdate;
import io.trino.plugin.hive.RcFileFileWriterFactory;
import io.trino.plugin.hive.TransactionalMetadataFactory;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.orc.OrcFileWriterFactory;
import io.trino.plugin.hive.orc.OrcPageSourceFactory;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetFileWriterFactory;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.hive.rcfile.RcFilePageSourceFactory;
import io.trino.plugin.hive.s3select.S3SelectRecordCursorProvider;
import io.trino.plugin.hive.s3select.TrinoS3ClientFactory;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.type.Type;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

/**
 * Binds all necessary classes needed for this module.
 */
public class CarbonDataModule extends HiveModule {

  private final String connectorId;

  public CarbonDataModule(String connectorId) {
    this.connectorId = requireNonNull(connectorId, "connector id is null");
  }

  @Override
  public void configure(Binder binder) {
    binder.bind(DirectoryLister.class).to(CachingDirectoryLister.class).in(Scopes.SINGLETON);
    binder.bind(CachingDirectoryLister.class).in(Scopes.SINGLETON);
    newExporter(binder).export(CachingDirectoryLister.class).withGeneratedName();

    configBinder(binder).bindConfig(HiveConfig.class);
    configBinder(binder).bindConfig(MetastoreConfig.class);

    binder.bind(HiveSessionProperties.class).in(Scopes.SINGLETON);
    binder.bind(HiveTableProperties.class).in(Scopes.SINGLETON);
    binder.bind(HiveAnalyzeProperties.class).in(Scopes.SINGLETON);
    newOptionalBinder(binder, HiveMaterializedViewPropertiesProvider.class).setDefault()
        .toInstance(ImmutableList::of);

    // support s3
    binder.bind(TrinoS3ClientFactory.class).in(Scopes.SINGLETON);

    binder.bind(HiveWriterStats.class).in(Scopes.SINGLETON);
    newExporter(binder).export(HiveWriterStats.class)
        .as(generatedNameOf(HiveWriterStats.class, connectorId));
    newSetBinder(binder, EventClient.class).addBinding().to(HiveEventClient.class)
        .in(Scopes.SINGLETON);
    binder.bind(HivePartitionManager.class).in(Scopes.SINGLETON);

    binder.bind(LocationService.class).to(HiveLocationService.class).in(Scopes.SINGLETON);
    binder.bind(HiveLocationService.class).to(CarbonDataLocationService.class).in(Scopes.SINGLETON);
    binder.bind(HiveMetadataFactory.class).to(CarbonMetaDataFactory.class).in(Scopes.SINGLETON);

    //todo ： this new type，don't know what's mean
    newOptionalBinder(binder, HiveRedirectionsProvider.class).setDefault()
        .to(NoneHiveRedirectionsProvider.class).in(Scopes.SINGLETON);
    newOptionalBinder(binder, HiveMaterializedViewMetadataFactory.class).setDefault()
        .to(DefaultHiveMaterializedViewMetadataFactory.class).in(Scopes.SINGLETON);
    newOptionalBinder(binder, TransactionalMetadataFactory.class).setDefault()
        .to(HiveMetadataFactory.class).in(Scopes.SINGLETON);

    binder.bind(HiveTransactionManager.class).in(Scopes.SINGLETON);
    binder.bind(ConnectorSplitManager.class).to(CarbonDataSplitManager.class).in(Scopes.SINGLETON);
    newExporter(binder).export(ConnectorSplitManager.class)
        .as(generator -> generator.generatedNameOf(HiveSplitManager.class, connectorId));
    binder.bind(ConnectorPageSourceProvider.class).to(CarbonDataPageSourceProvider.class)
        .in(Scopes.SINGLETON);
    binder.bind(ConnectorPageSinkProvider.class).to(CarbonDataPageSinkProvider.class)
        .in(Scopes.SINGLETON);
    binder.bind(ConnectorNodePartitioningProvider.class).to(HiveNodePartitioningProvider.class)
        .in(Scopes.SINGLETON);

    jsonCodecBinder(binder).bindJsonCodec(PartitionUpdate.class);

    binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
    newExporter(binder).export(FileFormatDataSourceStats.class)
        .as(generator -> generator.generatedNameOf(FileFormatDataSourceStats.class, connectorId));

    Multibinder<HivePageSourceFactory> pageSourceFactoryBinder =
        newSetBinder(binder, HivePageSourceFactory.class);
    pageSourceFactoryBinder.addBinding().to(OrcPageSourceFactory.class).in(Scopes.SINGLETON);
    pageSourceFactoryBinder.addBinding().to(ParquetPageSourceFactory.class).in(Scopes.SINGLETON);
    pageSourceFactoryBinder.addBinding().to(RcFilePageSourceFactory.class).in(Scopes.SINGLETON);

    Multibinder<HiveRecordCursorProvider> recordCursorProviderBinder =
        newSetBinder(binder, HiveRecordCursorProvider.class);
    recordCursorProviderBinder.addBinding().to(S3SelectRecordCursorProvider.class)
        .in(Scopes.SINGLETON);

    binder.bind(GenericHiveRecordCursorProvider.class).in(Scopes.SINGLETON);

    Multibinder<HiveFileWriterFactory> fileWriterFactoryBinder =
        newSetBinder(binder, HiveFileWriterFactory.class);
    binder.bind(OrcFileWriterFactory.class).in(Scopes.SINGLETON);
    newExporter(binder).export(OrcFileWriterFactory.class)
        .as(generatedNameOf(OrcFileWriterFactory.class, connectorId));

    configBinder(binder).bindConfig(OrcReaderConfig.class);
    configBinder(binder).bindConfig(OrcWriterConfig.class);

    fileWriterFactoryBinder.addBinding().to(OrcFileWriterFactory.class).in(Scopes.SINGLETON);
    fileWriterFactoryBinder.addBinding().to(RcFileFileWriterFactory.class).in(Scopes.SINGLETON);
    fileWriterFactoryBinder.addBinding().to(CarbonDataFileWriterFactory.class).in(Scopes.SINGLETON);
    binder.bind(CarbonTableReader.class).in(Scopes.SINGLETON);

    configBinder(binder).bindConfig(ParquetReaderConfig.class);
    configBinder(binder).bindConfig(ParquetWriterConfig.class);
    fileWriterFactoryBinder.addBinding().to(ParquetFileWriterFactory.class).in(Scopes.SINGLETON);

    jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);

    newSetBinder(binder, SystemTable.class);
    // configure carbon properties
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.IS_QUERY_FROM_PRESTO, "true");
  }
}
