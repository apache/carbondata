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

import static io.airlift.json.JsonBinder.jsonBinder;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.presto.impl.CarbonTableReader;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.event.client.EventClient;
import io.prestosql.plugin.hive.CachingDirectoryLister;
import io.prestosql.plugin.hive.CoercionPolicy;
import io.prestosql.plugin.hive.DirectoryLister;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.GenericHiveRecordCursorProvider;
import io.prestosql.plugin.hive.HiveAnalyzeProperties;
import io.prestosql.plugin.hive.HiveCoercionPolicy;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveEventClient;
import io.prestosql.plugin.hive.HiveFileWriterFactory;
import io.prestosql.plugin.hive.HiveHdfsModule;
import io.prestosql.plugin.hive.HiveLocationService;
import io.prestosql.plugin.hive.HiveMetadataFactory;
import io.prestosql.plugin.hive.HiveModule;
import io.prestosql.plugin.hive.HiveNodePartitioningProvider;
import io.prestosql.plugin.hive.HivePageSourceFactory;
import io.prestosql.plugin.hive.HivePartitionManager;
import io.prestosql.plugin.hive.HiveRecordCursorProvider;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.HiveTableProperties;
import io.prestosql.plugin.hive.HiveTransactionManager;
import io.prestosql.plugin.hive.HiveTypeTranslator;
import io.prestosql.plugin.hive.HiveWriterStats;
import io.prestosql.plugin.hive.LocationService;
import io.prestosql.plugin.hive.PartitionUpdate;
import io.prestosql.plugin.hive.RcFileFileWriterFactory;
import io.prestosql.plugin.hive.TransactionalMetadataFactory;
import io.prestosql.plugin.hive.TypeTranslator;
import io.prestosql.plugin.hive.orc.OrcFileWriterFactory;
import io.prestosql.plugin.hive.orc.OrcPageSourceFactory;
import io.prestosql.plugin.hive.orc.OrcReaderConfig;
import io.prestosql.plugin.hive.orc.OrcWriterConfig;
import io.prestosql.plugin.hive.parquet.ParquetPageSourceFactory;
import io.prestosql.plugin.hive.parquet.ParquetReaderConfig;
import io.prestosql.plugin.hive.parquet.ParquetWriterConfig;
import io.prestosql.plugin.hive.rcfile.RcFilePageSourceFactory;
import io.prestosql.plugin.hive.s3select.PrestoS3ClientFactory;
import io.prestosql.plugin.hive.s3select.S3SelectRecordCursorProvider;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.type.Type;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

import static org.weakref.jmx.guice.ExportBinder.newExporter;

/**
 * Binds all necessary classes needed for this module.
 */
public class CarbondataModule extends HiveModule {

  @Override public void configure(Binder binder) {
    binder.install(new HiveHdfsModule());

    binder.bind(TypeTranslator.class).toInstance(new HiveTypeTranslator());
    binder.bind(CoercionPolicy.class).to(HiveCoercionPolicy.class).in(Scopes.SINGLETON);

    binder.bind(DirectoryLister.class).to(CachingDirectoryLister.class).in(Scopes.SINGLETON);
    configBinder(binder).bindConfig(HiveConfig.class);

    binder.bind(HiveSessionProperties.class).in(Scopes.SINGLETON);
    binder.bind(HiveTableProperties.class).in(Scopes.SINGLETON);
    binder.bind(HiveAnalyzeProperties.class).in(Scopes.SINGLETON);

    binder.bind(PrestoS3ClientFactory.class).in(Scopes.SINGLETON);

    binder.bind(CachingDirectoryLister.class).in(Scopes.SINGLETON);
    newExporter(binder).export(CachingDirectoryLister.class).withGeneratedName();

    binder.bind(HiveWriterStats.class).in(Scopes.SINGLETON);
    newExporter(binder).export(HiveWriterStats.class).withGeneratedName();

    newSetBinder(binder, EventClient.class).addBinding().to(HiveEventClient.class)
        .in(Scopes.SINGLETON);
    binder.bind(HivePartitionManager.class).in(Scopes.SINGLETON);
    binder.bind(LocationService.class).to(HiveLocationService.class).in(Scopes.SINGLETON);
    binder.bind(HiveLocationService.class).to(CarbonDataLocationService.class).in(Scopes.SINGLETON);
    binder.bind(HiveMetadataFactory.class).to(CarbonMetadataFactory.class).in(Scopes.SINGLETON);
    binder.bind(TransactionalMetadataFactory.class).to(HiveMetadataFactory.class)
        .in(Scopes.SINGLETON);
    binder.bind(HiveTransactionManager.class).in(Scopes.SINGLETON);
    binder.bind(ConnectorSplitManager.class).to(CarbondataSplitManager.class).in(Scopes.SINGLETON);
    newExporter(binder).export(ConnectorSplitManager.class)
        .as(generator -> generator.generatedNameOf(CarbondataSplitManager.class));
    binder.bind(ConnectorPageSourceProvider.class).to(CarbondataPageSourceProvider.class)
        .in(Scopes.SINGLETON);
    binder.bind(ConnectorPageSinkProvider.class).to(CarbonDataPageSinkProvider.class)
        .in(Scopes.SINGLETON);
    binder.bind(ConnectorNodePartitioningProvider.class).to(HiveNodePartitioningProvider.class)
        .in(Scopes.SINGLETON);

    jsonCodecBinder(binder).bindJsonCodec(PartitionUpdate.class);

    binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
    newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

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
    newExporter(binder).export(OrcFileWriterFactory.class).withGeneratedName();
    configBinder(binder).bindConfig(OrcReaderConfig.class);
    configBinder(binder).bindConfig(OrcWriterConfig.class);
    fileWriterFactoryBinder.addBinding().to(OrcFileWriterFactory.class).in(Scopes.SINGLETON);
    fileWriterFactoryBinder.addBinding().to(RcFileFileWriterFactory.class).in(Scopes.SINGLETON);

    configBinder(binder).bindConfig(ParquetReaderConfig.class);
    configBinder(binder).bindConfig(ParquetWriterConfig.class);
    fileWriterFactoryBinder.addBinding().to(CarbonDataFileWriterFactory.class).in(Scopes.SINGLETON);
    binder.bind(CarbonTableReader.class).in(Scopes.SINGLETON);

    jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);

    newSetBinder(binder, SystemTable.class);

    // configure carbon properties
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.IS_QUERY_FROM_PRESTO, "true");
  }
}
