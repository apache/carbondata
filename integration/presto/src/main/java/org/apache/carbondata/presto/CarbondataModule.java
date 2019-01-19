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

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

import org.apache.carbondata.presto.impl.CarbonTableReader;

import com.facebook.presto.hive.CoercionPolicy;
import com.facebook.presto.hive.DirectoryLister;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.GenericHiveRecordCursorProvider;
import com.facebook.presto.hive.HadoopDirectoryLister;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationUpdater;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveClientModule;
import com.facebook.presto.hive.HiveCoercionPolicy;
import com.facebook.presto.hive.HiveConnectorId;
import com.facebook.presto.hive.HiveEventClient;
import com.facebook.presto.hive.HiveFileWriterFactory;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.HiveLocationService;
import com.facebook.presto.hive.HiveMetadataFactory;
import com.facebook.presto.hive.HiveNodePartitioningProvider;
import com.facebook.presto.hive.HivePageSinkProvider;
import com.facebook.presto.hive.HivePageSourceFactory;
import com.facebook.presto.hive.HivePartitionManager;
import com.facebook.presto.hive.HiveRecordCursorProvider;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.HiveSplitManager;
import com.facebook.presto.hive.HiveTableProperties;
import com.facebook.presto.hive.HiveTransactionManager;
import com.facebook.presto.hive.HiveTypeTranslator;
import com.facebook.presto.hive.HiveWriterStats;
import com.facebook.presto.hive.LocationService;
import com.facebook.presto.hive.NamenodeStats;
import com.facebook.presto.hive.OrcFileWriterConfig;
import com.facebook.presto.hive.OrcFileWriterFactory;
import com.facebook.presto.hive.PartitionUpdate;
import com.facebook.presto.hive.RcFileFileWriterFactory;
import com.facebook.presto.hive.TableParameterCodec;
import com.facebook.presto.hive.TransactionalMetadata;
import com.facebook.presto.hive.TypeTranslator;
import com.facebook.presto.hive.orc.DwrfPageSourceFactory;
import com.facebook.presto.hive.orc.OrcPageSourceFactory;
import com.facebook.presto.hive.parquet.ParquetPageSourceFactory;
import com.facebook.presto.hive.parquet.ParquetRecordCursorProvider;
import com.facebook.presto.hive.rcfile.RcFilePageSourceFactory;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import io.airlift.event.client.EventClient;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

/**
 * Binds all necessary classes needed for this module.
 */
public class CarbondataModule extends HiveClientModule {

  private final String connectorId;

  public CarbondataModule(String connectorId) {
    super(connectorId);
    this.connectorId = requireNonNull(connectorId, "connector id is null");
  }

  @Override public void configure(Binder binder) {
    binder.bind(HiveConnectorId.class).toInstance(new HiveConnectorId(connectorId));
    binder.bind(TypeTranslator.class).toInstance(new HiveTypeTranslator());
    binder.bind(CoercionPolicy.class).to(HiveCoercionPolicy.class).in(Scopes.SINGLETON);

    binder.bind(HdfsConfigurationUpdater.class).in(Scopes.SINGLETON);
    binder.bind(HdfsConfiguration.class).to(HiveHdfsConfiguration.class).in(Scopes.SINGLETON);
    binder.bind(HdfsEnvironment.class).in(Scopes.SINGLETON);
    binder.bind(DirectoryLister.class).to(HadoopDirectoryLister.class).in(Scopes.SINGLETON);
    configBinder(binder).bindConfig(HiveClientConfig.class);

    binder.bind(HiveSessionProperties.class).in(Scopes.SINGLETON);
    binder.bind(HiveTableProperties.class).in(Scopes.SINGLETON);

    binder.bind(NamenodeStats.class).in(Scopes.SINGLETON);
    newExporter(binder).export(NamenodeStats.class)
        .as(generatedNameOf(NamenodeStats.class, connectorId));

    Multibinder<HiveRecordCursorProvider> recordCursorProviderBinder =
        newSetBinder(binder, HiveRecordCursorProvider.class);
    recordCursorProviderBinder.addBinding().to(ParquetRecordCursorProvider.class)
        .in(Scopes.SINGLETON);
    recordCursorProviderBinder.addBinding().to(GenericHiveRecordCursorProvider.class)
        .in(Scopes.SINGLETON);

    binder.bind(HiveWriterStats.class).in(Scopes.SINGLETON);
    newExporter(binder).export(HiveWriterStats.class)
        .as(generatedNameOf(HiveWriterStats.class, connectorId));

    newSetBinder(binder, EventClient.class).addBinding().to(HiveEventClient.class)
        .in(Scopes.SINGLETON);
    binder.bind(HivePartitionManager.class).in(Scopes.SINGLETON);
    binder.bind(LocationService.class).to(HiveLocationService.class).in(Scopes.SINGLETON);
    binder.bind(TableParameterCodec.class).in(Scopes.SINGLETON);
    binder.bind(HiveMetadataFactory.class).in(Scopes.SINGLETON);
    binder.bind(new TypeLiteral<Supplier<TransactionalMetadata>>() {
    }).to(HiveMetadataFactory.class).in(Scopes.SINGLETON);
    binder.bind(HiveTransactionManager.class).in(Scopes.SINGLETON);
    binder.bind(ConnectorSplitManager.class).to(CarbondataSplitManager.class).in(Scopes.SINGLETON);
    newExporter(binder).export(ConnectorSplitManager.class)
        .as(generatedNameOf(HiveSplitManager.class, connectorId));
    binder.bind(ConnectorPageSourceProvider.class).to(CarbondataPageSourceProvider.class)
        .in(Scopes.SINGLETON);
    binder.bind(ConnectorPageSinkProvider.class).to(HivePageSinkProvider.class)
        .in(Scopes.SINGLETON);
    binder.bind(ConnectorNodePartitioningProvider.class).to(HiveNodePartitioningProvider.class)
        .in(Scopes.SINGLETON);

    jsonCodecBinder(binder).bindJsonCodec(PartitionUpdate.class);

    binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
    newExporter(binder).export(FileFormatDataSourceStats.class)
        .as(generatedNameOf(FileFormatDataSourceStats.class, connectorId));

    Multibinder<HivePageSourceFactory> pageSourceFactoryBinder =
        newSetBinder(binder, HivePageSourceFactory.class);
    pageSourceFactoryBinder.addBinding().to(OrcPageSourceFactory.class).in(Scopes.SINGLETON);
    pageSourceFactoryBinder.addBinding().to(DwrfPageSourceFactory.class).in(Scopes.SINGLETON);
    pageSourceFactoryBinder.addBinding().to(ParquetPageSourceFactory.class).in(Scopes.SINGLETON);
    pageSourceFactoryBinder.addBinding().to(RcFilePageSourceFactory.class).in(Scopes.SINGLETON);

    Multibinder<HiveFileWriterFactory> fileWriterFactoryBinder =
        newSetBinder(binder, HiveFileWriterFactory.class);
    binder.bind(OrcFileWriterFactory.class).in(Scopes.SINGLETON);
    newExporter(binder).export(OrcFileWriterFactory.class)
        .as(generatedNameOf(OrcFileWriterFactory.class, connectorId));
    configBinder(binder).bindConfig(OrcFileWriterConfig.class);
    fileWriterFactoryBinder.addBinding().to(OrcFileWriterFactory.class).in(Scopes.SINGLETON);
    fileWriterFactoryBinder.addBinding().to(RcFileFileWriterFactory.class).in(Scopes.SINGLETON);
    binder.bind(CarbonTableReader.class).in(Scopes.SINGLETON);
  }

}
