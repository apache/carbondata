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

import java.lang.management.ManagementFactory;
import java.lang.reflect.*;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.presto.impl.CarbonTableConfig;

import com.facebook.presto.hive.HiveConnector;
import com.facebook.presto.hive.HiveConnectorFactory;
import com.facebook.presto.hive.HiveMetadataFactory;
import com.facebook.presto.hive.HiveProcedureModule;
import com.facebook.presto.hive.HiveSchemaProperties;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.HiveTableProperties;
import com.facebook.presto.hive.HiveTransactionManager;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.RebindSafeMBeanServer;
import com.facebook.presto.hive.authentication.HiveAuthenticationModule;
import com.facebook.presto.hive.metastore.HiveMetastoreModule;
import com.facebook.presto.hive.s3.HiveS3Module;
import com.facebook.presto.hive.security.HiveSecurityModule;
import com.facebook.presto.hive.security.PartitionsAwareAccessControl;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorSplitManager;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeNodePartitioningProvider;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.json.JsonModule;
import io.airlift.units.DataSize;
import org.weakref.jmx.guice.MBeanModule;
import sun.reflect.ConstructorAccessor;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Build Carbondata Connector
 * It will be called by CarbondataPlugin
 */
public class CarbondataConnectorFactory extends HiveConnectorFactory {

  private final ClassLoader classLoader;

  public CarbondataConnectorFactory(String connectorName, ClassLoader classLoader) {
    super(connectorName, classLoader, null);
    this.classLoader = requireNonNull(classLoader, "classLoader is null");
  }

  @Override public Connector create(String catalogName, Map<String, String> config,
      ConnectorContext context) {
    requireNonNull(config, "config is null");

    try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
      Bootstrap app = new Bootstrap(new EventModule(), new MBeanModule(), new JsonModule(),
          new CarbondataModule(catalogName), new HiveS3Module(catalogName),
          new HiveMetastoreModule(catalogName, Optional.ofNullable(null)), new HiveSecurityModule(),
          new HiveAuthenticationModule(), new HiveProcedureModule(), binder -> {
        javax.management.MBeanServer platformMBeanServer =
            ManagementFactory.getPlatformMBeanServer();
        binder.bind(javax.management.MBeanServer.class)
            .toInstance(new RebindSafeMBeanServer(platformMBeanServer));
        binder.bind(NodeVersion.class)
            .toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
        binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
        binder.bind(PageSorter.class).toInstance(context.getPageSorter());
        configBinder(binder).bindConfig(CarbonTableConfig.class);
      });

      Injector injector =
          app.strictConfig().doNotInitializeLogging().setRequiredConfigurationProperties(config)
              .initialize();

      setCarbonEnum();

      LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
      HiveMetadataFactory metadataFactory = injector.getInstance(HiveMetadataFactory.class);
      HiveTransactionManager transactionManager =
          injector.getInstance(HiveTransactionManager.class);
      ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
      ConnectorPageSourceProvider connectorPageSource =
          injector.getInstance(ConnectorPageSourceProvider.class);
      ConnectorPageSinkProvider pageSinkProvider =
          injector.getInstance(ConnectorPageSinkProvider.class);
      ConnectorNodePartitioningProvider connectorDistributionProvider =
          injector.getInstance(ConnectorNodePartitioningProvider.class);
      HiveSessionProperties hiveSessionProperties =
          injector.getInstance(HiveSessionProperties.class);
      HiveTableProperties hiveTableProperties = injector.getInstance(HiveTableProperties.class);
      ConnectorAccessControl accessControl =
          new PartitionsAwareAccessControl(injector.getInstance(ConnectorAccessControl.class));
      Set<Procedure> procedures = injector.getInstance(Key.get(new TypeLiteral<Set<Procedure>>() {
      }));

      return new HiveConnector(lifeCycleManager, metadataFactory, transactionManager,
          new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
          new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
          new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader),
          new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
          ImmutableSet.of(), procedures, hiveSessionProperties.getSessionProperties(),
          HiveSchemaProperties.SCHEMA_PROPERTIES, hiveTableProperties.getTableProperties(),
          accessControl, classLoader);
    } catch (Exception e) {
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Set the Carbon format enum to HiveStorageFormat, its a hack but for time being it is best
   * choice to avoid lot of code change.
   *
   * @throws Exception
   */
  private void setCarbonEnum() throws Exception {
    for (HiveStorageFormat format : HiveStorageFormat.values()) {
      if (format.name().equals("CARBON")) {
        return;
      }
    }
    Constructor<?>[] declaredConstructors = HiveStorageFormat.class.getDeclaredConstructors();
    declaredConstructors[0].setAccessible(true);
    Field constructorAccessorField = Constructor.class.getDeclaredField("constructorAccessor");
    constructorAccessorField.setAccessible(true);
    ConstructorAccessor ca =
        (ConstructorAccessor) constructorAccessorField.get(declaredConstructors[0]);
    if (ca == null) {
      Method acquireConstructorAccessorMethod =
          Constructor.class.getDeclaredMethod("acquireConstructorAccessor");
      acquireConstructorAccessorMethod.setAccessible(true);
      ca = (ConstructorAccessor) acquireConstructorAccessorMethod.invoke(declaredConstructors[0]);
    }
    Object instance = ca.newInstance(new Object[] { "CARBON", HiveStorageFormat.values().length, "",
        CarbonTableInputFormat.class.getName(), CarbonTableOutputFormat.class.getName(),
        new DataSize(256.0D, DataSize.Unit.MEGABYTE) });
    Field values = HiveStorageFormat.class.getDeclaredField("$VALUES");
    values.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(values, values.getModifiers() & ~Modifier.FINAL);

    HiveStorageFormat[] hiveStorageFormats =
        new HiveStorageFormat[HiveStorageFormat.values().length + 1];
    HiveStorageFormat[] src = (HiveStorageFormat[]) values.get(null);
    System.arraycopy(src, 0, hiveStorageFormats, 0, src.length);
    hiveStorageFormats[src.length] = (HiveStorageFormat) instance;
    values.set(null, hiveStorageFormats);
  }
}