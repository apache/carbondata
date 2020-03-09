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

import java.lang.reflect.*;
import java.util.Map;

import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import com.google.inject.Module;
import io.airlift.units.DataSize;
import io.prestosql.plugin.hive.HiveConnectorFactory;
import io.prestosql.plugin.hive.HiveStorageFormat;
import io.prestosql.plugin.hive.InternalHiveConnectorFactory;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import sun.reflect.ConstructorAccessor;

import static com.google.common.base.Throwables.throwIfUnchecked;

/**
 * Build Carbondata Connector
 * It will be called by CarbondataPlugin
 */
public class CarbondataConnectorFactory extends HiveConnectorFactory {

  private Class<? extends Module> module;

  static {
    try {
      setCarbonEnum();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public CarbondataConnectorFactory(String name) {
    super(name);
  }

  public CarbondataConnectorFactory(String connectorName, Class<? extends Module> module) {
    super(connectorName, module);
    this.module = module;
  }


  @Override
  public Connector create(String catalogName, Map<String, String> config,
      ConnectorContext context) {
    ClassLoader classLoader = context.duplicatePluginClassLoader();
    try {
      Object moduleInstance = classLoader.loadClass(this.module.getName()).getConstructor().newInstance();
      Class<?> moduleClass = classLoader.loadClass(Module.class.getName());
      return (Connector) classLoader.loadClass(InternalCarbonDataConnectorFactory.class.getName())
          .getMethod("createConnector", String.class, Map.class, ConnectorContext.class, moduleClass)
          .invoke(null, catalogName, config, context, moduleInstance);
    }
    catch (InvocationTargetException e) {
      Throwable targetException = e.getTargetException();
      throwIfUnchecked(targetException);
      throw new RuntimeException(targetException);
    }
    catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Set the Carbon format enum to HiveStorageFormat, its a hack but for time being it is best
   * choice to avoid lot of code change.
   *
   * @throws Exception
   */
  private static void setCarbonEnum() throws Exception {
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