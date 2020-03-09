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

import org.apache.carbondata.hive.CarbonHiveSerDe;
import org.apache.carbondata.hive.MapredCarbonInputFormat;
import org.apache.carbondata.hive.MapredCarbonOutputFormat;

import com.google.inject.Module;
import io.airlift.units.DataSize;
import io.prestosql.plugin.hive.HiveConnectorFactory;
import io.prestosql.plugin.hive.HiveStorageFormat;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import sun.misc.Unsafe;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

/**
 * Build Carbondata Connector
 * It will be called by CarbondataPlugin
 */
public class CarbondataConnectorFactory extends HiveConnectorFactory {

  static {
    try {
      setCarbonEnum();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public CarbondataConnectorFactory(String connectorName) {
    this(connectorName, EmptyModule.class);
  }

  public CarbondataConnectorFactory(String connectorName, Class<? extends Module> module) {
    super(connectorName, module);
  }


  @Override
  public Connector create(
      String catalogName,
      Map<String, String> config,
      ConnectorContext context) {
    return InternalCarbonDataConnectorFactory
        .createConnector(catalogName, config, context, new EmptyModule());
  }

  /**
   * Set the Carbon format enum to HiveStorageFormat, its a hack but for time being it is best
   * choice to avoid lot of code change.
   *
   * @throws Exception
   */
  private static void setCarbonEnum() throws Exception {
    for (HiveStorageFormat format : HiveStorageFormat.values()) {
      if (format.name().equals("CARBON") || format.name().equals("ORG.APACHE.CARBONDATA.FORMAT")
          || format.name().equals("CARBONDATA")) {
        return;
      }
    }
    addHiveStorageFormatsForCarbondata("CARBON");
    addHiveStorageFormatsForCarbondata("ORG.APACHE.CARBONDATA.FORMAT");
    addHiveStorageFormatsForCarbondata("CARBONDATA");
  }

  private static void addHiveStorageFormatsForCarbondata(String storedAs) throws Exception {
    Constructor<?> constructor = Unsafe.class.getDeclaredConstructors()[0];
    constructor.setAccessible(true);
    Unsafe unsafe = (Unsafe) constructor.newInstance();
    HiveStorageFormat enumValue =
        (HiveStorageFormat) unsafe.allocateInstance(HiveStorageFormat.class);

    Field nameField = Enum.class.getDeclaredField("name");
    makeAccessible(nameField);
    nameField.set(enumValue, storedAs);

    Field ordinalField = Enum.class.getDeclaredField("ordinal");
    makeAccessible(ordinalField);
    ordinalField.setInt(enumValue, HiveStorageFormat.values().length);

    Field serdeField = HiveStorageFormat.class.getDeclaredField("serde");
    makeAccessible(serdeField);
    serdeField.set(enumValue, CarbonHiveSerDe.class.getName());

    Field inputFormatField = HiveStorageFormat.class.getDeclaredField("inputFormat");
    makeAccessible(inputFormatField);
    inputFormatField.set(enumValue, MapredCarbonInputFormat.class.getName());

    Field outputFormatField = HiveStorageFormat.class.getDeclaredField("outputFormat");
    makeAccessible(outputFormatField);
    outputFormatField.set(enumValue, MapredCarbonOutputFormat.class.getName());

    Field estimatedWriterSystemMemoryUsageField =
        HiveStorageFormat.class.getDeclaredField("estimatedWriterSystemMemoryUsage");
    makeAccessible(estimatedWriterSystemMemoryUsageField);
    estimatedWriterSystemMemoryUsageField.set(enumValue, new DataSize((long) 256, MEGABYTE));

    Field values = HiveStorageFormat.class.getDeclaredField("$VALUES");
    makeAccessible(values);
    HiveStorageFormat[] hiveStorageFormats =
        new HiveStorageFormat[HiveStorageFormat.values().length + 1];
    HiveStorageFormat[] src = (HiveStorageFormat[]) values.get(null);
    System.arraycopy(src, 0, hiveStorageFormats, 0, src.length);
    hiveStorageFormats[src.length] = enumValue;
    values.set(null, hiveStorageFormats);
  }

  private static void makeAccessible(Field field) throws Exception {
    field.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
  }

}
