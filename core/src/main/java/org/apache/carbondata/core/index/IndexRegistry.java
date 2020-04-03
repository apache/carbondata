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

package org.apache.carbondata.core.index;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.common.exceptions.MetadataProcessException;
import org.apache.carbondata.common.exceptions.sql.MalformedIndexCommandException;
import org.apache.carbondata.core.index.dev.Index;
import org.apache.carbondata.core.index.dev.IndexFactory;
import org.apache.carbondata.core.metadata.index.CarbonIndexProvider;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;

/**
 * Developer can register a index implementation with a short name.
 * After registration, user can use short name to create the index, like
 * <p>
 * {@code
 *  CREATE index dm ON TABLE table
 *  USING 'short-name-of-the-index'
 * }
 * otherwise, user should use the class name of the index implementation to create the index
 * (subclass of {@link IndexFactory})
 * <p>
 * {@code
 *  CREATE index dm ON TABLE table
 *  USING 'class-name-of-the-index'
 * }
 */
@InterfaceAudience.Developer("Index")
@InterfaceStability.Evolving
public class IndexRegistry {
  private static Map<String, String> shortNameToClassName = new ConcurrentHashMap<>();

  private static void registerIndex(String indexClassName, String shortName) {
    Objects.requireNonNull(indexClassName);
    Objects.requireNonNull(shortName);
    shortNameToClassName.put(shortName, indexClassName);
  }

  private static String getIndexClassName(String shortName) {
    Objects.requireNonNull(shortName);
    return shortNameToClassName.get(shortName);
  }

  public static IndexFactory<? extends Index> getIndexFactoryByShortName(
      CarbonTable table, IndexSchema indexSchema) throws MalformedIndexCommandException {
    String providerName = indexSchema.getProviderName();
    try {
      registerIndex(
          CarbonIndexProvider.get(providerName).getClassName(),
          CarbonIndexProvider.get(providerName).getIndexProviderName());
    } catch (UnsupportedOperationException ex) {
      throw new MalformedIndexCommandException("Index '" + providerName + "' not found", ex);
    }
    IndexFactory<? extends Index> indexFactory;
    String className = getIndexClassName(providerName.toLowerCase());
    if (className != null) {
      try {
        indexFactory = (IndexFactory<? extends Index>)
            Class.forName(className).getConstructors()[0].newInstance(table, indexSchema);
      } catch (ClassNotFoundException ex) {
        throw new MalformedIndexCommandException("Index '" + providerName + "' not found", ex);
      } catch (InvocationTargetException ex) {
        throw new MalformedIndexCommandException(ex.getTargetException().getMessage());
      } catch (InstantiationException | IllegalAccessException | IllegalArgumentException ex) {
        throw new MetadataProcessException(
            "failed to create Index '" + providerName + "': " + ex.getMessage(), ex);
      }
    } else {
      throw new MalformedIndexCommandException("Index '" + providerName + "' not found");
    }
    return indexFactory;
  }
}
