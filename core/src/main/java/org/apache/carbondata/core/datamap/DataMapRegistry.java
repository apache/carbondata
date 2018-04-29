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

package org.apache.carbondata.core.datamap;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.common.exceptions.MetadataProcessException;
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.core.datamap.dev.IndexDataMap;
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider;

/**
 * Developer can register a datamap implementation with a short name.
 * After registration, user can use short name to create the datamap, like
 * <p>
 * {@code
 *  CREATE DATAMAP dm ON TABLE table
 *  USING 'short-name-of-the-datamap'
 * }
 * otherwise, user should use the class name of the datamap implementation to create the datamap
 * (subclass of {@link IndexDataMap})
 * <p>
 * {@code
 *  CREATE DATAMAP dm ON TABLE table
 *  USING 'class-name-of-the-datamap'
 * }
 */
@InterfaceAudience.Developer("DataMap")
@InterfaceStability.Evolving
public class DataMapRegistry {
  private static Map<String, String> shortNameToClassName = new ConcurrentHashMap<>();

  private static void registerDataMap(String datamapClassName, String shortName) {
    Objects.requireNonNull(datamapClassName);
    Objects.requireNonNull(shortName);
    shortNameToClassName.put(shortName, datamapClassName);
  }

  private static String getDataMapClassName(String shortName) {
    Objects.requireNonNull(shortName);
    return shortNameToClassName.get(shortName);
  }

  public static IndexDataMap getDataMapByShortName(String providerName)
      throws MalformedDataMapCommandException {
    try {
      registerDataMap(
          DataMapClassProvider.getDataMapProviderOnName(providerName).getClassName(),
          DataMapClassProvider.getDataMapProviderOnName(providerName).getShortName());
    } catch (UnsupportedOperationException ex) {
      throw new MalformedDataMapCommandException("DataMap '" + providerName + "' not found", ex);
    }
    IndexDataMap indexDataMap;
    String className = getDataMapClassName(providerName.toLowerCase());
    if (className != null) {
      try {
        Class<? extends IndexDataMap> datamapClass =
            (Class<? extends IndexDataMap>) Class.forName(className);
        indexDataMap = datamapClass.newInstance();
      } catch (ClassNotFoundException ex) {
        throw new MalformedDataMapCommandException("DataMap '" + providerName + "' not found", ex);
      } catch (Throwable ex) {
        throw new MetadataProcessException("failed to create DataMap '" + providerName + "'", ex);
      }
    } else {
      throw new MalformedDataMapCommandException("DataMap '" + providerName + "' not found");
    }
    return indexDataMap;
  }
}
