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

package org.apache.carbondata.core.metadata;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * support converting database name to session-related name
 */
public abstract class DatabaseLocationProvider {

  private static final DatabaseLocationProvider PROVIDER;

  static {
    final String providerClassName = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.DATABASE_LOCATION_PROVIDER);
    final DatabaseLocationProvider provider;
    if (providerClassName == null) {
      provider = null;
    } else {
      try {
        final Class providerClass =
            DatabaseLocationProvider.class.getClassLoader().loadClass(providerClassName);
        provider = (DatabaseLocationProvider) providerClass.newInstance();
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException exception) {
        throw new RuntimeException(
            "Fail to construct database location provider[" + providerClassName + "].", exception);
      }
    }
    PROVIDER = provider;
  }

  public static DatabaseLocationProvider get() {
    return PROVIDER == null ? Default.INSTANCE : PROVIDER;
  }

  public abstract String provide(String originalDatabaseName);

  private static final class Default extends DatabaseLocationProvider {

    static final Default INSTANCE = new Default();

    @Override
    public String provide(final String originalDatabaseName) {
      return originalDatabaseName;
    }
  }
}
