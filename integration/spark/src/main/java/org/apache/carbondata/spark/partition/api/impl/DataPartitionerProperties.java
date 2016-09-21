/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.spark.partition.api.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

public final class DataPartitionerProperties {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataPartitionerProperties.class.getName());

  private static DataPartitionerProperties instance;

  private Properties properties;

  private DataPartitionerProperties() {
    properties = loadProperties();
  }

  public static DataPartitionerProperties getInstance() {
    if (instance == null) {
      instance = new DataPartitionerProperties();
    }
    return instance;
  }

  public String getValue(String key, String defaultVal) {
    return properties.getProperty(key, defaultVal);
  }

  public String getValue(String key) {
    return properties.getProperty(key);
  }

  /**
   * Read the properties from CSVFilePartitioner.properties
   */
  private Properties loadProperties() {
    Properties props = new Properties();

    File file = new File("DataPartitioner.properties");
    FileInputStream fis = null;
    try {
      if (file.exists()) {
        fis = new FileInputStream(file);

        props.load(fis);
      }
    } catch (Exception e) {
      LOGGER
          .error(e, e.getMessage());
    } finally {
      if (null != fis) {
        try {
          fis.close();
        } catch (IOException e) {
          LOGGER.error(e,
              e.getMessage());
        }
      }
    }

    return props;
  }
}
