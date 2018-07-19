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

package org.apache.carbondata.horizon.rest.controller;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.store.api.conf.StoreConf;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.CarbonSessionBuilder;
import org.apache.spark.sql.SparkSession;

public class SqlHorizon extends Horizon {

  private static LogService LOGGER =
      LogServiceFactory.getLogService(SqlHorizon.class.getCanonicalName());

  private static SparkSession session;
  private static Configuration configuration;
  private static String storeLocation;

  private static void createSession(String[] args) throws IOException {
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.STORE_LOCATION, storeLocation)
        .addProperty(CarbonCommonConstants.CARBON_TASK_LOCALITY, "false")
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
        .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
        .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
        .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC, "");

    SparkSession.Builder baseBuilder = SparkSession.builder()
        .appName("Horizon-SQL")
        .config("spark.ui.port", 9876)
        .config("spark.sql.crossJoin.enabled", "true");

    Iterator<Map.Entry<String, String>> iterator = configuration.iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      baseBuilder.config(entry.getKey(), entry.getValue());
    }

    session = new CarbonSessionBuilder(baseBuilder).build(storeLocation, null, true);
  }

  static SparkSession getSession() {
    return session;
  }

  public static void main(String[] args) {
    if (args.length < 5) {
      LOGGER.error("Usage: SqlHorizon <store location> <fs.s3a.endpoint> <fs.s3a.access.key>"
          + " <fs.s3a.secret.key> <fs.s3a.impl>");
      return;
    }

    try {
      storeLocation = args[0];
      configuration = new Configuration();
      configuration.set("fs.s3a.endpoint", args[1]);
      configuration.set("fs.s3a.access.key", args[2]);
      configuration.set("fs.s3a.secret.key", args[3]);
      configuration.set("fs.s3a.impl", args[4]);

      String ip = InetAddress.getLocalHost().getHostAddress();
      LOGGER.audit("Driver IP: " + ip);
    } catch (IOException e) {
      LOGGER.error(e);
      throw new RuntimeException(e);
    }

    // Start Spring
    String storeConfFile = System.getProperty(StoreConf.STORE_CONF_FILE);
    start(SqlHorizon.class, storeConfFile);

    try {
      createSession(args);
      Thread.sleep(Long.MAX_VALUE);
    } catch (IOException | InterruptedException e) {
      LOGGER.error(e);
      throw new RuntimeException(e);
    }
  }

}
