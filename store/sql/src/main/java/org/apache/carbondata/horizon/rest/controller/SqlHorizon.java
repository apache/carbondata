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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.sdk.store.conf.StoreConf;

import org.apache.spark.sql.CarbonSessionBuilder;
import org.apache.spark.sql.SparkSession;

public class SqlHorizon extends Horizon {

  private static LogService LOGGER =
      LogServiceFactory.getLogService(SqlHorizon.class.getCanonicalName());

  private static SparkSession session;
  private static String storeLocation;

  public static void main(String[] args) {
    if (args.length < 11) {
      LOGGER.error("Usage: SqlHorizon <store location> <fs.s3a.endpoint> <fs.s3a.access.key>"
          + " <fs.s3a.secret.key> <fs.s3a.impl> <dis.endpoint> <dis.region> <dis.projectid>"
          + " <hive metastore> <mrs hdfs url> <hive warehouse>");
      return;
    }

    try {
      storeLocation = args[0];
      FileFactory.getConfiguration().set("fs.defaultFS", args[9]);

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
    } catch (InterruptedException e) {
      LOGGER.error(e);
      throw new RuntimeException(e);
    }
  }

  private static void createSession(String[] args) {
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.STORE_LOCATION, storeLocation)
        .addProperty(CarbonCommonConstants.CARBON_TASK_LOCALITY, "false")
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
        .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
        .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
        .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC, "");

    SparkSession.Builder baseBuilder = SparkSession.builder()
        .appName("Horizon-SQL")
        .config("spark.ui.port", 9876)
        .config("spark.sql.crossJoin.enabled", "true")
        .config("carbon.source.endpoint", args[5])
        .config("carbon.source.region", args[6])
        .config("carbon.source.ak", args[2])
        .config("carbon.source.sk", args[3])
        .config("carbon.source.projectid", args[7])
        .config("spark.sql.warehouse.dir", args[10])
        .config("hive.metastore.uris", args[8])
        .config("spark.hadoop.fs.s3a.endpoint", args[1])
        .config("spark.hadoop.fs.s3a.access.key", args[2])
        .config("spark.hadoop.fs.s3a.secret.key", args[3])
        .config("spark.hadoop.fs.s3a.impl", args[4])
        .config("spark.hadoop.fs.defaultFS", args[9]);

    session = new CarbonSessionBuilder(baseBuilder).build(storeLocation, null, false);
  }

  static SparkSession getSession() {
    return session;
  }

}
