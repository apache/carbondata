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

import java.io.File;
import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.store.api.conf.StoreConf;

import org.apache.spark.sql.CarbonSessionBuilder;
import org.apache.spark.sql.SparkSession;

public class SqlHorizon extends Horizon {

  static SparkSession session;

  private static void createSession(String[] args) throws IOException {
    String rootPath = new File(SqlHorizon.class.getResource("/").getPath()
        + "../../../..").getCanonicalPath();
    String storeLocation = rootPath + "/examples/spark2/target/store";
    String warehouse = rootPath + "/examples/spark2/target/warehouse";
    String metastoredb = rootPath + "/examples/spark2/target";

    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
        .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
        .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
        .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC, "");

    int workThreadNum = 2;
    String masterUrl = "local[" + workThreadNum + "]";

    SparkSession.Builder baseBuilder = SparkSession
        .builder()
        .master(masterUrl)
        .appName("Horizon-SQL")
        .config("spark.ui.port", 9000)
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.driver.host", "localhost")
        .config("spark.sql.crossJoin.enabled", "true");
    session = new CarbonSessionBuilder(baseBuilder).build(storeLocation, metastoredb, true);
  }

  static SparkSession getSession() {
    return session;
  }

  public static void main(String[] args) {
    // Start Spring
    String storeConfFile = System.getProperty(StoreConf.STORE_CONF_FILE);
    if (storeConfFile == null) {
      storeConfFile = getStoreConfFile();
    }
    start(SqlHorizon.class, storeConfFile);

    try {
      // Start CarbonSession
      Thread.sleep(3000);
      createSession(args);
      Thread.sleep(Long.MAX_VALUE);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
