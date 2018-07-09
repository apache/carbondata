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

package org.apache.carbondata.horizon;

import java.io.File;
import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.horizon.rest.controller.Horizon;
import org.apache.carbondata.horizon.rest.model.view.CreateTableRequest;
import org.apache.carbondata.horizon.rest.model.view.DropTableRequest;
import org.apache.carbondata.horizon.rest.model.view.LoadRequest;
import org.apache.carbondata.horizon.rest.model.view.SelectRequest;
import org.apache.carbondata.horizon.rest.model.view.SelectResponse;
import org.apache.carbondata.store.api.conf.StoreConf;
import org.apache.carbondata.store.impl.distributed.Worker;
import org.apache.carbondata.store.util.StoreUtil;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.web.client.RestTemplate;

public class HorizonTest {

  private static Worker worker;
  private static String serviceUri = "http://localhost:8080";
  private static String projectFolder;

  private static RestTemplate restTemplate;

  @BeforeClass
  public static void setup() throws IOException, InterruptedException {
    projectFolder = new File(HorizonTest.class.getResource("/").getPath() +
        "../../../../").getCanonicalPath();
    String log4jFile = projectFolder + "/store/conf/log4j.properties";
    String confFile = projectFolder + "/store/conf/store.conf";

    System.setProperty("log.path", projectFolder + "/store/core/target/master_worker.log");
    System.setProperty("carbonstore.conf.file", confFile);
    StoreUtil.initLog4j(log4jFile);

    StoreConf storeConf = new StoreConf(confFile);
    storeConf.conf(
        StoreConf.STORE_LOCATION,
        storeConf.storeLocation() + System.currentTimeMillis());

    new Thread() {
      public void run() {
        Horizon.start(new String[0]);
      }
    }.start();
    Thread.sleep(10000);

    // start worker
    worker = new Worker(storeConf);
    worker.start();

    restTemplate = new RestTemplate();
  }

  @AfterClass
  public static void shutdown() {
    worker.stop();
    Horizon.stop();
  }

  @Test
  public void testHorizon() {
    DropTableRequest request = new DropTableRequest("default", "table_1", false);
    String response =
        restTemplate.postForObject(serviceUri + "/table/drop", request, String.class);
    Assert.assertEquals(true, Boolean.valueOf(response));

    // create table if not exists
    CreateTableRequest table = CreateTableRequest
        .builder()
        .ifNotExists()
        .databaseName("default")
        .tableName("table_1")
        .comment("first table")
        .column("shortField", "SHORT", "short field")
        .column("intField", "INT", "int field")
        .column("bigintField", "LONG", "long field")
        .column("doubleField", "DOUBLE", "double field")
        .column("stringField", "STRING", "string field")
        .column("timestampField", "TIMESTAMP", "timestamp field")
        .column("decimalField", "DECIMAL", 18, 2, "decimal field")
        .column("dateField", "DATE", "date field")
        .column("charField", "CHAR", "char field")
        .column("floatField", "FLOAT", "float field")
        .tblProperties(CarbonCommonConstants.SORT_COLUMNS, "intField")
        .create();
    String createTable =
        restTemplate.postForObject(serviceUri + "/table/create", table, String.class);
    Assert.assertEquals(true, Boolean.valueOf(createTable));

    // load one segment
    LoadRequest load = LoadRequest
        .builder()
        .databaseName("default")
        .tableName("table_1")
        .overwrite(false)
        .inputPath(projectFolder + "/store/core/src/test/resources/data1.csv")
        .options("header", "true")
        .create();
    String loadData =
        restTemplate.postForObject(serviceUri + "/table/load", load, String.class);
    Assert.assertEquals(true, Boolean.valueOf(loadData));

    // select row
    SelectRequest select = SelectRequest
        .builder()
        .databaseName("default")
        .tableName("table_1")
        .select("intField", "stringField")
        .limit(5)
        .create();
    SelectResponse result =
        restTemplate.postForObject(serviceUri + "/table/select", select, SelectResponse.class);
    Assert.assertEquals(5, result.getRows().length);

    // select row with filter
    SelectRequest filter = SelectRequest
        .builder()
        .databaseName("default")
        .tableName("table_1")
        .select("intField", "stringField")
        .filter("intField = 11")
        .limit(5)
        .create();
    SelectResponse fitlerResult =
        restTemplate.postForObject(serviceUri + "/table/select", filter, SelectResponse.class);
    Assert.assertEquals(1, fitlerResult.getRows().length);
  }

}
