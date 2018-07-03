package org.apache.carbondata.store;

import java.io.File;
import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.store.conf.StoreConf;
import org.apache.carbondata.store.rest.controller.Horizon;
import org.apache.carbondata.store.master.Master;
import org.apache.carbondata.store.rest.model.vo.LoadRequest;
import org.apache.carbondata.store.rest.model.vo.SelectRequest;
import org.apache.carbondata.store.rest.model.vo.SelectResponse;
import org.apache.carbondata.store.rest.model.vo.TableRequest;
import org.apache.carbondata.store.util.StoreUtil;
import org.apache.carbondata.store.worker.Worker;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.web.client.RestTemplate;

public class StoreTest {

  private static Master master;
  private static Worker worker;
  private static String serviceUri = "http://localhost:8080";
  private static String projectFolder;

  private static RestTemplate restTemplate;

  @BeforeClass
  public static void setup() throws IOException, InterruptedException {
    projectFolder = new File(StoreTest.class.getResource("/").getPath() +
        "../../../../").getCanonicalPath();
    String log4jFile = projectFolder + "/store/conf/log4j.properties";
    String confFile = projectFolder + "/store/conf/store.conf";

    System.setProperty("log.path", projectFolder + "/store/core/target/master_worker.log");
    StoreUtil.initLog4j(log4jFile);

    // start master
    master = Master.getInstance(new StoreConf(confFile));
    master.start();

    new Thread() {
      public void run() {
        Horizon.main(new String[0]);
      }
    }.start();
    Thread.sleep(10000);

    // start worker
    worker = new Worker(new StoreConf(confFile));
    worker.start();

    restTemplate = new RestTemplate();
  }

  @Test
  public void store() {
    // create table if not exists
    TableRequest table = TableRequest
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
        .inputPath(projectFolder + "/examples/spark2/src/main/resources/data1.csv")
        .options("header", "true")
        .create();
    String loadData =
        restTemplate.postForObject(serviceUri + "/table/load", load, String.class);
    Assert.assertEquals(true, Boolean.valueOf(loadData));

    // query
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
  }

  @AfterClass
  public static void release() throws InterruptedException {
    worker.stop();
    Horizon.close();
    master.stopService();
  }

}
