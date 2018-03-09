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

package org.apache.spark.sql.hive.tpcds

import java.io.File
import java.util.{Set => JavaSet}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.CacheTableCommand
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.{SessionState, SharedState, SQLConf, WithTestConf}
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.util.{ShutdownHookManager, Utils}

// SPARK-3729: Test key required to check for initialization errors with config.
object TestHive
  extends TestHiveContext(
    new SparkContext(
      System.getProperty("spark.sql.test.master", "local[1]"),
      "TestSQLContext",
      new SparkConf()
        .set("spark.sql.test", "")
        .set("spark.sql.hive.metastore.barrierPrefixes",
          "org.apache.spark.sql.hive.execution.PairSerDe")
        .set("spark.sql.warehouse.dir", TestHiveContext.makeWarehouseDir().toURI.getPath)
        // SPARK-8910
        .set("spark.ui.enabled", "false")))


case class TestHiveVersion(hiveClient: HiveClient)
  extends TestHiveContext(TestHive.sparkContext, hiveClient)


private[hive] class TestHiveExternalCatalog(
    conf: SparkConf,
    hadoopConf: Configuration,
    hiveClient: Option[HiveClient] = None)
  extends HiveExternalCatalog(conf, hadoopConf) with Logging {

  override lazy val client: HiveClient =
    hiveClient.getOrElse {
      HiveUtils.newClientForMetadata(conf, hadoopConf)
    }
}


private[hive] class TestHiveSharedState(
    sc: SparkContext,
    hiveClient: Option[HiveClient] = None)
  extends SharedState(sc) {

  override lazy val externalCatalog: TestHiveExternalCatalog = {
    new TestHiveExternalCatalog(
      sc.conf,
      sc.hadoopConfiguration,
      hiveClient)
  }
}


/**
 * A locally running test instance of Spark's Hive execution engine.
 *
 * Data from [[testTables]] will be automatically loaded whenever a query is run over those tables.
 * Calling [[reset]] will delete all tables and other state in the database, leaving the database
 * in a "clean" state.
 *
 * TestHive is singleton object version of this class because instantiating multiple copies of the
 * hive metastore seems to lead to weird non-deterministic failures.  Therefore, the execution of
 * test cases that rely on TestHive must be serialized.
 */
class TestHiveContext(
    @transient override val sparkSession: TestHiveSparkSession)
  extends SQLContext(sparkSession) {

  /**
   * If loadTestTables is false, no test tables are loaded. Note that this flag can only be true
   * when running in the JVM, i.e. it needs to be false when calling from Python.
   */
  def this(sc: SparkContext, loadTestTables: Boolean = true) {
    this(new TestHiveSparkSession(HiveUtils.withHiveExternalCatalog(sc), loadTestTables))
  }

  def this(sc: SparkContext, hiveClient: HiveClient) {
    this(new TestHiveSparkSession(HiveUtils.withHiveExternalCatalog(sc),
      hiveClient,
      loadTestTables = false))
  }

  override def newSession(): TestHiveContext = {
    new TestHiveContext(sparkSession.newSession())
  }

  def setCacheTables(c: Boolean): Unit = {
    sparkSession.setCacheTables(c)
  }

  def getHiveFile(path: String): File = {
    sparkSession.getHiveFile(path)
  }

  def loadTestTable(name: String): Unit = {
    sparkSession.loadTestTable(name)
  }

  def reset(): Unit = {
    sparkSession.reset()
  }

}

/**
 * A [[SparkSession]] used in [[TestHiveContext]].
 *
 * @param sc SparkContext
 * @param existingSharedState optional [[SharedState]]
 * @param parentSessionState optional parent [[SessionState]]
 * @param loadTestTables if true, load the test tables. They can only be loaded when running
 *                       in the JVM, i.e when calling from Python this flag has to be false.
 */
private[hive] class TestHiveSparkSession(
    @transient private val sc: SparkContext,
    @transient private val existingSharedState: Option[TestHiveSharedState],
    @transient private val parentSessionState: Option[SessionState],
    private val loadTestTables: Boolean)
  extends SparkSession(sc) with Logging { self =>

  def this(sc: SparkContext, loadTestTables: Boolean) {
    this(
      sc,
      existingSharedState = None,
      parentSessionState = None,
      loadTestTables)
  }

  def this(sc: SparkContext, hiveClient: HiveClient, loadTestTables: Boolean) {
    this(
      sc,
      existingSharedState = Some(new TestHiveSharedState(sc, Some(hiveClient))),
      parentSessionState = None,
      loadTestTables)
  }

  { // set the metastore temporary configuration
    val metastoreTempConf = HiveUtils.newTemporaryConfiguration(useInMemoryDerby = false) ++ Map(
      ConfVars.METASTORE_INTEGER_JDO_PUSHDOWN.varname -> "true",
      // scratch directory used by Hive's metastore client
      ConfVars.SCRATCHDIR.varname -> TestHiveContext.makeScratchDir().toURI.toString,
      ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY.varname -> "1")

    metastoreTempConf.foreach { case (k, v) =>
      sc.hadoopConfiguration.set(k, v)
    }
  }

  assume(sc.conf.get(CATALOG_IMPLEMENTATION) == "hive")

  @transient
  override lazy val sharedState: TestHiveSharedState = {
    existingSharedState.getOrElse(new TestHiveSharedState(sc))
  }

  @transient
  override lazy val sessionState: SessionState = {
    new TestHiveSessionStateBuilder(this, parentSessionState).build()
  }

  lazy val metadataHive: HiveClient = sharedState.externalCatalog.client.newSession()

  override def newSession(): TestHiveSparkSession = {
    new TestHiveSparkSession(sc, Some(sharedState), None, loadTestTables)
  }

  override def cloneSession(): SparkSession = {
    val result = new TestHiveSparkSession(
      sparkContext,
      Some(sharedState),
      Some(sessionState),
      loadTestTables)
    result.sessionState // force copy of SessionState
    result
  }

  private var cacheTables: Boolean = false

  def setCacheTables(c: Boolean): Unit = {
    cacheTables = c
  }

  // By clearing the port we force Spark to pick a new one.  This allows us to rerun tests
  // without restarting the JVM.
  System.clearProperty("spark.hostPort")

  // For some hive test case which contain ${system:test.tmp.dir}
  System.setProperty("test.tmp.dir", Utils.createTempDir().toURI.getPath)

  /** The location of the compiled hive distribution */
  lazy val hiveHome = envVarToFile("HIVE_HOME")

  /** The location of the hive source code. */
  lazy val hiveDevHome = envVarToFile("HIVE_DEV_HOME")

  /**
   * Returns the value of specified environmental variable as a [[java.io.File]] after checking
   * to ensure it exists
   */
  private def envVarToFile(envVar: String): Option[File] = {
    Option(System.getenv(envVar)).map(new File(_))
  }

  val hiveFilesTemp = File.createTempFile("catalystHiveFiles", "")
  hiveFilesTemp.delete()
  hiveFilesTemp.mkdir()
  ShutdownHookManager.registerShutdownDeleteDir(hiveFilesTemp)

  def getHiveFile(path: String): File = {
    new File(Thread.currentThread().getContextClassLoader.getResource(path).getFile)
  }

  private def quoteHiveFile(path : String) = if (Utils.isWindows) {
    getHiveFile(path).getPath.replace('\\', '/')
  } else {
    getHiveFile(path).getPath
  }

  def getWarehousePath(): String = {
    val tempConf = new SQLConf
    sc.conf.getAll.foreach { case (k, v) => tempConf.setConfString(k, v) }
    tempConf.warehousePath
  }

  val describedTable = "DESCRIBE (\\w+)".r

  case class TestTable(name: String, commands: (() => Unit)*)

  protected[hive] implicit class SqlCmd(sql: String) {
    def cmd: () => Unit = {
      () => new TestHiveQueryExecution(sql).hiveResultString(): Unit
    }
  }

  /**
   * A list of test tables and the DDL required to initialize them.  A test table is loaded on
   * demand when a query are run against it.
   */
  @transient
  lazy val testTables = new mutable.HashMap[String, TestTable]()

  def registerTestTable(testTable: TestTable): Unit = {
    testTables += (testTable.name -> testTable)
  }

  if (loadTestTables) {
    // The test tables that are defined in the Hive QTestUtil.
    // /itests/util/src/main/java/org/apache/hadoop/hive/ql/QTestUtil.java
    // https://github.com/apache/hive/blob/branch-0.13/data/scripts/q_test_init.sql
    @transient
    val hiveQTestUtilTables: Seq[TestTable] = Seq(
      TestTable("catalog_sales",
        s"""
           |CREATE TABLE catalog_sales (
           |  `cs_sold_date_sk` int,
           |  `cs_sold_time_sk` int,
           |  `cs_ship_date_sk` int,
           |  `cs_bill_customer_sk` int,
           |  `cs_bill_cdemo_sk` int,
           |  `cs_bill_hdemo_sk` int,
           |  `cs_bill_addr_sk` int,
           |  `cs_ship_customer_sk` int,
           |  `cs_ship_cdemo_sk` int,
           |  `cs_ship_hdemo_sk` int,
           |  `cs_ship_addr_sk` int,
           |  `cs_call_center_sk` int,
           |  `cs_catalog_page_sk` int,
           |  `cs_ship_mode_sk` int,
           |  `cs_warehouse_sk` int,
           |  `cs_item_sk` int,
           |  `cs_promo_sk` int,
           |  `cs_order_number` bigint,
           |  `cs_quantity` int,
           |  `cs_wholesale_cost` decimal(7,2),
           |  `cs_list_price` decimal(7,2),
           |  `cs_sales_price` decimal(7,2),
           |  `cs_ext_discount_amt` decimal(7,2),
           |  `cs_ext_sales_price` decimal(7,2),
           |  `cs_ext_wholesale_cost` decimal(7,2),
           |  `cs_ext_list_price` decimal(7,2),
           |  `cs_ext_tax` decimal(7,2),
           |  `cs_coupon_amt` decimal(7,2),
           |  `cs_ext_ship_cost` decimal(7,2),
           |  `cs_net_paid` decimal(7,2),
           |  `cs_net_paid_inc_tax` decimal(7,2),
           |  `cs_net_paid_inc_ship` decimal(7,2),
           |  `cs_net_paid_inc_ship_tax` decimal(7,2),
           |  `cs_net_profit` decimal(7,2)
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
           |STORED AS TEXTFILE
        """.stripMargin.trim.cmd,
        s"""LOAD DATA LOCAL INPATH '${quoteHiveFile("data/files/catalog_sales.dat")}' INTO TABLE catalog_sales""".cmd),
      TestTable("store_sales",
        s"""
           |CREATE TABLE store_sales (
           |  `ss_sold_date_sk` int,
           |  `ss_sold_time_sk` int,
           |  `ss_item_sk` int,
           |  `ss_customer_sk` int,
           |  `ss_cdemo_sk` int,
           |  `ss_hdemo_sk` int,
           |  `ss_addr_sk` int,
           |  `ss_store_sk` int,
           |  `ss_promo_sk` int,
           |  `ss_ticket_number` bigint,
           |  `ss_quantity` int,
           |  `ss_wholesale_cost` decimal(7,2),
           |  `ss_list_price` decimal(7,2),
           |  `ss_sales_price` decimal(7,2),
           |  `ss_ext_discount_amt` decimal(7,2),
           |  `ss_ext_sales_price` decimal(7,2),
           |  `ss_ext_wholesale_cost` decimal(7,2),
           |  `ss_ext_list_price` decimal(7,2),
           |  `ss_ext_tax` decimal(7,2),
           |  `ss_coupon_amt` decimal(7,2),
           |  `ss_net_paid` decimal(7,2),
           |  `ss_net_paid_inc_tax` decimal(7,2),
           |  `ss_net_profit` decimal(7,2)
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
           |STORED AS TEXTFILE
        """.stripMargin.trim.cmd,
        s"""LOAD DATA LOCAL INPATH '${quoteHiveFile("data/files/store_sales.dat")}' INTO TABLE store_sales""".cmd), 
      TestTable("web_sales",
        s"""
           |CREATE TABLE web_sales (
           |  `ws_sold_date_sk` int,
           |  `ws_sold_time_sk` int,
           |  `ws_ship_date_sk` int,
           |  `ws_item_sk` int,
           |  `ws_bill_customer_sk` int,
           |  `ws_bill_cdemo_sk` int,
           |  `ws_bill_hdemo_sk` int,
           |  `ws_bill_addr_sk` int,
           |  `ws_ship_customer_sk` int,
           |  `ws_ship_cdemo_sk` int,
           |  `ws_ship_hdemo_sk` int,
           |  `ws_ship_addr_sk` int,
           |  `ws_web_page_sk` int,
           |  `ws_web_site_sk` int,
           |  `ws_ship_mode_sk` int,
           |  `ws_warehouse_sk` int,
           |  `ws_promo_sk` int,
           |  `ws_order_number` bigint,
           |  `ws_quantity` int,
           |  `ws_wholesale_cost` decimal(7,2),
           |  `ws_list_price` decimal(7,2),
           |  `ws_sales_price` decimal(7,2),
           |  `ws_ext_discount_amt` decimal(7,2),
           |  `ws_ext_sales_price` decimal(7,2),
           |  `ws_ext_wholesale_cost` decimal(7,2),
           |  `ws_ext_list_price` decimal(7,2),
           |  `ws_ext_tax` decimal(7,2),
           |  `ws_coupon_amt` decimal(7,2),
           |  `ws_ext_ship_cost` decimal(7,2),
           |  `ws_net_paid` decimal(7,2),
           |  `ws_net_paid_inc_tax` decimal(7,2),
           |  `ws_net_paid_inc_ship` decimal(7,2),
           |  `ws_net_paid_inc_ship_tax` decimal(7,2),
           |  `ws_net_profit` decimal(7,2)
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
           |STORED AS TEXTFILE
        """.stripMargin.trim.cmd,
        s"""LOAD DATA LOCAL INPATH '${quoteHiveFile("data/files/web_sales.dat")}' INTO TABLE web_sales""".cmd),
      TestTable("item",
        s"""
           |CREATE TABLE item (
           |  `i_item_sk` int,
           |  `i_item_id` string,
           |  `i_rec_start_date` date,
           |  `i_rec_end_date` date,
           |  `i_item_desc` string,
           |  `i_current_price` decimal(7,2),
           |  `i_wholesale_cost` decimal(7,2),
           |  `i_brand_id` int,
           |  `i_brand` string,
           |  `i_class_id` int,
           |  `i_class` string,
           |  `i_category_id` int,
           |  `i_category` string,
           |  `i_manufact_id` int,
           |  `i_manufact` string,
           |  `i_size` string,
           |  `i_formulation` string,
           |  `i_color` string,
           |  `i_units` string,
           |  `i_container` string,
           |  `i_manager_id` int,
           |  `i_product_name` string
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
           |STORED AS TEXTFILE
        """.stripMargin.trim.cmd,
        s"""LOAD DATA LOCAL INPATH '${quoteHiveFile("data/files/item.dat")}' INTO TABLE item""".cmd),
      TestTable("customer",
        s"""
           |CREATE TABLE customer (
           |  `c_customer_sk` int,
           |  `c_customer_id` string,
           |  `c_current_cdemo_sk` int,
           |  `c_current_hdemo_sk` int,
           |  `c_current_addr_sk` int,
           |  `c_first_shipto_date_sk` int,
           |  `c_first_sales_date_sk` int,
           |  `c_salutation` string,
           |  `c_first_name` string,
           |  `c_last_name` string,
           |  `c_preferred_cust_flag` string,
           |  `c_birth_day` int,
           |  `c_birth_month` int,
           |  `c_birth_year` int,
           |  `c_birth_country` string,
           |  `c_login` string,
           |  `c_email_address` string,
           |  `c_last_review_date` string
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
           |STORED AS TEXTFILE
        """.stripMargin.trim.cmd,
        s"""LOAD DATA LOCAL INPATH '${quoteHiveFile("data/files/customer.dat")}' INTO TABLE customer""".cmd),
      TestTable("date_dim",
        s"""
           |CREATE TABLE date_dim (
           |  `d_date_sk` int,
           |  `d_date_id` string,
           |  `d_date` date,
           |  `d_month_seq` int,
           |  `d_week_seq` int,
           |  `d_quarter_seq` int,
           |  `d_year` int,
           |  `d_dow` int,
           |  `d_moy` int,
           |  `d_dom` int,
           |  `d_qoy` int,
           |  `d_fy_year` int,
           |  `d_fy_quarter_seq` int,
           |  `d_fy_week_seq` int,
           |  `d_day_name` string,
           |  `d_quarter_name` string,
           |  `d_holiday` string,
           |  `d_weekend` string,
           |  `d_following_holiday` string,
           |  `d_first_dom` int,
           |  `d_last_dom` int,
           |  `d_same_day_ly` int,
           |  `d_same_day_lq` int,
           |  `d_current_day` string,
           |  `d_current_week` string,
           |  `d_current_month` string,
           |  `d_current_quarter` string,
           |  `d_current_year` string
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
           |STORED AS TEXTFILE
        """.stripMargin.trim.cmd,
        s"""LOAD DATA LOCAL INPATH '${quoteHiveFile("data/files/date_dim.dat")}' INTO TABLE date_dim""".cmd),
      TestTable("sdr_dyn_seq_custer_iot_all_hour_60min",
        s"""
           |CREATE TABLE sdr_dyn_seq_custer_iot_all_hour_60min
           |(
           |    `dim_1`       String,
           |    `dim_51`      String,
           |    `starttime`   String, 
           |    `dim_2`       String,
           |    `dim_3`       String,
           |    `dim_4`       String,
           |    `dim_5`       String,
           |    `dim_6`       String,
           |    `dim_7`       String,
           |    `dim_8`       String,
           |    `dim_9`       String,
           |    `dim_10`      String, 
           |    `dim_11`      String, 
           |    `dim_12`      String, 
           |    `dim_13`      String, 
           |    `dim_14`      String, 
           |    `dim_15`      String, 
           |    `dim_16`      String, 
           |    `dim_17`      String, 
           |    `dim_18`      String, 
           |    `dim_19`      String, 
           |    `dim_20`      String, 
           |    `dim_21`      String, 
           |    `dim_22`      String, 
           |    `dim_23`      String, 
           |    `dim_24`      String, 
           |    `dim_25`      String, 
           |    `dim_26`      String, 
           |    `dim_27`      String, 
           |    `dim_28`      String, 
           |    `dim_29`      String, 
           |    `dim_30`      String, 
           |    `dim_31`      String, 
           |    `dim_32`      String, 
           |    `dim_33`      String, 
           |    `dim_34`      String, 
           |    `dim_35`      String, 
           |    `dim_36`      String, 
           |    `dim_37`      String, 
           |    `dim_38`      String, 
           |    `dim_39`      String, 
           |    `dim_40`      String, 
           |    `dim_41`      String, 
           |    `dim_42`      String, 
           |    `dim_43`      String, 
           |    `dim_44`      String, 
           |    `dim_45`      String, 
           |    `dim_46`      String, 
           |    `dim_47`      String, 
           |    `dim_48`      String, 
           |    `dim_49`      String,
           |    `dim_50`      String, 
           |    `dim_52`      String, 
           |    `dim_53`      String, 
           |    `dim_54`      String, 
           |    `dim_55`      String, 
           |    `dim_56`      String, 
           |    `dim_57`      String, 
           |    `dim_58`      String, 
           |    `dim_59`      String, 
           |    `dim_60`      String, 
           |    `dim_61`      String, 
           |    `dim_62`      String, 
           |    `dim_63`      String, 
           |    `dim_64`      String, 
           |    `dim_65`      String, 
           |    `dim_66`      String, 
           |    `dim_67`      String, 
           |    `dim_68`      String, 
           |    `dim_69`      String, 
           |    `dim_70`      String, 
           |    `dim_71`      String, 
           |    `dim_72`      String, 
           |    `dim_73`      String, 
           |    `dim_74`      String, 
           |    `dim_75`      String, 
           |    `dim_76`      String, 
           |    `dim_77`      String, 
           |    `dim_78`      String, 
           |    `dim_79`      String, 
           |    `dim_80`      String, 
           |    `dim_81`      String, 
           |    `dim_82`      String, 
           |    `dim_83`      String, 
           |    `dim_84`      String, 
           |    `dim_85`      String, 
           |    `dim_86`      String, 
           |    `dim_87`      String, 
           |    `dim_88`      String, 
           |    `dim_89`      String, 
           |    `dim_90`      String, 
           |    `dim_91`      String, 
           |    `dim_92`      String, 
           |    `dim_93`      String, 
           |    `dim_94`      String, 
           |    `dim_95`      String, 
           |    `dim_96`      String, 
           |    `dim_97`      String, 
           |    `dim_98`      String, 
           |    `dim_99`      String, 
           |    `dim_100`     String, 
           |    `counter_1`   double,
           |    `counter_2`   double,
           |    `counter_3`   double,
           |    `counter_4`   double,
           |    `counter_5`   double,
           |    `counter_6`   double,
           |    `counter_7`   double,
           |    `counter_8`   double,
           |    `counter_9`   double,
           |    `counter_10`  double,
           |    `counter_11`  double,
           |    `counter_12`  double,
           |    `counter_13`  double,
           |    `counter_14`  double,
           |    `counter_15`  double,
           |    `counter_16`  double,
           |    `counter_17`  double,
           |    `counter_18`  double,
           |    `counter_19`  double,
           |    `counter_20`  double,
           |    `counter_21`  double,
           |    `counter_22`  double,
           |    `counter_23`  double,
           |    `counter_24`  double,
           |    `counter_25`  double,
           |    `counter_26`  double,
           |    `counter_27`  double,
           |    `counter_28`  double,
           |    `counter_29`  double,
           |    `counter_30`  double,
           |    `counter_31`  double,
           |    `counter_32`  double,
           |    `counter_33`  double,
           |    `counter_34`  double,
           |    `counter_35`  double,
           |    `counter_36`  double,
           |    `counter_37`  double,
           |    `counter_38`  double,
           |    `counter_39`  double,
           |    `counter_40`  double,
           |    `counter_41`  double,
           |    `counter_42`  double,
           |    `counter_43`  double,
           |    `counter_44`  double,
           |    `counter_45`  double,
           |    `counter_46`  double,
           |    `counter_47`  double,
           |    `counter_48`  double,
           |    `counter_49`  double,
           |    `counter_50`  double,
           |    `counter_51`  double,
           |    `counter_52`  double,
           |    `counter_53`  double,
           |    `counter_54`  double,
           |    `counter_55`  double,
           |    `counter_56`  double,
           |    `counter_57`  double,
           |    `counter_58`  double,
           |    `counter_59`  double,
           |    `counter_60`  double,
           |    `counter_61`  double,
           |    `counter_62`  double,
           |    `counter_63`  double,
           |    `counter_64`  double,
           |    `counter_65`  double,
           |    `counter_66`  double,
           |    `counter_67`  double,
           |    `counter_68`  double,
           |    `counter_69`  double,
           |    `counter_70`  double,
           |    `counter_71`  double,
           |    `counter_72`  double,
           |    `counter_73`  double,
           |    `counter_74`  double,
           |    `counter_75`  double,
           |    `counter_76`  double,
           |    `counter_77`  double,
           |    `counter_78`  double,
           |    `counter_79`  double,
           |    `counter_80`  double,
           |    `counter_81`  double,
           |    `counter_82`  double,
           |    `counter_83`  double,
           |    `counter_84`  double,
           |    `counter_85`  double,
           |    `counter_86`  double,
           |    `counter_87`  double,
           |    `counter_88`  double,
           |    `counter_89`  double,
           |    `counter_90`  double,
           |    `counter_91`  double,
           |    `counter_92`  double,
           |    `counter_93`  double,
           |    `counter_94`  double,
           |    `counter_95`  double,
           |    `counter_96`  double,
           |    `counter_97`  double,
           |    `counter_98`  double,
           |    `counter_99`  double,
           |    `counter_100` double, 
           |    `batchno`     double
           |) 
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
           |STORED AS TEXTFILE
        """.stripMargin.trim.cmd,
        s"""LOAD DATA LOCAL INPATH '${quoteHiveFile("data/files/sdr_dyn_seq_custer_iot_all_hour_60min.dat")}' INTO TABLE SDR_DYN_SEQ_CUSTER_IOT_ALL_HOUR_60MIN""".cmd),
      TestTable("dim_apn_iot",
        s"""
           |CREATE TABLE dim_apn_iot
           |(
           |    `city_ascription`     String,
           |    `industry`            String,
           |    `apn_name`            String,
           |    `service_level`       String,
           |    `customer_name`       String,
           |    `id`                  bigint
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
           |STORED AS TEXTFILE
        """.stripMargin.trim.cmd,
        s"""LOAD DATA LOCAL INPATH '${quoteHiveFile("data/files/DIM_APN_IOT.dat")}' INTO TABLE DIM_APN_IOT""".cmd),
      TestTable("tradeflow_all",
        s"""
           |CREATE TABLE tradeflow_all 
           |USING parquet
           |OPTIONS (
           |  path '${quoteHiveFile("data/files/d944ec3565673beb-78c28edfa577c286_1251401173_data.0.parq")}'
           |)
         """.stripMargin.trim.cmd),
      TestTable("country",
        s"""
           |CREATE TABLE country 
           |USING parquet
           |OPTIONS (
           |  path '${quoteHiveFile("data/files/944b2c99c0758b2-d483027fae4f4b9f_1568698772_data.0.parq")}' 
           |)
        """.stripMargin.trim.cmd),
      TestTable("updatetime",
        s"""
           |CREATE TABLE updatetime
           |USING parquet
           |OPTIONS (
           |  path '${quoteHiveFile("data/files/4496693263a4ee6-1724efa8090227b0_1233867673_data.0.parq")}'
           |)
        """.stripMargin.trim.cmd),
      TestTable("country_general",
        s"""
           |CREATE TABLE country_general 
           |USING parquet
           |OPTIONS (
           |  path '${quoteHiveFile("data/files/44bdf9f8af7c55e-990a02877791fb85_1630871489_data.0.parq")}' 
           |)
        """.stripMargin.trim.cmd),
      TestTable("hs246",
        s"""
           |CREATE TABLE hs246 
           |USING parquet
           |OPTIONS (
           |  path '${quoteHiveFile("data/files/1c457b55c8c10df1-5c81abd56c05e598_1733531031_data.0.parq")}' 
           |)
        """.stripMargin.trim.cmd)
//      TestTable("tradeflow_all",
//        s"""
//           |CREATE TABLE tradeflow_all (
//           | m_month      smallint,
//           | hs_code      string  ,
//           | country      smallint,
//           | dollar_value double  ,
//           | quantity     double  ,
//           | unit         smallint,
//           | b_country    smallint,
//           | imex         smallint,
//           | y_year       smallint) 
//           | STORED AS parquet
//        """.stripMargin.trim.cmd,
//        s"""LOAD DATA LOCAL INPATH '${quoteHiveFile("data/files/d944ec3565673beb-78c28edfa577c286_1251401173_data.0.parq")}' INTO TRADEFLOW_ALL""".cmd),
//      TestTable("country",
//        s"""
//           |CREATE TABLE country (
//           | countryid   smallint ,
//           | country_en  string   ,
//           | country_cn  string   ) 
//           | STORED AS parquet
//        """.stripMargin.trim.cmd,
//        s"""LOAD DATA LOCAL INPATH '${quoteHiveFile("data/files/944b2c99c0758b2-d483027fae4f4b9f_1568698772_data.0.parq")}' INTO COUNTRY""".cmd),
//      TestTable("updatetime",
//        s"""
//           |CREATE TABLE updatetime (
//           | countryid     smallint ,
//           | imex          smallint ,
//           | hs_len        smallint ,
//           | minstartdate  string   ,
//           | startdate     string   ,
//           | newdate       string   ,
//           | minnewdate    string   )
//           | STORED AS parquet
//        """.stripMargin.trim.cmd,
//        s"""LOAD DATA LOCAL INPATH '${quoteHiveFile("data/files/4496693263a4ee6-1724efa8090227b0_1233867673_data.0.parq")}' INTO UPDATETIME""".cmd),
//      TestTable("hs246",
//        s"""
//           |CREATE TABLE hs246 (
//           | id     bigint ,
//           | hs     string ,
//           | hs_cn  string ,
//           | hs_en  string )
//           | STORED AS parquet
//        """.stripMargin.trim.cmd,
//        s"""LOAD DATA LOCAL INPATH '${quoteHiveFile("data/files/1c457b55c8c10df1-5c81abd56c05e598_1733531031_data.0.parq")}' INTO UPDATETIME""".cmd)
    )

    hiveQTestUtilTables.foreach(registerTestTable)
  }

  private val loadedTables = new collection.mutable.HashSet[String]

  def loadTestTable(name: String) {
    if (!(loadedTables contains name)) {
      // Marks the table as loaded first to prevent infinite mutually recursive table loading.
      loadedTables += name
      logDebug(s"Loading test table $name")
      val createCmds =
        testTables.get(name).map(_.commands).getOrElse(sys.error(s"Unknown test table $name"))
      createCmds.foreach(_())

      if (cacheTables) {
        new SQLContext(self).cacheTable(name)
      }
    }
  }

  /**
   * Records the UDFs present when the server starts, so we can delete ones that are created by
   * tests.
   */
  protected val originalUDFs: JavaSet[String] = FunctionRegistry.getFunctionNames

  /**
   * Resets the test instance by deleting any tables that have been created.
   * TODO: also clear out UDFs, views, etc.
   */
  def reset() {
    try {
      // HACK: Hive is too noisy by default.
      org.apache.log4j.LogManager.getCurrentLoggers.asScala.foreach { log =>
        val logger = log.asInstanceOf[org.apache.log4j.Logger]
        if (!logger.getName.contains("org.apache.spark")) {
          logger.setLevel(org.apache.log4j.Level.WARN)
        }
      }

      sharedState.cacheManager.clearCache()
      loadedTables.clear()
      sessionState.catalog.reset()
      metadataHive.reset()

      // HDFS root scratch dir requires the write all (733) permission. For each connecting user,
      // an HDFS scratch dir: ${hive.exec.scratchdir}/<username> is created, with
      // ${hive.scratch.dir.permission}. To resolve the permission issue, the simplest way is to
      // delete it. Later, it will be re-created with the right permission.
      val location = new Path(sc.hadoopConfiguration.get(ConfVars.SCRATCHDIR.varname))
      val fs = location.getFileSystem(sc.hadoopConfiguration)
      fs.delete(location, true)

      // Some tests corrupt this value on purpose, which breaks the RESET call below.
      sessionState.conf.setConfString("fs.defaultFS", new File(".").toURI.toString)
      // It is important that we RESET first as broken hooks that might have been set could break
      // other sql exec here.
      metadataHive.runSqlHive("RESET")
      // For some reason, RESET does not reset the following variables...
      // https://issues.apache.org/jira/browse/HIVE-9004
      metadataHive.runSqlHive("set hive.table.parameters.default=")
      metadataHive.runSqlHive("set datanucleus.cache.collections=true")
      metadataHive.runSqlHive("set datanucleus.cache.collections.lazy=true")
      // Lots of tests fail if we do not change the partition whitelist from the default.
      metadataHive.runSqlHive("set hive.metastore.partition.name.whitelist.pattern=.*")

      sessionState.catalog.setCurrentDatabase("default")
    } catch {
      case e: Exception =>
        logError("FATAL ERROR: Failed to reset TestDB state.", e)
    }
  }

}


private[hive] class TestHiveQueryExecution(
    sparkSession: TestHiveSparkSession,
    logicalPlan: LogicalPlan)
  extends QueryExecution(sparkSession, logicalPlan) with Logging {

  def this(sparkSession: TestHiveSparkSession, sql: String) {
    this(sparkSession, sparkSession.sessionState.sqlParser.parsePlan(sql))
  }

  def this(sql: String) {
    this(TestHive.sparkSession, sql)
  }

  override lazy val analyzed: LogicalPlan = {
    val describedTables = logical match {
      case CacheTableCommand(tbl, _, _) => tbl.table :: Nil
      case _ => Nil
    }

    // Make sure any test tables referenced are loaded.
    val referencedTables =
      describedTables ++
        logical.collect { case UnresolvedRelation(tableIdent) => tableIdent.table }
    val referencedTestTables = referencedTables.filter(sparkSession.testTables.contains)
    logDebug(s"Query references test tables: ${referencedTestTables.mkString(", ")}")
    referencedTestTables.foreach(sparkSession.loadTestTable)
    // Proceed with analysis.
    sparkSession.sessionState.analyzer.execute(logical)
  }
}


private[hive] object TestHiveContext {

  /**
   * A map used to store all confs that need to be overridden in sql/hive unit tests.
   */
  val overrideConfs: Map[String, String] =
    Map(
      // Fewer shuffle partitions to speed up testing.
      SQLConf.SHUFFLE_PARTITIONS.key -> "5"
    )

  def makeWarehouseDir(): File = {
    val warehouseDir = Utils.createTempDir(namePrefix = "warehouse")
    warehouseDir.delete()
    warehouseDir
  }

  def makeScratchDir(): File = {
    val scratchDir = Utils.createTempDir(namePrefix = "scratch")
    scratchDir.delete()
    scratchDir
  }

}

private[hive] class TestHiveSessionStateBuilder(
    session: SparkSession,
    state: Option[SessionState])
  extends HiveSessionStateBuilder(session, state)
  with WithTestConf {

  override def overrideConfs: Map[String, String] = TestHiveContext.overrideConfs

  override def createQueryExecution: (LogicalPlan) => QueryExecution = { plan =>
    new TestHiveQueryExecution(session.asInstanceOf[TestHiveSparkSession], plan)
  }

  override protected def newBuilder: NewBuilder = new TestHiveSessionStateBuilder(_, _)
}
