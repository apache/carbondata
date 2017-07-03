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

package org.apache.spark.sql.test

import java.io.{File, FilenameFilter}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * This class is a sql executor of unit test case for spark version 2.x.
 */

class Spark2TestQueryExecutor extends TestQueryExecutorRegister {

  override def sql(sqlText: String): DataFrame = Spark2TestQueryExecutor.spark.sql(sqlText)

  override def sqlContext: SQLContext = Spark2TestQueryExecutor.spark.sqlContext
}

object Spark2TestQueryExecutor {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  LOGGER.info("use TestQueryExecutorImplV2")
  CarbonProperties.getInstance()
    .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, TestQueryExecutor.timestampFormat)
    .addProperty(CarbonCommonConstants.STORE_LOCATION_TEMP_PATH,
      System.getProperty("java.io.tmpdir"))
    .addProperty(CarbonCommonConstants.LOCK_TYPE, CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL)
    .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FORCE")



  import org.apache.spark.sql.CarbonSession._
  val modules = Seq(TestQueryExecutor.projectPath+"/common/target",
    TestQueryExecutor.projectPath+"/core/target",
    TestQueryExecutor.projectPath+"/hadoop/target",
    TestQueryExecutor.projectPath+"/processing/target",
    TestQueryExecutor.projectPath+"/integration/spark-common/target",
    TestQueryExecutor.projectPath+"/integration/spark2/target",
    TestQueryExecutor.projectPath+"/integration/spark-common/target/jars")
  val jars = new ArrayBuffer[String]()
  modules.foreach { path =>
    val files = new File(path).listFiles(new FilenameFilter {
      override def accept(dir: File, name: String) = {
        name.endsWith(".jar")
      }
    })
    files.foreach(jars += _.getAbsolutePath)
  }

  val conf = new SparkConf().setJars(jars).
    set("spark.driver.memory", "2g").
    set("spark.executor.memory","4g").
    set("spark.executor.cores", "4")

  val spark = SparkSession
    .builder().config(conf)
    .master(TestQueryExecutor.masterUrl)
    .appName("Spark2TestQueryExecutor")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", TestQueryExecutor.warehouse)
    .config("spark.sql.crossJoin.enabled", "true")
    .getOrCreateCarbonSession(null, TestQueryExecutor.metastoredb)
  spark.sparkContext.setLogLevel("ERROR")

}
