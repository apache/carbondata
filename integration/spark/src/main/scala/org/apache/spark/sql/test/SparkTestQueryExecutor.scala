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

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.carbondata.execution.datasources.CarbonFileIndexReplaceRule
import org.apache.spark.sql.test.TestQueryExecutor.{hdfsUrl, integrationPath, warehouse}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties

/**
 * This class is a sql executor of unit test case for spark version 2.x.
 */

class SparkTestQueryExecutor extends TestQueryExecutorRegister {

  override def sql(sqlText: String): DataFrame = SparkTestQueryExecutor.spark.sql(sqlText)

  override def sqlContext: SQLContext = SparkTestQueryExecutor.spark.sqlContext

  override def stop(): Unit = SparkTestQueryExecutor.spark.stop()
}

object SparkTestQueryExecutor {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  LOGGER.info("use TestQueryExecutorImpl")
  val conf = new SparkConf()
  if (!TestQueryExecutor.masterUrl.startsWith("local")) {
    conf.setJars(TestQueryExecutor.jars).
      set("spark.driver.memory", "14g").
      set("spark.executor.memory", "8g").
      set("spark.executor.cores", "2").
      set("spark.executor.instances", "2").
      set("spark.cores.max", "4")
    FileFactory.getConfiguration.
      set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
  }

  if (System.getProperty("spark.hadoop.hive.metastore.uris") != null) {
    conf.set("spark.hadoop.hive.metastore.uris",
      System.getProperty("spark.hadoop.hive.metastore.uris"))
  }
  val extensions = CarbonProperties
    .getInstance()
    .getProperty("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
  val spark = SparkSession
    .builder()
    .config(conf)
    .master(TestQueryExecutor.masterUrl)
    .appName("SparkTestQueryExecutor")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", warehouse)
    .config("spark.sql.crossJoin.enabled", "true")
    .config("spark.sql.extensions", extensions)
    .getOrCreate()
  spark.experimental.extraOptimizations = Seq(new CarbonFileIndexReplaceRule)
  CarbonEnv.getInstance(spark)
  if (warehouse.startsWith("hdfs://")) {
    System.setProperty(CarbonCommonConstants.HDFS_TEMP_LOCATION, warehouse)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOCK_TYPE,
      CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS)
    ResourceRegisterAndCopier.
      copyResourcesifNotExists(hdfsUrl, s"$integrationPath/spark/src/test/resources",
        s"$integrationPath//spark-common-cluster-test/src/test/resources/testdatafileslist.txt")
  }
  FileFactory.getConfiguration.
    set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
  spark.sparkContext.setLogLevel("ERROR")
}
