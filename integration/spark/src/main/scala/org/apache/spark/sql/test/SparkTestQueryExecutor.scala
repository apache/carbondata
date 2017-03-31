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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{CarbonContext, DataFrame, SQLContext}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * This class is a sql executor of unit test case for spark version 1.x.
 */

class SparkTestQueryExecutor extends TestQueryExecutorRegister {
  override def sql(sqlText: String): DataFrame = SparkTestQueryExecutor.cc.sql(sqlText)

  override def sqlContext: SQLContext = SparkTestQueryExecutor.cc
}

object SparkTestQueryExecutor {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  LOGGER.info("use TestQueryExecutorImplV1")
  CarbonProperties.getInstance()
    .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, TestQueryExecutor.timestampFormat)
    .addProperty(CarbonCommonConstants.STORE_LOCATION_TEMP_PATH,
      System.getProperty("java.io.tmpdir"))
    .addProperty(CarbonCommonConstants.LOCK_TYPE, CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL)

  val sc = new SparkContext(new SparkConf()
    .setAppName("CarbonSpark")
    .setMaster("local[2]")
    .set("spark.sql.shuffle.partitions", "20"))
  sc.setLogLevel("ERROR")

  val cc = new CarbonContext(sc, TestQueryExecutor.storeLocation, TestQueryExecutor.metastoredb)
}
