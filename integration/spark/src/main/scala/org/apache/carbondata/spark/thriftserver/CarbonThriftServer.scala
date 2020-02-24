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

package org.apache.carbondata.spark.thriftserver

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.slf4j.{Logger, LoggerFactory}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.util.CarbonSparkUtil

/**
 * CarbonThriftServer support different modes:
 * 1. read/write data from/to HDFS or local, no parameter is required in this case
 * 2. read/write data from/to S3, it needs provide access-key, secret-key, s3-endpoint
 */
object CarbonThriftServer {

  def main(args: Array[String]): Unit = {
    if (args.length != 0 && args.length != 3) {
      val logger: Logger = LoggerFactory.getLogger(this.getClass)
      logger.error("parameters: [access-key] [secret-key] [s3-endpoint]")
      System.exit(0)
    }
    val sparkConf = new SparkConf(loadDefaults = true)
    val builder = SparkSession
      .builder()
      .config(sparkConf)
      .appName("Carbon Thrift Server(uses CarbonExtensions)")
      .enableHiveSupport()
      .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
    configPropertiesFile(sparkConf, builder)
    if (args.length == 3) {
      builder.config(CarbonSparkUtil.getSparkConfForS3(args(0), args(1), args(2)))
    }
    val spark = builder.getOrCreate()
    CarbonEnv.getInstance(spark)
    waitingForSparkLaunch()
    HiveThriftServer2.startWithContext(spark.sqlContext)
  }

  private def waitingForSparkLaunch(): Unit = {
    val warmUpTime = CarbonProperties.getInstance().getProperty("carbon.spark.warmUpTime", "5000")
    try {
      Thread.sleep(Integer.parseInt(warmUpTime))
    } catch {
      case e: Exception =>
        val LOG = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
        LOG.error(s"Wrong value for carbon.spark.warmUpTime $warmUpTime " +
                  "Using default Value and proceeding")
        Thread.sleep(5000)
    }
  }

  private def configPropertiesFile(sparkConf: SparkConf, builder: SparkSession.Builder): Unit = {
    sparkConf.contains("carbon.properties.filepath") match {
      case false =>
        val sparkHome = System.getenv.get("SPARK_HOME")
        if (null != sparkHome) {
          val file = new File(sparkHome + '/' + "conf" + '/' + "carbon.properties")
          if (file.exists()) {
            builder.config("carbon.properties.filepath", file.getCanonicalPath)
            System.setProperty("carbon.properties.filepath", file.getCanonicalPath)
          }
        }
      case true =>
        System.setProperty(
          "carbon.properties.filepath", sparkConf.get("carbon.properties.filepath"))
    }
  }
}
