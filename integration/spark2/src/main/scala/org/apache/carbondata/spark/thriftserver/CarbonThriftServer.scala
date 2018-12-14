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

import org.apache.hadoop.fs.s3a.Constants.{ACCESS_KEY, ENDPOINT, SECRET_KEY}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.slf4j.{Logger, LoggerFactory}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * CarbonThriftServer support different modes:
 * 1. read/write data from/to HDFS or local,it only needs configurate storePath
 * 2. read/write data from/to S3, it needs provide access-key, secret-key, s3-endpoint
 */
object CarbonThriftServer {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.CarbonSession._

    val sparkConf = new SparkConf(loadDefaults = true)

    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    if (args.length != 0 && args.length != 1 && args.length != 4) {
      logger.error("parameters: storePath [access-key] [secret-key] [s3-endpoint]")
      System.exit(0)
    }

    val builder = SparkSession
      .builder()
      .config(sparkConf)
      .appName("Carbon Thrift Server(uses CarbonSession)")
      .enableHiveSupport()

    if (!sparkConf.contains("carbon.properties.filepath")) {
      val sparkHome = System.getenv.get("SPARK_HOME")
      if (null != sparkHome) {
        val file = new File(sparkHome + '/' + "conf" + '/' + "carbon.properties")
        if (file.exists()) {
          builder.config("carbon.properties.filepath", file.getCanonicalPath)
          System.setProperty("carbon.properties.filepath", file.getCanonicalPath)
        }
      }
    } else {
      System.setProperty("carbon.properties.filepath", sparkConf.get("carbon.properties.filepath"))
    }

    val storePath = if (args.length > 0) args.head else null

    val spark = if (args.length <= 1) {
      builder.getOrCreateCarbonSession(storePath)
    } else {
      val (accessKey, secretKey, endpoint) = getKeyOnPrefix(args(0))
      builder.config(accessKey, args(1))
        .config(secretKey, args(2))
        .config(endpoint, getS3EndPoint(args))
        .getOrCreateCarbonSession(storePath)
    }

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

    HiveThriftServer2.startWithContext(spark.sqlContext)
  }

  def getKeyOnPrefix(path: String): (String, String, String) = {
    val endPoint = "spark.hadoop." + ENDPOINT
    if (path.startsWith(CarbonCommonConstants.S3A_PREFIX)) {
      ("spark.hadoop." + ACCESS_KEY, "spark.hadoop." + SECRET_KEY, endPoint)
    } else if (path.startsWith(CarbonCommonConstants.S3N_PREFIX)) {
      ("spark.hadoop." + CarbonCommonConstants.S3N_ACCESS_KEY,
        "spark.hadoop." + CarbonCommonConstants.S3N_SECRET_KEY, endPoint)
    } else if (path.startsWith(CarbonCommonConstants.S3_PREFIX)) {
      ("spark.hadoop." + CarbonCommonConstants.S3_ACCESS_KEY,
        "spark.hadoop." + CarbonCommonConstants.S3_SECRET_KEY, endPoint)
    } else {
      throw new Exception("Incorrect Store Path")
    }
  }

  def getS3EndPoint(args: Array[String]): String = {
    if (args.length >= 4 && args(3).contains(".com")) args(3)
    else ""
  }

}
