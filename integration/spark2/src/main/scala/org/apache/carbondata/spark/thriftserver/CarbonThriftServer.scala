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
  * 1. read/write data from/to HDFS or local,it only needs configurate storePath
  * 2. read/write data from/to S3, it needs provide access-key, secret-key, s3-endpoint
  */
object CarbonThriftServer {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.CarbonUtils._

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
      .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")

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
      builder.getOrCreate()
    } else {
      val (accessKey, secretKey, endpoint) = CarbonSparkUtil.getKeyOnPrefix(args(0))
      builder.config(accessKey, args(1))
        .config(secretKey, args(2))
        .config(endpoint, CarbonSparkUtil.getS3EndPoint(args))
        .getOrCreate()
    }
    CarbonEnv.getInstance(spark)

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

}
