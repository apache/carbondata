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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.api.CarbonProperties
import org.apache.carbondata.core.constants.CarbonCommonConstants

object CarbonThriftServer {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.CarbonSession._

    val sparkConf = new SparkConf(loadDefaults = true)
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

    val spark = builder.getOrCreateCarbonSession(storePath)
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
