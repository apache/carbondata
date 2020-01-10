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

package org.apache.spark.sql.hive.cli

import org.apache.spark.SparkContext
import org.apache.spark.sql.{CarbonEnv, SparkSession, SQLContext}
import org.apache.spark.sql.hive.thriftserver.{SparkSQLCLIDriver, SparkSQLEnv}

import org.apache.carbondata.common.logging.LogServiceFactory

object CarbonSQLCLIDriver {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  var hiveContext: SQLContext = _
  var sparkContext: SparkContext = _

  def main(args: Array[String]): Unit = {
    init()
    SparkSQLEnv.sparkContext = sparkContext
    SparkSQLEnv.sqlContext = hiveContext
    SparkSQLCLIDriver.installSignalHandler()
    SparkSQLCLIDriver.main(args)
  }

  def init() {
    if (hiveContext == null) {
      val storePath = System.getenv("CARBON_HOME") + "/bin/carbonsqlclistore"
      val warehouse = System.getenv("CARBON_HOME") + "/warehouse"
      val carbon = SparkSession
          .builder()
          .master(System.getProperty("spark.master"))
          .appName("CarbonSQLCLIDriver")
          .config("spark.sql.warehouse.dir", warehouse)
          .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
          .getOrCreate()
      CarbonEnv.getInstance(carbon)

      hiveContext = carbon.sqlContext
      hiveContext.conf.getAllConfs.toSeq.sorted.foreach { case (k, v) =>
        LOGGER.debug(s"HiveConf var: $k=$v")
      }
      sparkContext = carbon.sqlContext.sparkContext

    }
  }

}
