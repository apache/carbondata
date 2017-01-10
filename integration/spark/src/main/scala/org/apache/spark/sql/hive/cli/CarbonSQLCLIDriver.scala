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

import java.io.File

import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler.StatsReportListener
import org.apache.spark.sql.CarbonContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.{SparkSQLCLIDriver, SparkSQLEnv}
import org.apache.spark.util.Utils

import org.apache.carbondata.common.logging.LogServiceFactory

object CarbonSQLCLIDriver {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  var hiveContext: HiveContext = _
  var sparkContext: SparkContext = _

  def main(args: Array[String]): Unit = {
    init()
    SparkSQLEnv.sparkContext = sparkContext
    SparkSQLEnv.hiveContext = hiveContext
    SparkSQLCLIDriver.installSignalHandler()
    SparkSQLCLIDriver.main(args)
  }

  def init() {
    if (hiveContext == null) {
      val sparkConf = new SparkConf(loadDefaults = true)
      val maybeSerializer = sparkConf.getOption("spark.serializer")
      val maybeKryoReferenceTracking = sparkConf.getOption("spark.kryo.referenceTracking")
      // If user doesn't specify the appName, we want to get [SparkSQL::localHostName] instead of
      // the default appName [CarbonSQLCLIDriver] in cli or beeline.
      val maybeAppName = sparkConf
        .getOption("spark.app.name")
        .filterNot(_ == classOf[SparkSQLCLIDriver].getName)
      val maybeStorePath = sparkConf.getOption("spark.carbon.storepath")

      sparkConf
        .setAppName(maybeAppName.getOrElse(s"CarbonSparkSQL::${ Utils.localHostName() }"))
        .set(
          "spark.serializer",
          maybeSerializer.getOrElse("org.apache.spark.serializer.KryoSerializer"))
        .set(
          "spark.kryo.referenceTracking",
          maybeKryoReferenceTracking.getOrElse("false"))

      sparkContext = new SparkContext(sparkConf)
      sparkContext.addSparkListener(new StatsReportListener())
      val path = System.getenv("CARBON_HOME") + "/bin/carbonsqlclistore"
      val store = new File(path)
      store.mkdirs()
      hiveContext = new CarbonContext(sparkContext,
        maybeStorePath.getOrElse(store.getCanonicalPath),
        store.getCanonicalPath)

      hiveContext.setConf("spark.sql.hive.version", HiveContext.hiveExecutionVersion)
      hiveContext.hiveconf.getAllProperties.asScala.toSeq.sorted.foreach { case (k, v) =>
        LOGGER.debug(s"HiveConf var: $k=$v")
      }
    }
  }

}
