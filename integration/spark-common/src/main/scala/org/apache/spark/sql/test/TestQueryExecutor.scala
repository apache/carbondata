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
import java.util.ServiceLoader

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.util.Utils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties

/**
 * the sql executor of spark-common-test
 */
trait TestQueryExecutorRegister {
  def sql(sqlText: String): DataFrame

  def stop()

  def sqlContext: SQLContext
}

object TestQueryExecutor {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  val projectPath = new File(this.getClass.getResource("/").getPath + "../../../..")
    .getCanonicalPath
  LOGGER.info(s"project path: $projectPath")
  val integrationPath = s"$projectPath/integration"
  val metastoredb = s"$integrationPath/spark-common/target"
  val masterUrl = {
    val property = System.getProperty("spark.master.url")
    if (property == null) {
      "local[2]"
    } else {
      property
    }
  }

  val hdfsUrl = {
    val property = System.getProperty("hdfs.url")
    if (property == null) {
      "local"
    } else {
      LOGGER.info("HDFS PATH given : " + property)
      property
    }
  }

  val resourcesPath = if (hdfsUrl.startsWith("hdfs://")) {
    hdfsUrl
  } else {
    s"$integrationPath/spark-common-test/src/test/resources"
  }

  val hadoopConf = new Configuration()

  val storeLocation = if (hdfsUrl.startsWith("hdfs://")) {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOCK_TYPE,
      CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS)
    val carbonFile = FileFactory.getCarbonFile(hadoopConf, s"$hdfsUrl/store",
      FileFactory.getFileType(s"$hdfsUrl/store"))
    FileFactory.deleteAllCarbonFilesOfDir(carbonFile)
    s"$hdfsUrl/store_" + System.nanoTime()
  } else {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOCK_TYPE,
      CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL)
    s"$integrationPath/spark-common/target/store"
  }
  val warehouse = if (hdfsUrl.startsWith("hdfs://")) {
    val carbonFile = FileFactory.getCarbonFile(hadoopConf, s"$hdfsUrl/warehouse",
      FileFactory.getFileType(s"$hdfsUrl/warehouse"))
    FileFactory.deleteAllCarbonFilesOfDir(carbonFile)
    s"$hdfsUrl/warehouse_" + System.nanoTime()
  } else {
    s"$integrationPath/spark-common/target/warehouse"
  }

  val hiveresultpath = if (hdfsUrl.startsWith("hdfs://")) {
    val p = s"$hdfsUrl/hiveresultpath"
    FileFactory.mkdirs(hadoopConf, p, FileFactory.getFileType(p))
    p
  } else {
    val p = s"$integrationPath/spark-common/target/hiveresultpath"
    new File(p).mkdirs()
    p
  }

  LOGGER.info(s"""Store path taken $storeLocation""")
  LOGGER.info(s"""Warehouse path taken $warehouse""")
  LOGGER.info(s"""Resource path taken $resourcesPath""")

  lazy val modules = Seq(TestQueryExecutor.projectPath + "/common/target",
    TestQueryExecutor.projectPath + "/core/target",
    TestQueryExecutor.projectPath + "/hadoop/target",
    TestQueryExecutor.projectPath + "/processing/target",
    TestQueryExecutor.projectPath + "/integration/spark-common/target",
    TestQueryExecutor.projectPath + "/integration/spark2/target",
    TestQueryExecutor.projectPath + "/integration/spark-common/target/jars")
  lazy val jars = {
    val jarsLocal = new ArrayBuffer[String]()
    modules.foreach { path =>
      val files = new File(path).listFiles(new FilenameFilter {
        override def accept(dir: File, name: String) = {
          name.endsWith(".jar")
        }
      })
      files.foreach(jarsLocal += _.getAbsolutePath)
    }
    jarsLocal
  }

  val INSTANCE = lookupQueryExecutor.newInstance().asInstanceOf[TestQueryExecutorRegister]
  CarbonProperties.getInstance()
    .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FORCE")
    .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC, "/tmp/carbon/badrecords")
    .addProperty(CarbonCommonConstants.DICTIONARY_SERVER_PORT,
      (CarbonCommonConstants.DICTIONARY_SERVER_PORT_DEFAULT.toInt + Random.nextInt(100)) + "")
    .addProperty(CarbonCommonConstants.CARBON_MAX_DRIVER_LRU_CACHE_SIZE, "1024")
      .addProperty(CarbonCommonConstants.CARBON_MAX_EXECUTOR_LRU_CACHE_SIZE, "1024")

  private def lookupQueryExecutor: Class[_] = {
    ServiceLoader.load(classOf[TestQueryExecutorRegister], Utils.getContextOrSparkClassLoader)
      .iterator().next().getClass
  }

}
