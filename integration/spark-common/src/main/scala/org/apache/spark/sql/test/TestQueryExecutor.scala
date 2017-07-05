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
  val timestampFormat = "dd-MM-yyyy"
  val masterUrl = {
    val property = System.getProperty("spark.master.url")
    if (property == null) {
//      "spark://root1-ThinkPad-T440p:7077"
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
      println("HDFS PATH given : "+property)
      property
    }
  }

  val resourcesPath = if (hdfsUrl.startsWith("hdfs://")) {
    ResourceRegisterAndCopier.
      copyResourcesifNotExists(hdfsUrl, s"$integrationPath/spark-common-test/src/test/resources")
    hdfsUrl
  } else {
    s"$integrationPath/spark-common-test/src/test/resources"
  }

  val storeLocation = if (hdfsUrl.startsWith("hdfs://")) {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOCK_TYPE,
        CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS)
    val carbonFile = FileFactory.
      getCarbonFile(s"$hdfsUrl/store", FileFactory.getFileType(s"$hdfsUrl/store"))
    FileFactory.deleteAllCarbonFilesOfDir(carbonFile)
    s"$hdfsUrl/store"
  } else {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOCK_TYPE,
      CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL)
    s"$integrationPath/spark-common/target/store"
  }
  val warehouse = if (hdfsUrl.startsWith("hdfs://")) {
    val carbonFile = FileFactory.
      getCarbonFile(s"$hdfsUrl/warehouse", FileFactory.getFileType(s"$hdfsUrl/warehouse"))
    FileFactory.deleteAllCarbonFilesOfDir(carbonFile)
    s"$hdfsUrl/warehouse"
  } else {
    s"$integrationPath/spark-common/target/warehouse"
  }

  println(s"""Store path taken $storeLocation""")
  println(s"""Warehouse path taken $warehouse""")
  println(s"""Resource path taken $resourcesPath""")

  lazy val modules = Seq(TestQueryExecutor.projectPath+"/common/target",
    TestQueryExecutor.projectPath+"/core/target",
    TestQueryExecutor.projectPath+"/hadoop/target",
    TestQueryExecutor.projectPath+"/processing/target",
    TestQueryExecutor.projectPath+"/integration/spark-common/target",
    TestQueryExecutor.projectPath+"/integration/spark2/target",
    TestQueryExecutor.projectPath+"/integration/spark-common/target/jars")
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
  private def lookupQueryExecutor: Class[_] = {
    ServiceLoader.load(classOf[TestQueryExecutorRegister], Utils.getContextOrSparkClassLoader)
      .iterator().next().getClass
  }

}
