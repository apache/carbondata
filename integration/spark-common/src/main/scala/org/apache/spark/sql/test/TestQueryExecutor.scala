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

import java.io.File
import java.util.ServiceLoader

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.util.Utils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * the sql executor of spark-common-test
 */
trait TestQueryExecutorRegister {
  def sql(sqlText: String): DataFrame

  def sqlContext: SQLContext
}

object TestQueryExecutor {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  val projectPath = new File(this.getClass.getResource("/").getPath + "../../../..")
    .getCanonicalPath
  LOGGER.info(s"project path: $projectPath")
  val integrationPath = s"$projectPath/integration"
  val resourcesPath = s"$integrationPath/spark-common-test/src/test/resources"
  val storeLocation = s"$integrationPath/spark-common/target/store"
  val warehouse = s"$integrationPath/spark-common/target/warehouse"
  val metastoredb = s"$integrationPath/spark-common/target"
  val timestampFormat = "dd-MM-yyyy"

  val INSTANCE = lookupQueryExecutor.newInstance().asInstanceOf[TestQueryExecutorRegister]
  CarbonProperties.getInstance()
    .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FORCE")
  private def lookupQueryExecutor: Class[_] = {
    ServiceLoader.load(classOf[TestQueryExecutorRegister], Utils.getContextOrSparkClassLoader)
      .iterator().next().getClass
  }

}
