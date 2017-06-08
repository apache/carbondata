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

package org.apache.spark.sql

import java.util.Map
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.hive.{CarbonMetastore, CarbonSessionCatalog}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, SessionParams}
import org.apache.carbondata.spark.rdd.SparkReadSupport
import org.apache.carbondata.spark.readsupport.SparkRowReadSupportImpl

/**
 * Carbon Environment for unified context
 */
class CarbonEnv {

  var carbonMetastore: CarbonMetastore = _

  var sessionParams: SessionParams = _

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  // set readsupport class global so that the executor can get it.
  SparkReadSupport.readSupportClass = classOf[SparkRowReadSupportImpl]

  var initialized = false

  def init(sparkSession: SparkSession): Unit = {
    sparkSession.udf.register("getTupleId", () => "")
    if (!initialized) {
      sessionParams = new SessionParams()
      carbonMetastore = {
        val storePath =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION)
        LOGGER.info(s"carbon env initial: $storePath")
        new CarbonMetastore(sparkSession.conf, storePath, sessionParams)
      }
      CarbonProperties.getInstance.addProperty(CarbonCommonConstants.IS_DRIVER_INSTANCE, "true")
      initialized = true
    }
  }
}

object CarbonEnv {

  val carbonEnvMap: Map[SparkSession, CarbonEnv] =
    new ConcurrentHashMap[SparkSession, CarbonEnv]

  def getInstance(sparkSession: SparkSession): CarbonEnv = {
    if (sparkSession.isInstanceOf[CarbonSession]) {
      sparkSession.sessionState.catalog.asInstanceOf[CarbonSessionCatalog].carbonEnv
    } else {
      var carbonEnv: CarbonEnv = carbonEnvMap.get(sparkSession)
      if (carbonEnv == null) {
        carbonEnv = new CarbonEnv
        carbonEnv.init(sparkSession)
        carbonEnvMap.put(sparkSession, carbonEnv)
      }
      carbonEnv
    }
  }
}


