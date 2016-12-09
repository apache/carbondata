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

import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.sql.hive.CarbonMetastore

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.rdd.SparkCommonEnv
import org.apache.carbondata.spark.readsupport.SparkRowReadSupportImpl

/**
 * Carbon Environment for unified context
 */
case class CarbonEnv(carbonMetastore: CarbonMetastore)

object CarbonEnv {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  @volatile private var carbonEnv: CarbonEnv = _

  var initialized = false

  def init(sqlContext: SQLContext): Unit = {
    if (!initialized) {
      val catalog = {
        val storePath = CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION)
        LOGGER.info(s"carbon env initial: $storePath")
        new CarbonMetastore(sqlContext.sparkSession.conf, storePath)
      }
      carbonEnv = CarbonEnv(catalog)
      setSparkCommonEnv(sqlContext)
      initialized = true
    }
  }

  def get: CarbonEnv = {
    carbonEnv
  }

  private def setSparkCommonEnv(sqlContext: SQLContext): Unit = {
    SparkCommonEnv.readSupportClass = classOf[SparkRowReadSupportImpl]
    SparkCommonEnv.numExistingExecutors = sqlContext.sparkContext.schedulerBackend match {
      case b: CoarseGrainedSchedulerBackend => b.getExecutorIds().length
      case _ => 0
    }
  }
}


