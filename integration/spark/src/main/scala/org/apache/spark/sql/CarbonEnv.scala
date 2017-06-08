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
import org.apache.spark.sql.hive.{CarbonIUDAnalysisRule, CarbonMetastore}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util. CarbonProperties
import org.apache.carbondata.hadoop.readsupport.impl.RawDataReadSupport
import org.apache.carbondata.spark.rdd.SparkReadSupport

case class CarbonEnv(carbonMetastore: CarbonMetastore)

object CarbonEnv {

  @volatile private var carbonEnv: CarbonEnv = _

  // set readsupport class global so that the executor can get it.
  SparkReadSupport.readSupportClass = classOf[RawDataReadSupport]

  var initialized = false

  def init(sqlContext: SQLContext): Unit = {
    if (!initialized) {
      val cc = sqlContext.asInstanceOf[CarbonContext]
      val catalog = new CarbonMetastore(cc, cc.storePath, cc.hiveClientInterface, "")
      carbonEnv = CarbonEnv(catalog)
      CarbonIUDAnalysisRule.init(sqlContext)
      initialized = true
      CarbonProperties.getInstance.addProperty(CarbonCommonConstants.IS_DRIVER_INSTANCE, "true")
    }
  }

  def get: CarbonEnv = {
    if (initialized) carbonEnv
    else throw new RuntimeException("CarbonEnv not initialized")
  }
}


