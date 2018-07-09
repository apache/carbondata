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

package org.apache.carbondata.store

import java.net.InetAddress

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.annotations.InterfaceAudience
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.util.Util
import org.apache.carbondata.store.api.CarbonStoreFactory
import org.apache.carbondata.store.api.conf.StoreConf
import org.apache.carbondata.store.impl.worker.Worker

/**
 * A CarbonStore implementation that uses Spark as underlying compute engine
 * with CarbonData query optimization capability
 */
@InterfaceAudience.Internal
object WorkerManager {
  private final val LOG = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  // TODO: how to ensure task is sent to every executor?
  def startAllWorker(session: SparkSession, storeConf: StoreConf): Unit = {
    val numExecutors = session.sparkContext.getExecutorMemoryStatus.keySet.size
    LOG.info("Starting search mode master")
    val rows = session.sparkContext.parallelize(1 to numExecutors * 10, numExecutors)
      .mapPartitions { f =>
        // start worker
        var storeLocation: String = null
        val carbonUseLocalDir = CarbonProperties.getInstance()
          .getProperty("carbon.use.local.dir", "false")
        if (carbonUseLocalDir.equalsIgnoreCase("true")) {
          val storeLocations = Util.getConfiguredLocalDirs(SparkEnv.get.conf)
          if (null != storeLocations && storeLocations.nonEmpty) {
            storeLocation = storeLocations.mkString(",")
          }
          if (storeLocation == null) {
            storeLocation = System.getProperty("java.io.tmpdir")
          }
        } else {
          storeLocation = System.getProperty("java.io.tmpdir")
        }
        storeConf.conf(StoreConf.STORE_TEMP_LOCATION, storeLocation)

        val worker = new Worker(storeConf)
        worker.start()
        new Iterator[Int] {
          override def hasNext: Boolean = false

          override def next(): Int = 1
        }
      }.collect()
    LOG.info(s"Tried to start $numExecutors workers, $rows workers started successfully")
  }

}
