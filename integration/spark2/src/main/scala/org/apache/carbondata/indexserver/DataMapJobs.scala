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
package org.apache.carbondata.indexserver

import java.util

import scala.collection.JavaConverters._

import org.apache.log4j.Logger
import org.apache.spark.util.SizeEstimator

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.{AbstractDataMapJob, DistributableDataMapFormat}
import org.apache.carbondata.core.indexstore.ExtendedBlocklet
import org.apache.carbondata.spark.util.CarbonScalaUtil.logTime

/**
 * Spark job to execute datamap job and prune all the datamaps distributable. This job will prune
 * and cache the appropriate datamaps in executor LRUCache.
 */
class DistributedDataMapJob extends AbstractDataMapJob {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def execute(dataMapFormat: DistributableDataMapFormat): util.List[ExtendedBlocklet] = {
    if (LOGGER.isDebugEnabled) {
      val messageSize = SizeEstimator.estimate(dataMapFormat)
      LOGGER.debug(s"Size of message sent to Index Server: $messageSize")
    }
    val (resonse, time) = logTime {
      IndexServer.getClient.getSplits(dataMapFormat).toList.asJava
    }
    LOGGER.info(s"Time taken to get response from server: $time ms")
    resonse
  }
}

/**
 * Spark job to execute datamap job and prune all the datamaps distributable. This job will just
 * prune the datamaps but will not cache in executors.
 */
class EmbeddedDataMapJob extends AbstractDataMapJob {

  override def execute(dataMapFormat: DistributableDataMapFormat): util.List[ExtendedBlocklet] = {
    IndexServer.getSplits(dataMapFormat).toList.asJava
  }

}
