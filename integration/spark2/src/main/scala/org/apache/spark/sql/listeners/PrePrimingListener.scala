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

package org.apache.spark.sql.listeners

import scala.collection.JavaConverters._

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.DistributableDataMapFormat
import org.apache.carbondata.events.{Event, IndexServerLoadEvent, OperationContext, OperationEventListener}
import org.apache.carbondata.indexserver.IndexServer

// Listener for the PrePriming Event. This listener calls the index server using an Asynccall
object PrePrimingEventListener extends OperationEventListener {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  override def onEvent(event: Event,
      operationContext: OperationContext): Unit = {
    val prePrimingEvent = event.asInstanceOf[IndexServerLoadEvent]
    val carbonTable = prePrimingEvent.carbonTable
    val dataMapFormat = new DistributableDataMapFormat(carbonTable,
      null,
      prePrimingEvent.segment.asJava,
      prePrimingEvent.invalidsegment.asJava,
      null,
      false,
      null,
      false,
      true)
    if (prePrimingEvent.segment.length != 0) {
      try {
        IndexServer.getClient.getCount(dataMapFormat)
      }
      catch {
        // Consider a scenario where prepriming is in progress and the index server crashes, in
        // this case since we should not fail the corresponding operation where pre-priming is
        // triggered. Because prepriming is an optimization for cache loading prior to query,
        // so no exception should be thrown.
        case ex: Exception =>
          LOGGER.error(s"Prepriming failed for table ${carbonTable.getTableName} ", ex)
      }
    }
  }
}
