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

package org.apache.carbondata.streaming

import java.util
import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{CarbonAppendableStreamSink, StreamExecution}
import org.apache.spark.sql.streaming.StreamingQueryListener

import org.apache.carbondata.common.logging.LogServiceFactory

class CarbonStreamingQueryListener(spark: SparkSession) extends StreamingQueryListener {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  private val cache = new util.HashMap[UUID, String]()

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    val streamQuery = spark.streams.get(event.id)
    val qry = if (streamQuery.isInstanceOf[StreamExecution]) {
      // adapt spark 2.1
      streamQuery.asInstanceOf[StreamExecution]
    } else {
      // adapt spark 2.2 and later version
      val clazz = Class.forName("org.apache.spark.sql.execution.streaming.StreamingQueryWrapper")
      val method = clazz.getMethod("streamingQuery")
      method.invoke(streamQuery).asInstanceOf[StreamExecution]
    }
    if (qry.sink.isInstanceOf[CarbonAppendableStreamSink]) {
      LOGGER.info("Carbon streaming query started: " + event.id)
      val sink = qry.sink.asInstanceOf[CarbonAppendableStreamSink]
      val carbonTable = sink.carbonTable
      cache.put(event.id, carbonTable.getTableUniqueName)
    }
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    val tableUniqueName = cache.remove(event.id)
    if (null != tableUniqueName) {
      LOGGER.info("Carbon streaming query End: " + event.id)
      StreamSinkFactory.unLock(tableUniqueName)
    }
  }
}
