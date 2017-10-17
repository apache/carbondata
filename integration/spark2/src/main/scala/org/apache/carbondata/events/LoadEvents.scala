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

package org.apache.carbondata.events

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.processing.loading.model.CarbonLoadModel

/**
 * Class for handling operations before start of a load process.
 * Example usage: For validation purpose
 */
case class LoadTablePreExecutionEvent(sparkSession: SparkSession,
    carbonTableIdentifier: CarbonTableIdentifier,
    carbonLoadModel: CarbonLoadModel) extends LoadEvent {
  override def getEventType: String = LoadTablePreExecutionEvent.eventType
}

/**
 * Class for handling operations after data load completion and before final
 * commit of load operation. Example usage: For loading pre-aggregate tables
 */
case class LoadTablePostExecutionEvent(sparkSession: SparkSession,
    carbonTableIdentifier: CarbonTableIdentifier,
    carbonLoadModel: CarbonLoadModel) extends LoadEvent {
  override def getEventType: String = LoadTablePostExecutionEvent.eventType
}

/**
 * Class for handling clean up in case of any failure and abort the operation.
 */
case class LoadTableAbortExecutionEvent(sparkSession: SparkSession,
    carbonTableIdentifier: CarbonTableIdentifier,
    carbonLoadModel: CarbonLoadModel) extends LoadEvent {
  override def getEventType: String = LoadTableAbortExecutionEvent.eventType
}

object LoadTablePreExecutionEvent {
  val eventType = LoadTablePreExecutionEvent.getClass.getName
}

object LoadTablePostExecutionEvent {
  val eventType = LoadTablePostExecutionEvent.getClass.getName
}

object LoadTableAbortExecutionEvent {
  val eventType = LoadTableAbortExecutionEvent.getClass.getName
}
