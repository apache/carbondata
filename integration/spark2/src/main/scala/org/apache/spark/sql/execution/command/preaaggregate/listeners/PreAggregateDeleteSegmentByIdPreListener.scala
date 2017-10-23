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

package org.apache.spark.sql.execution.command.preaaggregate.listeners

import org.apache.carbondata.events.{DeleteSegmentByIdPreEvent, Event, OperationContext, OperationEventListener}


object PreAggregateDeleteSegmentByIdPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val tableEvent = event.asInstanceOf[DeleteSegmentByIdPreEvent]
    val carbonTable = tableEvent.carbonTable
    if (carbonTable != null) {
      if (carbonTable.hasPreAggregateTables) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on tables which have a pre-aggregate table")
      }
      if (carbonTable.isPreAggregateTable) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on pre-aggregate table")
      }
    }
  }

}
