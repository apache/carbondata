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
import org.apache.carbondata.events._
import org.apache.carbondata.view.MVManagerInSpark

object ShowCacheEventListener extends OperationEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   */
  override protected def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case showTableCacheEvent: ShowTableCacheEvent =>
        val carbonTable = showTableCacheEvent.carbonTable
        val internalCall = showTableCacheEvent.internalCall
        if (carbonTable.isMV && !internalCall) {
          throw new UnsupportedOperationException("Operation not allowed on child table.")
        }

        val childTables = operationContext.getProperty(carbonTable.getTableUniqueName)
          .asInstanceOf[List[(String, String)]]
        val views =
          MVManagerInSpark.get(showTableCacheEvent.sparkSession).getSchemasOnTable(carbonTable)
        if (!views.isEmpty) {
          val mvTables = views.asScala.collect {
            case view =>
              (
                s"${view.getIdentifier.getDatabaseName}-${view.getIdentifier.getTableName}",
                "mv",
                view.getIdentifier.getTableId
              )
          }
          operationContext.setProperty(carbonTable.getTableUniqueName, childTables ++ mvTables)
        }
    }
  }
}
