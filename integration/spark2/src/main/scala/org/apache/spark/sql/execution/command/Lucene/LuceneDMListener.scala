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

package org.apache.spark.sql.execution.command.Lucene

import scala.collection.JavaConverters._

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.events._

object LuceneRenameTablePreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event,
      operationContext: OperationContext): Unit = {
    val renameTablePostListener = event.asInstanceOf[AlterTableRenamePreEvent]
    val carbonTable = renameTablePostListener.carbonTable
    val schemas = DataMapStoreManager.getInstance().getAllDataMapSchemas(carbonTable)
    if (schemas != null) {
      schemas.asScala.foreach(schema =>
        if (schema.getProviderName
              .equalsIgnoreCase(DataMapClassProvider.LUCENEFG.getShortName) ||
            schema.getProviderName
              .equalsIgnoreCase(DataMapClassProvider.LUCENECG.getShortName)) {
          throw new UnsupportedOperationException(
            "Rename operation is not supported for table with Lucene DataMap")
        }
      )
    }
  }
}

object LuceneAddColumnPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event,
      operationContext: OperationContext): Unit = {
    val dataTypeChangePreListener = event.asInstanceOf[AlterTableAddColumnPreEvent]
    val carbonTable = dataTypeChangePreListener.carbonTable
    var schemas = DataMapStoreManager.getInstance().getAllDataMapSchemas(carbonTable)
    if (schemas != null) {
      schemas.asScala.foreach(schema =>
        if (schema.getProviderName
              .equalsIgnoreCase(DataMapClassProvider.LUCENEFG.getShortName) ||
            schema.getProviderName
              .equalsIgnoreCase(DataMapClassProvider.LUCENECG.getShortName)) {
          throw new UnsupportedOperationException(
            "Add column operation is not supported for table with Lucene DataMap")
        }
      )
    }
  }
}

object LuceneDropColumnPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event,
      operationContext: OperationContext): Unit = {
    val dataTypeChangePreListener = event.asInstanceOf[AlterTableDropColumnPreEvent]
    val carbonTable = dataTypeChangePreListener.carbonTable
    var schemas = DataMapStoreManager.getInstance().getAllDataMapSchemas(carbonTable)
    if (schemas != null) {
      schemas.asScala.foreach(schema =>
        if (schema.getProviderName
              .equalsIgnoreCase(DataMapClassProvider.LUCENEFG.getShortName) ||
            schema.getProviderName
              .equalsIgnoreCase(DataMapClassProvider.LUCENECG.getShortName)) {
          throw new UnsupportedOperationException(
            "Drop column operation is not supported for table with Lucene DataMap")
        }
      )
    }
  }
}

object LuceneChangeDataTypePreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event,
      operationContext: OperationContext): Unit = {
    val dataTypeChangePreListener = event.asInstanceOf[AlterTableDataTypeChangePreEvent]
    val carbonTable = dataTypeChangePreListener.carbonTable
    var schemas = DataMapStoreManager.getInstance().getAllDataMapSchemas(carbonTable)
    if (schemas != null) {
      schemas.asScala.foreach(schema =>
        if (schema.getProviderName
              .equalsIgnoreCase(DataMapClassProvider.LUCENEFG.getShortName) ||
            schema.getProviderName
              .equalsIgnoreCase(DataMapClassProvider.LUCENECG.getShortName)) {
          throw new UnsupportedOperationException(
            "Change DataType operation is not supported for table with Lucene DataMap")
        }
      )
    }
  }
}
