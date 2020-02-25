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

package org.apache.spark.sql.execution.command.index

import java.util.Optional

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.command.DataCommand
import org.apache.spark.sql.secondaryindex.command.SIRebuildSegmentRunner

import org.apache.carbondata.common.exceptions.sql.MalformedIndexCommandException
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.datamap.DataMapManager
import org.apache.carbondata.events.{UpdateDataMapPostExecutionEvent, _}

/**
 * Rebuild the index through sync with main table data. After sync with parent table's it enables
 * the index.
 */
case class CarbonRefreshIndexCommand(
    indexName: String,
    parentTableIdent: TableIdentifier,
    segments: Option[List[String]]) extends DataCommand {

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val parentTable = CarbonEnv.getCarbonTable(parentTableIdent)(sparkSession)
    setAuditTable(parentTable)
    val indexOp = DataMapStoreManager.getInstance().getIndexInTable(parentTable, indexName)
    if (!indexOp.isPresent) {
      val indexTable = try {
        CarbonEnv.getCarbonTable(parentTableIdent.database, indexName)(sparkSession)
      } catch {
        case t: NoSuchTableException =>
          throw new MalformedIndexCommandException(
            s"Index with name $indexName does not exist on table ${parentTableIdent.table}")
      }
      refreshIndexTable(parentTable, indexTable, sparkSession)
    } else {
      refreshIndex(sparkSession, parentTable, indexOp)
    }
    Seq.empty
  }

  private def refreshIndexTable(
      parentTable: CarbonTable,
      indexTable: CarbonTable,
      sparkSession: SparkSession): Unit = {
    SIRebuildSegmentRunner(parentTable, indexTable, segments).run(sparkSession)
  }

  private def refreshIndex(
      sparkSession: SparkSession,
      parentTable: CarbonTable,
      indexOp: Optional[DataMapSchema]): Unit = {
    val schema = indexOp.get
    if (!schema.isLazy) {
      throw new MalformedIndexCommandException(
        s"Non-lazy index $indexName does not support manual refresh")
    }

    val provider = DataMapManager.get().getDataMapProvider(parentTable, schema, sparkSession)
    provider.rebuild()

    // After rebuild successfully enable the index.
    val operationContext = new OperationContext()
    val storeLocation = CarbonProperties.getInstance().getSystemFolderLocation
    val preExecEvent = UpdateDataMapPreExecutionEvent(
      sparkSession, storeLocation,
      new TableIdentifier(parentTable.getTableName, Some(parentTable.getDatabaseName)))
    OperationListenerBus.getInstance().fireEvent(preExecEvent, operationContext)

    DataMapStatusManager.enableDataMap(indexName)

    val postExecEvent = UpdateDataMapPostExecutionEvent(
      sparkSession, storeLocation,
      new TableIdentifier(parentTable.getTableName, Some(parentTable.getDatabaseName)))
    OperationListenerBus.getInstance().fireEvent(postExecEvent, operationContext)
  }

  override protected def opName: String = "REFRESH INDEX"
}
