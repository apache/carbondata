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

import java.util

import scala.util.control.Breaks.{break, breakable}

import org.apache.spark.sql.{CarbonDataCommands, CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.index.CarbonIndexUtil
import org.apache.spark.sql.secondaryindex.command.SIRebuildSegmentRunner

import org.apache.carbondata.common.exceptions.sql.MalformedIndexCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.index.status.IndexStatus
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.index.IndexType
import org.apache.carbondata.core.metadata.schema.indextable.IndexTableInfo
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, IndexSchema}
import org.apache.carbondata.index.IndexProvider

/**
 * Rebuild the index through sync with main table data. After sync with parent table's it enables
 * the index.
 */
case class CarbonRefreshIndexCommand(
    indexName: String,
    parentTableIdent: TableIdentifier,
    segments: Option[List[String]]) extends CarbonDataCommands {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val parentTable = CarbonEnv.getCarbonTable(parentTableIdent)(sparkSession)
    setAuditTable(parentTable)
    val indexProviderMap = parentTable.getIndexesMap
    val secondaryIndexes = indexProviderMap.get(IndexType.SI.getIndexProviderName)
    if (null != secondaryIndexes && secondaryIndexes.containsKey(indexName)) {
      val indexTable = try {
        CarbonEnv.getCarbonTable(parentTableIdent.database, indexName)(sparkSession)
      } catch {
        case t: NoSuchTableException =>
          throw new MalformedIndexCommandException(
            s"Index with name $indexName does not exist on table ${parentTableIdent.table}")
      }
      refreshIndexTable(parentTable, indexTable, sparkSession)
    } else {
      refreshIndex(sparkSession, parentTable)
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
      parentTable: CarbonTable): Unit = {
    var indexInfo: util.Map[String, String] = new util.HashMap[String, String]()
    val cgAndFgIndexIterator = CarbonIndexUtil.getCGAndFGIndexes(parentTable).entrySet().iterator()
    breakable {
      while (cgAndFgIndexIterator.hasNext) {
        val indexMap = cgAndFgIndexIterator.next().getValue
        if (indexMap.containsKey(indexName)) {
          indexInfo = indexMap.get(indexName)
          break()
        }
      }
    }
    if (indexInfo.isEmpty) {
      throw new MalformedIndexCommandException(
        "Index with name `" + indexName + "` is not present" +
        "on table `" + parentTable.getTableName + "`")
    }
    val indexProviderName = indexInfo.get(CarbonCommonConstants.INDEX_PROVIDER)
    val schema = new IndexSchema(indexName, indexProviderName)
    schema.setProperties(indexInfo)
    if (!schema.isLazy) {
      throw new MalformedIndexCommandException(
        s"Non-lazy index $indexName does not support manual refresh")
    }

    val provider = new IndexProvider(parentTable, schema, sparkSession)
    provider.rebuild()
    // enable bloom or lucene index
    // get metadata lock to avoid concurrent create index operations
    val metadataLock = CarbonLockFactory.getCarbonLockObj(
      parentTable.getAbsoluteTableIdentifier,
      LockUsage.METADATA_LOCK)
    try {
      if (metadataLock.lockWithRetries()) {
        LOGGER.info(s"Acquired the metadata lock for table " +
                    s"${ parentTable.getDatabaseName}.${ parentTable.getTableName }")
        val oldIndexInfo = parentTable.getIndexInfo
        val updatedIndexInfo = IndexTableInfo.setIndexStatus(oldIndexInfo,
          indexName,
          IndexStatus.ENABLED)

        // set index information in parent table
        val parentIndexMetadata = parentTable.getIndexMetadata
        parentIndexMetadata.updateIndexStatus(indexProviderName,
          indexName,
          IndexStatus.ENABLED.name())
        parentTable.getTableInfo.getFactTable.getTableProperties
          .put(parentTable.getCarbonTableIdentifier.getTableId, parentIndexMetadata.serialize)

        sparkSession.sql(
          s"""ALTER TABLE ${ parentTable.getDatabaseName }.${ parentTable.getTableName } SET
             |SERDEPROPERTIES ('indexInfo' =
             |'$updatedIndexInfo')""".stripMargin).collect()

        // modify the tableProperties of mainTable by adding "indexExists" property
        CarbonIndexUtil
          .addOrModifyTableProperty(
            parentTable,
            Map("indexExists" -> "true"), needLock = false)(sparkSession)

        val identifier = TableIdentifier(parentTable.getTableName,
          Some(parentTable.getDatabaseName))

        // refresh the parent table relation
        sparkSession.sessionState.catalog.refreshTable(identifier)
      }
    } finally {
      metadataLock.unlock()
    }
  }

  override protected def opName: String = "REFRESH INDEX"
}
