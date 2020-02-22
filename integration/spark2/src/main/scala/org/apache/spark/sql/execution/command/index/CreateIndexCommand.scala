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

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.secondaryindex.command.IndexModel

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedIndexCommandException, NoSuchIndexException}
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.metadata.ColumnarFormatVersion
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.datamap.{DataMapClassProvider, DataMapProperty}
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.datamap.IndexProvider
import org.apache.carbondata.events._

/**
 * Below command class will be used to create index on table
 * and updating the parent table about the index information
 */
case class CreateIndexCommand(
    indexModel: IndexModel,
    indexProviderName: String,
    properties: Map[String, String],
    ifNotExistsSet: Boolean = false,
    var deferredRebuild: Boolean = false)
  extends AtomicRunnableCommand {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
  private var provider: IndexProvider = _
  private var mainTable: CarbonTable = _
  private var dataMapSchema: DataMapSchema = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    // since streaming segment does not support building index yet,
    // so streaming table does not support create index
    mainTable = CarbonEnv.getCarbonTable(indexModel.dbName, indexModel.tableName)(sparkSession)
    val indexName = indexModel.indexName

    setAuditTable(mainTable)
    setAuditInfo(Map("provider" -> indexProviderName, "indexName" -> indexName) ++ properties)

    if (!mainTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    if (DataMapStoreManager.getInstance().isIndexExist(mainTable.getTableId, indexName)) {
      if (!ifNotExistsSet) {
        throw new NoSuchIndexException(indexName)
      } else {
        return Seq.empty
      }
    }

    if (CarbonUtil.getFormatVersion(mainTable) != ColumnarFormatVersion.V3) {
      throw new MalformedCarbonCommandException(s"Unsupported operation on table with " +
                                                s"V1 or V2 format data")
    }

    dataMapSchema = new DataMapSchema(indexName, indexProviderName)

    val property = properties.map(x => (x._1.trim, x._2.trim)).asJava
    val javaMap = new java.util.HashMap[String, String](property)
    javaMap.put(DataMapProperty.DEFERRED_REBUILD, deferredRebuild.toString)
    javaMap.put(CarbonCommonConstants.INDEX_COLUMNS, indexModel.columnNames.mkString(","))
    dataMapSchema.setProperties(javaMap)

    if (dataMapSchema.isIndexDataMap && mainTable == null) {
      throw new MalformedIndexCommandException(
        "To create index, main table is required. Use `CREATE INDEX ... ON TABLE ...` ")
    }
    provider = new IndexProvider(mainTable, dataMapSchema, sparkSession)
    if (deferredRebuild && !provider.supportRebuild()) {
      throw new MalformedIndexCommandException(
        s"DEFERRED REFRESH is not supported on this index $indexName" +
        s" with provider ${dataMapSchema.getProviderName}")
    }

    if (mainTable.isMVTable) {
      throw new MalformedIndexCommandException(
        "Cannot create index on MV table " + mainTable.getTableUniqueName)
    }

    val storeLocation: String = CarbonProperties.getInstance().getSystemFolderLocation
    val operationContext: OperationContext = new OperationContext()

    // check whether the column has index created already
    val isBloomFilter = DataMapClassProvider.BLOOMFILTER.getShortName
      .equalsIgnoreCase(indexProviderName)
    val indexes = DataMapStoreManager.getInstance.getAllIndex(mainTable).asScala
    val thisDmProviderName = provider.getDataMapSchema.getProviderName
    val existingIndexColumn4ThisProvider = indexes.filter { index =>
      thisDmProviderName.equalsIgnoreCase(index.getDataMapSchema.getProviderName)
    }.flatMap { index =>
      index.getDataMapSchema.getIndexColumns
    }.distinct

    provider.getIndexedColumns.asScala.foreach { column =>
      if (existingIndexColumn4ThisProvider.contains(column.getColName)) {
        throw new MalformedIndexCommandException(String.format(
          "column '%s' already has %s index created",
          column.getColName, thisDmProviderName))
      } else if (isBloomFilter) {
        if (column.getDataType == DataTypes.BINARY) {
          throw new MalformedIndexCommandException(
            s"BloomFilter does not support Binary datatype column: ${
              column.getColName
            }")
        }
        // For bloomfilter, the index column datatype cannot be complex type
        if (column.isComplex) {
          throw new MalformedIndexCommandException(
            s"BloomFilter does not support complex datatype column: ${
              column.getColName
            }")
        }
      }
    }

    val table = TableIdentifier(indexModel.tableName, indexModel.dbName)
    val preExecEvent = CreateDataMapPreExecutionEvent(sparkSession, storeLocation, table)
    OperationListenerBus.getInstance().fireEvent(preExecEvent, operationContext)

    provider.initMeta(null)
    DataMapStatusManager.disableDataMap(indexName)

    val postExecEvent = CreateDataMapPostExecutionEvent(sparkSession,
      storeLocation, Some(table), indexProviderName)
    OperationListenerBus.getInstance().fireEvent(postExecEvent, operationContext)
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    if (provider != null) {
      provider.initData()
      if (!deferredRebuild) {
        provider.rebuild()
        val table = TableIdentifier(indexModel.tableName, indexModel.dbName)
        val operationContext = new OperationContext()
        val storeLocation = CarbonProperties.getInstance().getSystemFolderLocation
        val preExecEvent = UpdateDataMapPreExecutionEvent(sparkSession, storeLocation, table)
        OperationListenerBus.getInstance().fireEvent(preExecEvent, operationContext)

        DataMapStatusManager.enableDataMap(indexModel.indexName)

        val postExecEvent = UpdateDataMapPostExecutionEvent(sparkSession, storeLocation, table)
        OperationListenerBus.getInstance().fireEvent(postExecEvent, operationContext)
      }
    }
    Seq.empty
  }

  override def undoMetadata(sparkSession: SparkSession, exception: Exception): Seq[Row] = {
    if (provider != null) {
      val table = TableIdentifier(mainTable.getTableName, Some(mainTable.getDatabaseName))
      CarbonDropIndexCommand(
        indexName = indexModel.indexName,
        ifExistsSet = true,
        table = table
      ).run(sparkSession)
    }
    Seq.empty
  }

  override protected def opName: String = "CREATE INDEX"
}

