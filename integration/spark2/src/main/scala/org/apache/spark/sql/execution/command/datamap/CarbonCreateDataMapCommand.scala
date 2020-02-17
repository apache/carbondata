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
package org.apache.spark.sql.execution.command.datamap

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.secondaryindex.command.ErrorMessage

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedDataMapCommandException}
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.{DataMapProvider, DataMapStoreManager}
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.metadata.ColumnarFormatVersion
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.datamap.{DataMapClassProvider, DataMapProperty}
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.datamap.{DataMapManager, IndexDataMapProvider}
import org.apache.carbondata.events._

/**
 * Below command class will be used to create datamap on table
 * and updating the parent table about the datamap information
 */
case class CarbonCreateDataMapCommand(
    dataMapName: String,
    tableIdentifier: Option[TableIdentifier],
    dmProviderName: String,
    dmProperties: Map[String, String],
    queryString: Option[String],
    ifNotExistsSet: Boolean = false,
    var deferredRebuild: Boolean = false)
  extends AtomicRunnableCommand {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
  private var dataMapProvider: DataMapProvider = _
  private var mainTable: CarbonTable = _
  private var dataMapSchema: DataMapSchema = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    // since streaming segment does not support building index and pre-aggregate yet,
    // so streaming table does not support create datamap
    mainTable = tableIdentifier match {
      case Some(table) =>
        CarbonEnv.getCarbonTable(table.database, table.table)(sparkSession)
      case _ => null
    }

    if (mainTable != null) {
      setAuditTable(mainTable)
    }
    setAuditInfo(Map("provider" -> dmProviderName, "dmName" -> dataMapName) ++ dmProperties)

    if (mainTable != null && !mainTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    if (mainTable != null && CarbonUtil.getFormatVersion(mainTable) != ColumnarFormatVersion.V3) {
      throw new MalformedCarbonCommandException(s"Unsupported operation on table with " +
                                                s"V1 or V2 format data")
    }

    dataMapSchema = new DataMapSchema(dataMapName, dmProviderName)

    val property = dmProperties.map(x => (x._1.trim, x._2.trim)).asJava
    val javaMap = new java.util.HashMap[String, String](property)
    javaMap.put(DataMapProperty.DEFERRED_REBUILD, deferredRebuild.toString)
    dataMapSchema.setProperties(javaMap)

    if (dataMapSchema.isIndexDataMap && mainTable == null) {
      throw new MalformedDataMapCommandException(
        "For this datamap, main table is required. Use `CREATE DATAMAP ... ON TABLE ...` ")
    }
    dataMapProvider = DataMapManager.get.getDataMapProvider(mainTable, dataMapSchema, sparkSession)
    if (deferredRebuild && !dataMapProvider.supportRebuild()) {
      throw new MalformedDataMapCommandException(
        s"DEFERRED REBUILD is not supported on this datamap $dataMapName" +
        s" with provider ${dataMapSchema.getProviderName}")
    }

    if (null != mainTable) {
      if (mainTable.isChildTableForMV) {
        throw new MalformedDataMapCommandException(
          "Cannot create DataMap on child table " + mainTable.getTableUniqueName)
      }
    }
    if (!dataMapSchema.isIndexDataMap) {
      if (DataMapStoreManager.getInstance().getAllDataMapSchemas.asScala
        .exists(_.getDataMapName.equalsIgnoreCase(dataMapSchema.getDataMapName))) {
        if (!ifNotExistsSet) {
          throw new MalformedDataMapCommandException(
            "DataMap with name " + dataMapSchema.getDataMapName + " already exists in storage")
        } else {
          return Seq.empty
        }
      }
    }

    val systemFolderLocation: String = CarbonProperties.getInstance().getSystemFolderLocation
    val operationContext: OperationContext = new OperationContext()

    // If it is index datamap, check whether the column has datamap created already
    dataMapProvider match {
      case provider: IndexDataMapProvider =>
        if (mainTable.isIndexTable) {
          throw new ErrorMessage(
            "Datamap creation on Secondary Index table is not supported")
        }
        val isBloomFilter = DataMapClassProvider.BLOOMFILTER.getShortName
          .equalsIgnoreCase(dmProviderName)
        val datamaps = DataMapStoreManager.getInstance.getAllDataMap(mainTable).asScala
        val thisDmProviderName =
          dataMapProvider.asInstanceOf[IndexDataMapProvider].getDataMapSchema.getProviderName
        val existingIndexColumn4ThisProvider = datamaps.filter { datamap =>
          thisDmProviderName.equalsIgnoreCase(datamap.getDataMapSchema.getProviderName)
        }.flatMap { datamap =>
          datamap.getDataMapSchema.getIndexColumns
        }.distinct

        provider.getIndexedColumns.asScala.foreach { column =>
          if (existingIndexColumn4ThisProvider.contains(column.getColName)) {
            throw new MalformedDataMapCommandException(String.format(
              "column '%s' already has %s index datamap created",
              column.getColName, thisDmProviderName))
          } else if (isBloomFilter) {
            if (column.getDataType == DataTypes.BINARY) {
              throw new MalformedDataMapCommandException(
                s"BloomFilter datamap does not support Binary datatype column: ${
                  column.getColName
                }")
            }
            // if datamap provider is bloomfilter,the index column datatype cannot be complex type
            if (column.isComplex) {
              throw new MalformedDataMapCommandException(
                s"BloomFilter datamap does not support complex datatype column: ${
                  column.getColName
                }")
            }
          }
        }

        val createDataMapPreExecutionEvent: CreateDataMapPreExecutionEvent =
          new CreateDataMapPreExecutionEvent(sparkSession,
            systemFolderLocation, tableIdentifier.get)
        OperationListenerBus.getInstance().fireEvent(createDataMapPreExecutionEvent,
          operationContext)
        dataMapProvider.initMeta(queryString.orNull)
        DataMapStatusManager.disableDataMap(dataMapName)
      case _ =>
        val createDataMapPreExecutionEvent: CreateDataMapPreExecutionEvent =
          CreateDataMapPreExecutionEvent(sparkSession,
            systemFolderLocation, tableIdentifier.orNull)
        OperationListenerBus.getInstance().fireEvent(createDataMapPreExecutionEvent,
          operationContext)
        dataMapProvider.initMeta(queryString.orNull)
    }
    val createDataMapPostExecutionEvent: CreateDataMapPostExecutionEvent =
      CreateDataMapPostExecutionEvent(sparkSession,
        systemFolderLocation, tableIdentifier, dmProviderName)
    OperationListenerBus.getInstance().fireEvent(createDataMapPostExecutionEvent,
      operationContext)
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    if (dataMapProvider != null) {
      dataMapProvider.initData()
      // TODO: remove these checks once the preaggregate and preaggregate timeseries are deprecated
      if (mainTable != null && !deferredRebuild && dataMapSchema.isIndexDataMap) {
        dataMapProvider.rebuild()
        if (dataMapSchema.isIndexDataMap) {
          val operationContext: OperationContext = new OperationContext()
          val systemFolderLocation: String = CarbonProperties.getInstance().getSystemFolderLocation
          val updateDataMapPreExecutionEvent: UpdateDataMapPreExecutionEvent =
            new UpdateDataMapPreExecutionEvent(sparkSession,
              systemFolderLocation, tableIdentifier.get)
          OperationListenerBus.getInstance().fireEvent(updateDataMapPreExecutionEvent,
            operationContext)
          DataMapStatusManager.enableDataMap(dataMapName)
          val updateDataMapPostExecutionEvent: UpdateDataMapPostExecutionEvent =
            new UpdateDataMapPostExecutionEvent(sparkSession,
              systemFolderLocation, tableIdentifier.get)
          OperationListenerBus.getInstance().fireEvent(updateDataMapPostExecutionEvent,
            operationContext)
        }
      }
      if (null != dataMapSchema.getRelationIdentifier && !dataMapSchema.isIndexDataMap &&
          !dataMapSchema.isLazy) {
        DataMapStatusManager.enableDataMap(dataMapName)
      }
    }
    Seq.empty
  }

  override def undoMetadata(sparkSession: SparkSession, exception: Exception): Seq[Row] = {
    if (dataMapProvider != null) {
      val table =
        if (mainTable != null) {
          Some(TableIdentifier(mainTable.getTableName, Some(mainTable.getDatabaseName)))
        } else {
          None
        }
        CarbonDropDataMapCommand(
          dataMapName,
          true,
          table,
          forceDrop = false).run(sparkSession)
    }
    Seq.empty
  }

  override protected def opName: String = "CREATE DATAMAP"
}

