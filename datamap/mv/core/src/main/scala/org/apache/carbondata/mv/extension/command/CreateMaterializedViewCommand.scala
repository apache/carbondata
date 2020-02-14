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

package org.apache.carbondata.mv.extension.command

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql._
import org.apache.spark.sql.execution.command._

import org.apache.carbondata.common.exceptions.sql.MalformedMaterializedViewException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.{DataMapProvider, DataMapStoreManager}
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.metadata.schema.datamap.{DataMapClassProvider, DataMapProperty}
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.datamap.DataMapManager
import org.apache.carbondata.events._

/**
 * Create Materialized View Command implementation
 * It will create the MV table, load the MV table (if deferred rebuild is false),
 * and register the MV schema in [[DataMapStoreManager]]
 */
case class CreateMaterializedViewCommand(
    mvName: String,
    properties: Map[String, String],
    queryString: Option[String],
    ifNotExistsSet: Boolean = false,
    deferredRebuild: Boolean = false)
  extends AtomicRunnableCommand {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
  private var dataMapProvider: DataMapProvider = _
  private var dataMapSchema: DataMapSchema = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {

    setAuditInfo(Map("mvName" -> mvName) ++ properties)

    val mutableMap = mutable.Map[String, String](properties.toSeq: _*)
    mutableMap.put(DataMapProperty.DEFERRED_REBUILD, deferredRebuild.toString)

    dataMapSchema = new DataMapSchema(mvName, DataMapClassProvider.MV.name())
    dataMapSchema.setProperties(mutableMap.asJava)
    dataMapProvider = DataMapManager.get.getDataMapProvider(null, dataMapSchema, sparkSession)
    if (DataMapStoreManager.getInstance().getAllDataMapSchemas.asScala
      .exists(_.getDataMapName.equalsIgnoreCase(dataMapSchema.getDataMapName))) {
      if (!ifNotExistsSet) {
        throw new MalformedMaterializedViewException(
          s"Materialized view with name ${dataMapSchema.getDataMapName} already exists")
      } else {
        return Seq.empty
      }
    }

    val systemFolderLocation: String = CarbonProperties.getInstance().getSystemFolderLocation
    val operationContext: OperationContext = new OperationContext()
    val preExecEvent = CreateDataMapPreExecutionEvent(sparkSession, systemFolderLocation, null)
    OperationListenerBus.getInstance().fireEvent(preExecEvent, operationContext)

    dataMapProvider.initMeta(queryString.orNull)

    val postExecEvent = CreateDataMapPostExecutionEvent(
      sparkSession, systemFolderLocation, null, DataMapClassProvider.MV.name())
    OperationListenerBus.getInstance().fireEvent(postExecEvent, operationContext)
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    if (dataMapProvider != null) {
      dataMapProvider.initData()
      if (!dataMapSchema.isLazy) {
        DataMapStatusManager.enableDataMap(mvName)
      }
    }
    Seq.empty
  }

  override def undoMetadata(sparkSession: SparkSession, exception: Exception): Seq[Row] = {
    if (dataMapProvider != null) {
      DropMaterializedViewCommand(mvName, true).run(sparkSession)
    }
    Seq.empty
  }

  override protected def opName: String = "CREATE MATERIALIZED VIEW"
}

