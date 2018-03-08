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

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedDataMapCommandException}
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.datamap.{DataMapManager, DataMapProvider}

/**
 * Below command class will be used to create datamap on table
 * and updating the parent table about the datamap information
 */
case class CarbonCreateDataMapCommand(
    dataMapName: String,
    tableIdentifier: TableIdentifier,
    dmClassName: String,
    dmProperties: Map[String, String],
    queryString: Option[String],
    ifNotExistsSet: Boolean = false)
  extends AtomicRunnableCommand {

  private var dataMapProvider: DataMapProvider = _
  private var mainTable: CarbonTable = _
  private var dataMapSchema: DataMapSchema = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    // since streaming segment does not support building index and pre-aggregate yet,
    // so streaming table does not support create datamap
    mainTable =
      CarbonEnv.getCarbonTable(tableIdentifier.database, tableIdentifier.table)(sparkSession)
    if (mainTable.isStreamingTable) {
      throw new MalformedCarbonCommandException("Streaming table does not support creating datamap")
    }

    if (mainTable.getDataMapSchema(dataMapName) != null) {
      if (!ifNotExistsSet) {
        throw new MalformedDataMapCommandException(s"DataMap name '$dataMapName' already exist")
      } else {
        return Seq.empty
      }
    }

    dataMapSchema = new DataMapSchema(dataMapName, dmClassName)
    dataMapSchema.setProperties(new java.util.HashMap[String, String](
      dmProperties.map(x => (x._1.trim, x._2.trim)).asJava))
    dataMapProvider = DataMapManager.get().getDataMapProvider(dataMapSchema)
    dataMapProvider.initMeta(mainTable, dataMapSchema, queryString.orNull, sparkSession)

    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    LOGGER.audit(s"DataMap $dataMapName successfully added to Table ${tableIdentifier.table}")
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    if (dataMapProvider != null) {
      dataMapProvider.initData(mainTable, sparkSession)
      if (mainTable.isAutoRefreshDataMap) {
        dataMapProvider.rebuild(mainTable, sparkSession)
      }
    }
    Seq.empty
  }

  override def undoMetadata(sparkSession: SparkSession, exception: Exception): Seq[Row] = {
    if (dataMapProvider != null) {
      dataMapProvider.freeMeta(mainTable, dataMapSchema, sparkSession)
    }
    Seq.empty
  }
}

