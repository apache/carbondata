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

import java.util

import scala.collection.JavaConverters._
import scala.util.control.Breaks._

import com.google.gson.Gson
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{Checker, DataCommand}
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.{DataMapStoreManager, DataMapUtil}
import org.apache.carbondata.core.datamap.status.{DataMapSegmentStatusUtil, DataMapStatus, DataMapStatusManager}
import org.apache.carbondata.core.metadata.schema.datamap.{DataMapClassProvider, DataMapProperty}
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath

/**
 * Show the datamaps on the table
 *
 * @param tableIdentifier
 */
case class CarbonDataMapShowCommand(tableIdentifier: Option[TableIdentifier])
  extends DataCommand {

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("DataMapName", StringType, nullable = false)(),
      AttributeReference("ClassName", StringType, nullable = false)(),
      AttributeReference("Associated Table", StringType, nullable = false)(),
      AttributeReference("DataMap Properties", StringType, nullable = false)(),
      AttributeReference("DataMap Status", StringType, nullable = false)(),
      AttributeReference("Sync Status", StringType, nullable = false)())
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    convertToRow(getAllDataMaps(sparkSession))
  }

  /**
   * get all datamaps for this table, including preagg, index datamaps and mv
   */
  def getAllDataMaps(sparkSession: SparkSession): util.List[DataMapSchema] = {
    val dataMapSchemaList: util.List[DataMapSchema] = new util.ArrayList[DataMapSchema]()
    tableIdentifier match {
      case Some(table) =>
        val carbonTable = CarbonEnv.getCarbonTable(table)(sparkSession)
        setAuditTable(carbonTable)
        Checker.validateTableExists(table.database, table.table, sparkSession)
        if (carbonTable.hasDataMapSchema) {
          dataMapSchemaList.addAll(carbonTable.getTableInfo.getDataMapSchemaList)
        }
        val indexSchemas = DataMapStoreManager.getInstance().getDataMapSchemasOfTable(carbonTable)
        if (!indexSchemas.isEmpty) {
          dataMapSchemaList.addAll(indexSchemas)
        }
      case _ =>
        dataMapSchemaList.addAll(DataMapStoreManager.getInstance().getAllDataMapSchemas)
    }
    dataMapSchemaList
  }

  private def convertToRow(schemaList: util.List[DataMapSchema]) = {
    if (schemaList != null && schemaList.size() > 0) {
      schemaList.asScala
        .map { s =>
          val relationIdentifier = s.getRelationIdentifier
          val table = relationIdentifier.getDatabaseName + "." + relationIdentifier.getTableName
          // preaggregate datamap does not support user specified property, therefor we return empty
          val dmPropertieStr = if (s.getProviderName.equalsIgnoreCase(
              DataMapClassProvider.PREAGGREGATE.getShortName)) {
            ""
          } else {
            s.getProperties.asScala
              // ignore internal used property
              .filter(p => !p._1.equalsIgnoreCase(DataMapProperty.DEFERRED_REBUILD) &&
                           !p._1.equalsIgnoreCase(DataMapProperty.CHILD_SELECT_QUERY) &&
                           !p._1.equalsIgnoreCase(DataMapProperty.QUERY_TYPE))
              .map(p => s"'${ p._1 }'='${ p._2 }'").toSeq
              .sorted.mkString(", ")
          }
          // Get datamap status and sync information details
          var dataMapStatus = "NA"
          var syncInfo: String = "NA"
          if (!s.getProviderName.equalsIgnoreCase(
            DataMapClassProvider.PREAGGREGATE.getShortName) && !s.getProviderName.equalsIgnoreCase(
            DataMapClassProvider.TIMESERIES.getShortName)) {
            if (DataMapStatusManager.getEnabledDataMapStatusDetails
              .exists(_.getDataMapName.equalsIgnoreCase(s.getDataMapName))) {
              dataMapStatus = DataMapStatus.ENABLED.name()
            } else {
              dataMapStatus = DataMapStatus.DISABLED.name()
            }
            val loadMetadataDetails = SegmentStatusManager
              .readLoadMetadata(CarbonTablePath
                .getMetadataPath(s.getRelationIdentifier.getTablePath))
            if (!s.isIndexDataMap && loadMetadataDetails.nonEmpty) {
              breakable({
                for (i <- loadMetadataDetails.length - 1 to 0 by -1) {
                  if (loadMetadataDetails(i).getSegmentStatus.equals(SegmentStatus.SUCCESS)) {
                    val segmentMaps =
                      DataMapSegmentStatusUtil.getSegmentMap(loadMetadataDetails(i).getExtraInfo)
                    val syncInfoMap = new util.HashMap[String, String]()
                    val iterator = segmentMaps.entrySet().iterator()
                    while (iterator.hasNext) {
                      val entry = iterator.next()

                      syncInfoMap.put(entry.getKey, DataMapUtil.getMaxSegmentID(entry.getValue))
                    }
                    val loadEndTime =
                      if (loadMetadataDetails(i).getLoadEndTime ==
                          CarbonCommonConstants.SEGMENT_LOAD_TIME_DEFAULT) {
                        "NA"
                      } else {
                        new java.sql.Timestamp(loadMetadataDetails(i).getLoadEndTime).toString
                      }
                    syncInfoMap.put(CarbonCommonConstants.LOAD_SYNC_TIME, loadEndTime)
                    syncInfo = new Gson().toJson(syncInfoMap)
                    break()
                  }
                }
              })
            }
          }
          Row(s.getDataMapName, s.getProviderName, table, dmPropertieStr, dataMapStatus, syncInfo)
      }
    } else {
      Seq.empty
    }
  }

  override protected def opName: String = "SHOW DATAMAP"
}
