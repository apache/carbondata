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

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.DataCommand

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.datamap.DataMapManager

/**
 * Refresh the datamaps through sync with main table data. After sync with parent table's it enables
 * the datamap.
 */
case class CarbonDataMapRefreshCommand(
    dataMapName: String,
    tableIdentifier: Option[TableIdentifier]) extends DataCommand {

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val schema = DataMapStoreManager.getInstance().getDataMapSchema(dataMapName)
    val provider = DataMapManager.get().getDataMapProvider(schema, sparkSession)
    val table = tableIdentifier match {
      case Some(identifier) =>
        CarbonEnv.getCarbonTable(identifier)(sparkSession)
      case _ => null
    }
    // Sync the datamap with parent table
    provider.rebuild(table, schema)
    // After sync success enable the datamap.
    DataMapStatusManager.enableDataMap(dataMapName)
    Seq.empty
  }

}
