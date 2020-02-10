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
package org.apache.spark.sql.secondaryindex.events

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.events.{Event, LoadEventInfo}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel

/**
 * Class for handling operations before start of a load process.
 * Example usage: For validation purpose
 */
case class LoadTableSIPreExecutionEvent(sparkSession: SparkSession,
    carbonTableIdentifier: CarbonTableIdentifier,
    carbonLoadModel: CarbonLoadModel,
    indexCarbonTable: CarbonTable) extends Event with LoadEventInfo

/**
 * Class for handling operations after data load completion and before final
 * commit of load operation. Example usage: For loading pre-aggregate tables
 */
case class LoadTableSIPostExecutionEvent(sparkSession: SparkSession,
    carbonTableIdentifier: CarbonTableIdentifier,
    carbonLoadModel: CarbonLoadModel,
    carbonTable: CarbonTable)
  extends Event with LoadEventInfo

/**
 * Class for handling clean up in case of any failure and abort the operation.
 */
case class LoadTableSIAbortExecutionEvent(sparkSession: SparkSession,
    carbonTableIdentifier: CarbonTableIdentifier,
    carbonLoadModel: CarbonLoadModel) extends Event with LoadEventInfo
