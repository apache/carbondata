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

package org.apache.carbondata.spark.rdd

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.execution.command.CompactionModel

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger.CarbonDataMergerUtil

abstract class Compactor(carbonLoadModel: CarbonLoadModel, compactionModel: CompactionModel) {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def executeCompaction(): Unit

  def identifySegmentsToBeMerged(): java.util.List[LoadMetadataDetails] = {
    val customSegmentIds: util.List[String] = if (compactionModel.customSegmentIds.isDefined) {
      compactionModel.customSegmentIds.get.asJava
    } else {
      new util.ArrayList[String]()
    }
    CarbonDataMergerUtil
      .identifySegmentsToBeMerged(carbonLoadModel,
        compactionModel.compactionSize,
        new util.ArrayList(
          carbonLoadModel.getLoadMetadataDetails.asScala.filter(_.isCarbonFormat).asJava),
        compactionModel.compactionType,
        customSegmentIds)
  }
}
