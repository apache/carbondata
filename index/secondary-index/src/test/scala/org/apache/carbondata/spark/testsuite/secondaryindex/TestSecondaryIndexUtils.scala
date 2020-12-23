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
package org.apache.carbondata.spark.testsuite.secondaryindex

import java.io.IOException
import java.util

import mockit.{Mock, MockUp}
import org.apache.spark.sql.{Row, SparkSession, SQLContext}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.table.CarbonCreateDataSourceTableCommand
import org.apache.spark.sql.index.CarbonIndexUtil
import org.apache.spark.sql.secondaryindex.events.SILoadEventListener
import org.apache.spark.sql.secondaryindex.joins.BroadCastSIFilterPushJoin
import org.apache.spark.sql.secondaryindex.util.SecondaryIndexUtil

import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException
import org.apache.carbondata.core.locks.AbstractCarbonLock
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.events.{Event, OperationContext}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.store.CarbonFactDataHandlerColumnar

object TestSecondaryIndexUtils {
  /**
   * Method to check whether the filter is push down to SI table or not
   *
   * @param sparkPlan
   * @return
   */
  def isFilterPushedDownToSI(sparkPlan: SparkPlan): Boolean = {
    var isValidPlan = false
    sparkPlan.transform {
      case broadCastSIFilterPushDown: BroadCastSIFilterPushJoin =>
        isValidPlan = true
        broadCastSIFilterPushDown
    }
    isValidPlan
  }

  def mockTableLock(): MockUp[AbstractCarbonLock] = {
    val mock: MockUp[AbstractCarbonLock] = new MockUp[AbstractCarbonLock]() {
      @Mock
      def lockWithRetries(): Boolean = {
        false
      }
    }
    mock
  }

  def mockGetSecondaryIndexFromCarbon(): MockUp[CarbonIndexUtil.type] = {
    val mock: MockUp[CarbonIndexUtil.type ] = new MockUp[CarbonIndexUtil.type]() {
      @Mock
      def getSecondaryIndexes(carbonTable: CarbonTable): java.util.List[String] = {
        val x = new java.util.ArrayList[String]
        x.add("indextable1")
        x
      }
    }
    mock
  }

  def mockIsFileExists(): MockUp[CarbonUtil] = {
    val mock: MockUp[CarbonUtil] = new MockUp[CarbonUtil]() {
      @Mock
      def isFileExists(fileName: String): Boolean = {
        true
      }
    }
    mock
  }

  def mockCreateTable(): MockUp[CarbonCreateDataSourceTableCommand] = {
    val mock: MockUp[CarbonCreateDataSourceTableCommand] =
      new MockUp[CarbonCreateDataSourceTableCommand]() {
      @Mock
      def processMetadata(sparkSession: SparkSession): Seq[Row] = {
        throw new IOException("An exception occurred while creating index table.")
      }
    }
    mock
  }

  def mockDataHandler(): MockUp[CarbonFactDataHandlerColumnar] = {
    val mock: MockUp[CarbonFactDataHandlerColumnar] = new MockUp[CarbonFactDataHandlerColumnar]() {
      @Mock
      def finish(): Unit = {
        throw new CarbonDataWriterException ("An exception occurred while " +
            "writing data to SI table.")
      }
    }
    mock
  }

  def mockDataFileMerge(): MockUp[SecondaryIndexUtil.type] = {
    val mock: MockUp[SecondaryIndexUtil.type] = new MockUp[SecondaryIndexUtil.type ]() {
      @Mock
      def mergeDataFilesSISegments(segmentIdToLoadStartTimeMapping: scala.collection.mutable
      .Map[String, java.lang.Long],
       indexCarbonTable: CarbonTable,
       loadsToMerge: util.List[LoadMetadataDetails],
       carbonLoadModel: CarbonLoadModel,
       isRebuildCommand: Boolean = false)
      (sqlContext: SQLContext): Set[String] = {
        throw new RuntimeException("An exception occurred while merging data files in SI")
      }
    }
    mock
  }

  def mockLoadEventListner(): MockUp[SILoadEventListener] = {
    val mock: MockUp[SILoadEventListener] = new MockUp[SILoadEventListener]() {
      @Mock
      def onEvent(event: Event,
                  operationContext: OperationContext): Unit = {
        throw new RuntimeException("An exception occurred while loading data to SI table")
      }
    }
    mock
  }
}
