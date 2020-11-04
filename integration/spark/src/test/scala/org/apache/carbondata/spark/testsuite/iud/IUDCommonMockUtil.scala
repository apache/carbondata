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
package org.apache.carbondata.spark.testsuite.iud

import mockit.{Mock, MockUp}
import org.apache.spark.sql.{Dataset, Row, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{ExecutionErrors, UpdateTableModel}
import org.apache.spark.sql.execution.command.mutation.{CarbonProjectForUpdateCommand, HorizontalCompaction}
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.core.index.Segment
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, DeleteDeltaBlockDetails, SegmentUpdateDetails}
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails
import org.apache.carbondata.core.view.{MVManager, MVSchema}
import org.apache.carbondata.core.writer.CarbonDeleteDeltaWriterImpl
import org.apache.carbondata.events.OperationContext
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory

object IUDCommonMockUtil extends QueryTest {
  def wrappedSqlText(sqlText: String): Unit = {
    try {
      sql(sqlText).collect()
    } catch {
      case ex: Exception =>
    }
  }

  def mockWriteTableStatusFailure(sqlText: String) {
    val mock = new MockUp[CarbonUpdateUtil]() {
      @Mock
      def updateTableMetadataStatus(updatedSegmentsList: java.util.Set[String],
          table: CarbonTable,
          updatedTimeStamp: String,
          isTimestampUpdateRequired: Boolean,
          isUpdateStatusFileUpdateRequired: Boolean,
          segmentsToBeDeleted: java.util.Set[String],
          segmentFilesTobeUpdated: java.util.Set[String],
          uuid: String,
          newLoadEntry: LoadMetadataDetails): Boolean = {
        return false
      }
    }
    wrappedSqlText(sqlText)
    mock.tearDown()
  }

  def mockWriteUpdateTableStatusFailure(sqlText: String) {
    val mock = new MockUp[CarbonUpdateUtil]() {
      @Mock
      def updateSegmentStatus(updateDetailsList: java.util.List[SegmentUpdateDetails],
          table: CarbonTable,
          updateStatusFileIdentifier: String,
          isCompaction: Boolean): Boolean = {
        return false
      }
    }
    wrappedSqlText(sqlText)
    mock.tearDown()
  }

  def mockInsertDataFailure(exception: Exception, sqlText: String) {
    val mock = new MockUp[CarbonProjectForUpdateCommand]() {
      @Mock
      def performUpdate(dataFrame: Dataset[Row],
          databaseNameOp: Option[String],
          tableName: String,
          plan: LogicalPlan,
          sparkSession: SparkSession,
          updateTableModel: UpdateTableModel,
          executorErrors: ExecutionErrors): Unit = {
        throw exception
      }
    }
    wrappedSqlText(sqlText)
    mock.tearDown()
  }

  def mockWriteDeleteDeltaFailure(exception: Exception, sqlText: String) {
    val mock = new MockUp[CarbonDeleteDeltaWriterImpl]() {
      @Mock
      def write(deleteDeltaBlockDetails: DeleteDeltaBlockDetails): Unit = {
        throw exception
      }
    }
    wrappedSqlText(sqlText)
    mock.tearDown()
  }

  def mockMVRefreshFailure(exception: Exception, sqlText: String) {
    val mock = new MockUp[MVManager]() {
      @Mock
      def onTruncate(schemas: java.util.List[MVSchema]): Unit = {
        throw exception
      }
    }
    wrappedSqlText(sqlText)
    mock.tearDown()
  }

  def mockMinorCompactionFailure(exception: Exception, sqlText: String) {
    val mock = new MockUp[CarbonDataRDDFactory.type]() {
      @Mock
      def handleSegmentMerging(sqlContext: SQLContext,
          carbonLoadModel: CarbonLoadModel,
          carbonTable: CarbonTable,
          compactedSegments: java.util.List[String],
          operationContext: OperationContext): Unit = {
        throw exception
      }
    }
    wrappedSqlText(sqlText)
    mock.tearDown()
  }


  def mockHorizontalCompactionFailure(exception: Exception, sqlText: String) {
    val mock = new MockUp[HorizontalCompaction.type]() {
      @Mock
      def tryHorizontalCompaction(sparkSession: SparkSession, carbonTable: CarbonTable,
          updatedSegmentList: Set[String]): Unit = {
        throw exception
      }
    }
    wrappedSqlText(sqlText)
    mock.tearDown()
  }
}
