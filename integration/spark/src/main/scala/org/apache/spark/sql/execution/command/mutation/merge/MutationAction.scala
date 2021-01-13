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
package org.apache.spark.sql.execution.command.mutation.merge

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.ExecutionErrors
import org.apache.spark.sql.execution.command.mutation.DeleteExecution

import org.apache.carbondata.core.index.Segment
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, SegmentUpdateDetails}
import org.apache.carbondata.processing.loading.FailureCauses

/**
 * It apply the mutations like update and delete delta on to the store.
 */
abstract class MutationAction(sparkSession: SparkSession, carbonTable: CarbonTable) {

  /**
   * The RDD of tupleIds and delta status will be processed here to write the delta on store
   */
  def handleAction(dataRDD: RDD[Row],
      executorErrors: ExecutionErrors,
      trxMgr: TranxManager): (util.List[SegmentUpdateDetails], util.HashSet[String])

  protected def handle(sparkSession: SparkSession,
      carbonTable: CarbonTable,
      factTimestamp: Long,
      dataRDD: RDD[Row],
      executorErrors: ExecutionErrors,
      condition: (Int) => Boolean): (util.List[SegmentUpdateDetails], util.HashSet[String]) = {
    val update = dataRDD.filter { row =>
      val status = row.get(1)
      status != null && condition(status.asInstanceOf[Int])
    }
    val tuple1 = DeleteExecution.deleteDeltaExecutionInternal(carbonTable,
      sparkSession, update,
      factTimestamp.toString,
      true, executorErrors, Some(0))
    MutationActionFactory.checkErrors(executorErrors)
    val tupleProcessed1 = DeleteExecution.processSegments(executorErrors, tuple1._1, tuple1._2)
    MutationActionFactory.checkErrors(executorErrors)
    (tupleProcessed1._1, tupleProcessed1._2)
  }

}

/**
 * It apply the update delta records to store in one transaction
 */
case class HandleUpdateAction(sparkSession: SparkSession, carbonTable: CarbonTable)
  extends MutationAction(sparkSession, carbonTable) {

  override def handleAction(dataRDD: RDD[Row],
      executorErrors: ExecutionErrors,
      trxMgr: TranxManager): (util.List[SegmentUpdateDetails], util.HashSet[String]) = {
    handle(sparkSession, carbonTable, trxMgr.getNextTransaction(this),
      dataRDD, executorErrors, (status) => (status == 101) || (status == 102))
  }
}

/**
 * It apply the delete delta records to store in one transaction
 */
case class HandleDeleteAction(sparkSession: SparkSession, carbonTable: CarbonTable)
  extends MutationAction(sparkSession, carbonTable) {

  override def handleAction(dataRDD: RDD[Row],
      executorErrors: ExecutionErrors,
      trxMgr: TranxManager): (util.List[SegmentUpdateDetails], util.HashSet[String]) = {
    handle(sparkSession, carbonTable, trxMgr.getNextTransaction(this),
      dataRDD, executorErrors, (status) => (status == 100) || (status == 102))
  }
}

/**
 * It apply the multiple mutations of delta records to store in multiple transactions.
 */
case class MultipleMutationAction(sparkSession: SparkSession,
    carbonTable: CarbonTable,
    mutations: Seq[MutationAction])
  extends MutationAction(sparkSession, carbonTable) {

  override def handleAction(dataRDD: RDD[Row],
      executorErrors: ExecutionErrors,
      trxMgr: TranxManager): (util.List[SegmentUpdateDetails], util.HashSet[String]) = {
    var (updates: util.List[SegmentUpdateDetails], segs: util.HashSet[String]) =
      (new util.ArrayList[SegmentUpdateDetails], new util.HashSet[String])
    mutations.foreach { m =>
      val (l, r) = m.handleAction(dataRDD, executorErrors, trxMgr)
      l.asScala.foreach { entry =>
        CarbonUpdateUtil.mergeSegmentUpdate(false, updates, entry)
      }
      segs.addAll(r)
    }
    (updates, segs)
  }
}

/**
 * It apply the delete and update delta records to store in a single transaction
 */
case class HandleUpdateAndDeleteAction(sparkSession: SparkSession, carbonTable: CarbonTable)
  extends MutationAction(sparkSession, carbonTable) {

  override def handleAction(dataRDD: RDD[Row],
      executorErrors: ExecutionErrors,
      trxMgr: TranxManager): (util.List[SegmentUpdateDetails], util.HashSet[String]) = {
    handle(sparkSession, carbonTable, trxMgr.getNextTransaction(this),
      dataRDD, executorErrors, (status) => (status == 100) || (status == 101) || (status == 102))
  }
}

object MutationActionFactory {

  /**
   * It is a factory method to generate a respective mutation action for update and delete.
   */
  def getMutationAction(sparkSession: SparkSession,
      carbonTable: CarbonTable,
      hasDelAction: Boolean,
      hasUpAction: Boolean,
      hasInsertHistUpd: Boolean,
      hasInsertHistDel: Boolean): MutationAction = {
    var actions = Seq.empty[MutationAction]
    // If the merge has history insert action then write the delete delta in two separate actions.
    // As it is needed to know which are deleted records and which are insert records.
    if (hasInsertHistDel || hasInsertHistUpd) {
      if (hasUpAction) {
        actions ++= Seq(HandleUpdateAction(sparkSession, carbonTable))
      }
      if (hasDelAction) {
        actions ++= Seq(HandleDeleteAction(sparkSession, carbonTable))
      }
    } else {
      // If there is no history insert action then apply it in single flow.
      actions ++= Seq(HandleUpdateAndDeleteAction(sparkSession, carbonTable))
    }
    if (actions.length == 1) {
      actions.head
    } else {
      // If it has multiple actions to apply then combine to multi action.
      MultipleMutationAction(sparkSession, carbonTable, actions)
    }
  }

  def checkErrors(executorErrors: ExecutionErrors): Unit = {
    // Check for any failures occurred during delete delta execution
    if (executorErrors.failureCauses != FailureCauses.NONE) {
      throw new CarbonMergeDataSetException(executorErrors.errorMsg)
    }
  }
}
