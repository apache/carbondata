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

import scala.collection.JavaConverters._

import org.apache.spark.CarbonInputMetrics
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, Dataset, Row, SparkSession}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.DataTypeConverterImpl
import org.apache.carbondata.hadoop.CarbonProjection
import org.apache.carbondata.spark.rdd.CarbonDeltaRowScanRDD
import org.apache.carbondata.spark.readsupport.SparkGenericRowReadSupportImpl

object HistoryTableLoadHelper {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  /**
   * Load to history table by reading the data from target table using the last transactions. So
   * here we read the deleted data from target table by using delta and load them to history table.
   */
  def loadHistoryTable(sparkSession: SparkSession,
      rltn: CarbonDatasourceHadoopRelation,
      carbonTable: CarbonTable,
      trxMgr: TranxManager,
      mutationAction: MutationAction,
      mergeMatches: MergeDataSetMatches): Unit = {
    if (!mutationAction.isInstanceOf[HandleUpdateAndDeleteAction]) {
      val insert = mergeMatches
        .matchList
        .filter { f =>
          f.getActions.exists(_.isInstanceOf[InsertInHistoryTableAction])
        }
        .head
        .getActions
        .find(_.isInstanceOf[InsertInHistoryTableAction])
        .get
        .asInstanceOf[InsertInHistoryTableAction]
      // Get the history table dataframe.
      val histDataFrame: Dataset[Row] = sparkSession.table(insert.historyTable)
      // check if the user wants to insert update history records into history table.
      val updateDataFrame = if (trxMgr.getUpdateTrx != -1) {
        // Get the insertHistoryAction related to update action.
        val insertHist = mergeMatches.matchList.filter { f =>
          f.getActions.exists(_.isInstanceOf[InsertInHistoryTableAction]) &&
          f.getActions.exists(_.isInstanceOf[UpdateAction])
        }.head.getActions.filter(_.isInstanceOf[InsertInHistoryTableAction]).head.
          asInstanceOf[InsertInHistoryTableAction]
        // Create the dataframe to fetch history updated records.
        Some(createHistoryDataFrame(sparkSession, rltn, carbonTable, insertHist,
          histDataFrame, trxMgr.getUpdateTrx))
      } else {
        None
      }
      // check if the user wants to insert delete history records into history table.
      val delDataFrame = if (trxMgr.getDeleteTrx != -1) {
        val insertHist = mergeMatches.matchList.filter { f =>
          f.getActions.exists(_.isInstanceOf[InsertInHistoryTableAction]) &&
          f.getActions.exists(_.isInstanceOf[DeleteAction])
        }.head.getActions.filter(_.isInstanceOf[InsertInHistoryTableAction]).head.
          asInstanceOf[InsertInHistoryTableAction]
        Some(createHistoryDataFrame(sparkSession, rltn, carbonTable, insertHist,
          histDataFrame: Dataset[Row], trxMgr.getDeleteTrx))
      } else {
        None
      }

      val unionDf = (updateDataFrame, delDataFrame) match {
        case (Some(u), Some(d)) => u.union(d)
        case (Some(u), None) => u
        case (None, Some(d)) => d
        case _ => throw new CarbonMergeDataSetException("Some thing is wrong")
      }

      val alias = carbonTable.getTableName + System.currentTimeMillis()
      unionDf.createOrReplaceTempView(alias)
      val start = System.currentTimeMillis()
      sparkSession.sql(s"insert into ${ insert.historyTable.quotedString } " +
                       s"select * from ${ alias }")
      LOGGER.info("Time taken to insert into history table " + (System.currentTimeMillis() - start))
    }
  }

  /**
   * It creates the dataframe to fetch deleted/updated records in the particular transaction.
   */
  private def createHistoryDataFrame(sparkSession: SparkSession,
      rltn: CarbonDatasourceHadoopRelation,
      carbonTable: CarbonTable,
      insertHist: InsertInHistoryTableAction,
      histDataFrame: Dataset[Row],
      factTimestamp: Long) = {
    val rdd1 = new CarbonDeltaRowScanRDD[Row](sparkSession,
      carbonTable.getTableInfo.serialize(),
      carbonTable.getTableInfo,
      null,
      new CarbonProjection(
        carbonTable.getCreateOrderColumn().asScala.map(_.getColName).toArray),
      null,
      carbonTable.getAbsoluteTableIdentifier,
      new CarbonInputMetrics,
      classOf[DataTypeConverterImpl],
      classOf[SparkGenericRowReadSupportImpl],
      factTimestamp.toString)

    val frame1 = sparkSession.createDataFrame(rdd1, rltn.carbonRelation.schema)
    val histOutput = histDataFrame.queryExecution.analyzed.output
    val cols = histOutput.map { a =>
      insertHist.insertMap.find(p => p._1.toString().equalsIgnoreCase(a.name)) match {
        case Some((k, v)) => v
        case _ =>
          throw new CarbonMergeDataSetException(
            " All columns of history table are mapped in " + insertHist)
      }
    }
    frame1.select(cols: _*)
  }

}
