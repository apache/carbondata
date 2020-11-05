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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.command.AtomicRunnableCommand
import org.apache.spark.sql.execution.command.mutation.merge._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.merge.model.{CarbonMergeIntoModel, TableModel}
import org.apache.spark.util.SparkUtil._
import org.apache.spark.util.TableAPIUtil

case class CarbonMergeIntoSQLCommand(mergeInto: CarbonMergeIntoModel)
  extends AtomicRunnableCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val sourceTable: TableModel = mergeInto.getSource
    val targetTable: TableModel = mergeInto.getTarget
    val mergeCondition: Expression = mergeInto.getMergeCondition
    val mergeExpression: Seq[Expression] = convertExpressionList(mergeInto.getMergeExpressions)
    val mergeActions: Seq[MergeAction] = convertMergeActionList(mergeInto.getMergeActions)

    // validate the table
    val sourceDatabaseName =
      CarbonEnv.getDatabaseName(Option(sourceTable.getDatabase))(sparkSession)
    val sourceTableName = sourceTable.getTable
    val targetDatabaseName =
      CarbonEnv.getDatabaseName(Option(targetTable.getDatabase))(sparkSession)
    val targetTableName = targetTable.getTable
    TableAPIUtil.validateTableExists(sparkSession,
      sourceDatabaseName,
      sourceTableName)
    TableAPIUtil.validateTableExists(sparkSession,
      targetDatabaseName,
      targetTableName)

    val srcDf = sparkSession.sql(s"""SELECT * FROM ${sourceDatabaseName}.${sourceTableName}""")
    val tgDf = sparkSession.sql(s"""SELECT * FROM ${targetDatabaseName}.${targetTableName}""")

    var matches = scala.collection.mutable.ArrayBuffer[MergeMatch]()
    val mergeExpLength: Int = mergeExpression.length

    // This for loop will gather the match condition and match action to build the MergeMatch
    for (x <- 0 until mergeExpLength) {
      val currExpression: Expression = mergeExpression.apply(x)
      val currAction: MergeAction = mergeActions.apply(x)
      // Use pattern matching to convert current actions to Map
      // Since the delete action will delete the whole line, we don't need to build map here
      currAction match {
        case action: UpdateAction =>
          if (action.isStar) {
            val srcCols = srcDf.columns
            val tgCols = tgDf.columns
            action.updateMap = Map[Column, Column]()
            for (i <- srcCols.indices) {
              action.updateMap
                .+=(col(tgCols.apply(i)) ->
                    col(mergeInto.getSource.getTable + "." + srcCols.apply(i)))
            }
          }
        case action: InsertAction =>
          if (action.isStar) {
            val srcCols = srcDf.columns
            val tgCols = tgDf.columns
            action.insertMap = Map[Column, Column]()
            for (i <- srcCols.indices) {
              action.insertMap
                .+=(col(tgCols.apply(i)) ->
                    col(mergeInto.getSource.getTable + "." + srcCols.apply(i)))
            }
          }
        case _ =>
      }
      if (currExpression == null) {
        // According to the merge actions to re-generate matches
        if (currAction.isInstanceOf[DeleteAction] || currAction.isInstanceOf[UpdateAction]) {
          matches ++= Seq(WhenMatched().addAction(currAction))
        } else {
          matches ++= Seq(WhenNotMatched().addAction(currAction))
        }
      } else {
        // Since the mergeExpression is not null, we need to Initialize the
        // WhenMatched/WhenNotMatched with the Expression
        val carbonMergeExpression: Option[Column] = Option(Column(currExpression))
        if (currAction.isInstanceOf[DeleteAction] || currAction.isInstanceOf[UpdateAction]) {
          matches ++= Seq(WhenMatched(carbonMergeExpression).addAction(currAction))
        } else {
          matches ++= Seq(WhenNotMatched(carbonMergeExpression).addAction(currAction))
        }
      }
    }
    val joinExpression = Column(mergeCondition)
    val mergeDataSetMatches: MergeDataSetMatches = MergeDataSetMatches(joinExpression,
      matches.toList)

    CarbonMergeDataSetCommand(tgDf, srcDf, mergeDataSetMatches).run(sparkSession)
  }

  override protected def opName: String = "MERGE SQL COMMAND"
}
