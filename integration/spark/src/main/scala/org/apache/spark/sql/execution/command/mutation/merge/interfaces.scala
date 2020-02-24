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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.util.LongAccumulator

/**
 * It describes the type of match like whenmatched or whennotmatched etc., it holds all the actions
 * to be done when this match passes.
 */
abstract class MergeMatch extends Serializable {

  var list: ArrayBuffer[MergeAction] = new ArrayBuffer[MergeAction]()

  def getExp: Option[Column]

  def addAction(action: MergeAction): MergeMatch = {
    list += action
    this
  }

  def getActions: List[MergeAction] = {
    list.toList
  }

  def updateActions(actions: List[MergeAction]): MergeMatch = {
    list = new ArrayBuffer[MergeAction]()
    list ++= actions
    this
  }
}

/**
 * It describes the type of action like update,delete or insert
 */
trait MergeAction extends Serializable

/**
 * It is the holder to keep all the matches and join condition.
 */
case class MergeDataSetMatches(joinExpr: Column, matchList: List[MergeMatch]) extends Serializable

case class WhenMatched(expression: Option[Column] = None) extends MergeMatch {
  override def getExp: Option[Column] = expression
}

case class WhenNotMatched(expression: Option[Column] = None) extends MergeMatch {
  override def getExp: Option[Column] = expression
}

case class WhenNotMatchedAndExistsOnlyOnTarget(expression: Option[Column] = None)
  extends MergeMatch {
  override def getExp: Option[Column] = expression
}

case class UpdateAction(updateMap: Map[Column, Column]) extends MergeAction

case class InsertAction(insertMap: Map[Column, Column]) extends MergeAction

/**
 * It inserts the history data into history table
 */
case class InsertInHistoryTableAction(insertMap: Map[Column, Column], historyTable: TableIdentifier)
  extends MergeAction

case class DeleteAction() extends MergeAction

case class Stats(insertedRows: LongAccumulator,
    updatedRows: LongAccumulator,
    deletedRows: LongAccumulator)
