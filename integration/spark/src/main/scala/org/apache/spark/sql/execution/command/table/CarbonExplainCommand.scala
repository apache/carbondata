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

package org.apache.spark.sql.execution.command.table

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, Union}
import org.apache.spark.sql.execution.command.{ExplainCommand, MetadataCommand}
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.core.profiler.ExplainCollector

case class CarbonExplainCommand(
    child: LogicalPlan,
    override val output: Seq[Attribute] =
    Seq(AttributeReference("plan", StringType, nullable = true)()))
  extends MetadataCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val explainCommand = child.asInstanceOf[ExplainCommand]
    setAuditInfo(Map("query" -> explainCommand.logicalPlan.simpleString))
    val isCommand = explainCommand.logicalPlan match {
      case _: Command => true
      case Union(childern) if childern.forall(_.isInstanceOf[Command]) => true
      case _ => false
    }

    if (explainCommand.logicalPlan.isStreaming || isCommand) {
      explainCommand.run(sparkSession)
    } else {
      CarbonExplainCommand.collectProfiler(explainCommand, sparkSession) ++
      explainCommand.run(sparkSession)
    }
  }

  override protected def opName: String = "EXPLAIN"
}

case class CarbonInternalExplainCommand(
    explainCommand: ExplainCommand,
    override val output: Seq[Attribute] =
    Seq(AttributeReference("plan", StringType, nullable = true)()))
  extends MetadataCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    CarbonExplainCommand
      .collectProfiler(explainCommand, sparkSession) ++ explainCommand.run(sparkSession)
  }

  override protected def opName: String = "Carbon EXPLAIN"
}

object CarbonExplainCommand {
  def collectProfiler(
      explain: ExplainCommand,
      sparkSession: SparkSession): Seq[Row] = {
    try {
      ExplainCollector.setup()
      if (ExplainCollector.enabled()) {
        val queryExecution =
          sparkSession.sessionState.executePlan(explain.logicalPlan)
        queryExecution.toRdd.partitions
        // For count(*) queries the explain collector will be disabled, so profiler
        // informations not required in such scenarios.
        if (null == ExplainCollector.getFormatedOutput) {
          Seq.empty
        }
        Seq(Row("== CarbonData Profiler ==\n" + ExplainCollector.getFormatedOutput))
      } else {
        Seq.empty
      }
    } finally {
      ExplainCollector.remove()
    }
  }
}
