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

package org.apache.spark.sql.execution.command.management

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{Checker, DataCommand}
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.tool.CarbonCli

/**
 * CarbonCLi command class which is integrated to cli and sql support is provided via this class
 * @param databaseNameOp
 * @param tableName
 * @param commandOptions
 */
case class CarbonCliCommand(
    databaseNameOp: Option[String],
    tableName: String,
    commandOptions: String)
  extends DataCommand {

  override def output: Seq[Attribute] = {
      Seq(AttributeReference("CarbonCli", StringType, nullable = false)())
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    setAuditTable(carbonTable)
    setAuditInfo(Map("options" -> commandOptions))
    val commandArgs: Seq[String] = commandOptions.split("\\s+").map(_.trim)
    val finalCommands = commandArgs.exists(_.equalsIgnoreCase("-p")) match {
      case true =>
        commandArgs
      case false =>
        val needPath = commandArgs.exists { command =>
          command.equalsIgnoreCase("summary") || command.equalsIgnoreCase("benchmark")
        }
        needPath match {
          case true =>
            commandArgs ++ Seq("-p", carbonTable.getTablePath)
          case false =>
            commandArgs
        }
    }
    val summaryOutput = new util.ArrayList[String]()
    CarbonCli.run(finalCommands.toArray, summaryOutput, false)
    summaryOutput.asScala.map(x =>
      Row(x)
    )
  }

  override protected def opName: String = "CLI"
}
