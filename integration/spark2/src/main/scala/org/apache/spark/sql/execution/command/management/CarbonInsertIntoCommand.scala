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

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.DataCommand

import org.apache.carbondata.spark.util.CarbonSparkUtil

case class CarbonInsertIntoCommand(
    relation: CarbonDatasourceHadoopRelation,
    child: LogicalPlan,
    overwrite: Boolean,
    partition: Map[String, Option[String]])
  extends DataCommand {

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val df = Dataset.ofRows(sparkSession, child)
    val header = relation.tableSchema.get.fields.map(_.name).mkString(",")
    val load = CarbonLoadDataCommand(
      databaseNameOp = Some(relation.carbonRelation.databaseName),
      tableName = relation.carbonRelation.tableName,
      factPathFromUser = null,
      dimFilesPath = Seq(),
      options = scala.collection.immutable.Map("fileheader" -> header),
      isOverwriteTable = overwrite,
      inputSqlString = null,
      dataFrame = Some(df),
      updateModel = None,
      tableInfoOp = None,
      internalOptions = Map.empty,
      partition = partition).run(sparkSession)
    // updating relation metadata. This is in case of auto detect high cardinality
    relation.carbonRelation.metaData =
      CarbonSparkUtil.createSparkMeta(relation.carbonRelation.carbonTable)
    load
  }
}
