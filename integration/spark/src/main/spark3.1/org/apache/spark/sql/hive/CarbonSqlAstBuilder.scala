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

package org.apache.spark.sql.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParserUtils.string
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.{AddTableColumnsContext, CreateTableContext, CreateTableLikeContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.parser.{CarbonHelperSqlAstBuilder, CarbonSpark2SqlParser, CarbonSparkSqlParserUtil}

class CarbonSqlAstBuilder(conf: SQLConf, parser: CarbonSpark2SqlParser, sparkSession: SparkSession)
  extends SparkSqlAstBuilder with SqlAstBuilderHelper {

  val helper = new CarbonHelperSqlAstBuilder(conf, parser, sparkSession)
 // CreateHiveTableContext
  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = {
    val fileStorage = CarbonSparkSqlParserUtil.getFileStorage(ctx.createTableClauses
        .createFileFormat(0))

    if (fileStorage.equalsIgnoreCase("'carbondata'") ||
        fileStorage.equalsIgnoreCase("carbondata") ||
        fileStorage.equalsIgnoreCase("'carbonfile'") ||
        fileStorage.equalsIgnoreCase("'org.apache.carbondata.format'")) {

      val createTableClauses = ctx.createTableClauses()
      val commentSpec = createTableClauses.commentSpec()
      val commentSpecContext = if (commentSpec.isEmpty) {
        null
      } else {
        string(commentSpec.get(0).STRING())
      }
      val createTableTuple = (ctx.createTableHeader, createTableClauses.skewSpec(0),
        createTableClauses.bucketSpec(0), createTableClauses.partitioning, ctx.colTypeList(),
        createTableClauses.tablePropertyList(0), createTableClauses.locationSpec(0),
        Option(commentSpecContext), ctx.AS, ctx.query, fileStorage)
      helper.createCarbonTable(createTableTuple)
    } else {
      super.visitCreateTable(ctx)
    }
  }

  override def visitAddTableColumns(ctx: AddTableColumnsContext): LogicalPlan = {
    visitAddTableColumns(parser, ctx)
  }
}
