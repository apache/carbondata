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

package org.apache.spark.sql.parser

import org.apache.spark.sql.{CarbonEnv, CarbonThreadUtil, CarbonToSparkAdapter, SparkSession}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.spark.util.CarbonScalaUtil

/**
 * parser order: carbon parser => spark parser
 */
class CarbonExtensionSqlParser(
    conf: SQLConf,
    sparkSession: SparkSession,
    initialParser: ParserInterface
) extends SparkSqlParser {

  val parser = new CarbonExtensionSpark2SqlParser
  val antlrParser = new CarbonAntlrParser(this)

  override def parsePlan(sqlText: String): LogicalPlan = {
    parser.synchronized {
      CarbonEnv.getInstance(sparkSession)
    }
    CarbonThreadUtil.updateSessionInfoToCurrentThread(sparkSession)
    try {
      parser.parse(sqlText)
    } catch {
      case ce: MalformedCarbonCommandException =>
        throw ce
      case ct: Throwable =>
        try {
          antlrParser.parse(sqlText)
        } catch {
          case ce: MalformedCarbonCommandException =>
            throw ce
          case at: Throwable =>
            try {
              val parsedPlan = CarbonToSparkAdapter.getUpdatedPlan(initialParser.parsePlan(sqlText),
                sqlText)
              CarbonScalaUtil.cleanParserThreadLocals
              parsedPlan
            } catch {
              case mce: MalformedCarbonCommandException =>
                throw mce
              case st: Throwable =>
                st.printStackTrace(System.err)
                CarbonScalaUtil.cleanParserThreadLocals
                CarbonException.analysisException(
                  s"""== Spark Parser: ${initialParser.getClass.getName} ==
                     |${st.getMessage}
                     |== Carbon Parser: ${ parser.getClass.getName } ==
                     |${ct.getMessage}
                     |== Antlr Parser: ${antlrParser.getClass.getName} ==
                     |${at.getMessage}
               """.stripMargin.trim)
            }
        }
    }
  }
}
