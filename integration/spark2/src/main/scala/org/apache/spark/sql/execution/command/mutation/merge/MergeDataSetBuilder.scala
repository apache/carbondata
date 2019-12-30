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

import org.apache.spark.sql.{AnalysisException, Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.expr

/**
 * Builder class to generate and execute merge
 */
class MergeDataSetBuilder(existingDsOri: Dataset[Row], currDs: Dataset[Row],
    joinExpr: Column, sparkSession: SparkSession) {

  def this(existingDsOri: Dataset[Row], currDs: Dataset[Row],
      joinExpr: String, sparkSession: SparkSession) {
    this(existingDsOri, currDs, expr(joinExpr), sparkSession)
  }

  val matchList: util.List[MergeMatch] = new util.ArrayList[MergeMatch]()

  def whenMatched(): MergeDataSetBuilder = {
    matchList.add(WhenMatched())
    this
  }

  def whenMatched(expression: String): MergeDataSetBuilder = {
    matchList.add(WhenMatched(Some(expr(expression))))
    this
  }

  def whenMatched(expression: Column): MergeDataSetBuilder = {
    matchList.add(WhenMatched(Some(expression)))
    this
  }

  def whenNotMatched(): MergeDataSetBuilder = {
    matchList.add(WhenNotMatched())
    this
  }

  def whenNotMatched(expression: String): MergeDataSetBuilder = {
    matchList.add(WhenNotMatched(Some(expr(expression))))
    this
  }

  def whenNotMatched(expression: Column): MergeDataSetBuilder = {
    matchList.add(WhenNotMatched(Some(expression)))
    this
  }

  def whenNotMatchedAndExistsOnlyOnTarget(): MergeDataSetBuilder = {
    matchList.add(WhenNotMatchedAndExistsOnlyOnTarget())
    this
  }

  def whenNotMatchedAndExistsOnlyOnTarget(expression: String): MergeDataSetBuilder = {
    matchList.add(WhenNotMatchedAndExistsOnlyOnTarget(Some(expr(expression))))
    this
  }

  def whenNotMatchedAndExistsOnlyOnTarget(expression: Column): MergeDataSetBuilder = {
    matchList.add(WhenNotMatchedAndExistsOnlyOnTarget(Some(expression)))
    this
  }

  def updateExpr(expression: Map[Any, Any]): MergeDataSetBuilder = {
    checkBuilder
    matchList.get(matchList.size() - 1).addAction(UpdateAction(convertMap(expression)))
    this
  }

  def insertExpr(expression: Map[Any, Any]): MergeDataSetBuilder = {
    checkBuilder
    matchList.get(matchList.size() - 1).addAction(InsertAction(convertMap(expression)))
    this
  }

  def delete(): MergeDataSetBuilder = {
    checkBuilder
    matchList.get(matchList.size() - 1).addAction(DeleteAction())
    this
  }

  def build(): CarbonMergeDataSetCommand = {
    checkBuilder
    CarbonMergeDataSetCommand(existingDsOri, currDs,
      MergeDataSetMatches(joinExpr, matchList.asScala.toList))
  }

  def execute(): Unit = {
    build().run(sparkSession)
  }

  private def convertMap(exprMap: Map[Any, Any]): Map[Column, Column] = {
    if (exprMap.exists{ case (k, v) =>
      !(checkType(k) && checkType(v))
    }) {
      throw new AnalysisException(
        "Expression map should only contain either String or Column " + exprMap)
    }
    def checkType(obj: Any) = obj.isInstanceOf[String] || obj.isInstanceOf[Column]
    def convert(obj: Any) =
      if (obj.isInstanceOf[Column]) obj.asInstanceOf[Column] else expr(obj.toString)
    exprMap.map{ case (k, v) =>
      (convert(k), convert(v))
    }
  }

  private def checkBuilder(): Unit = {
    if (matchList.size() == 0) {
      throw new AnalysisException("Atleast one matcher should be called before calling an action")
    }
  }

}

