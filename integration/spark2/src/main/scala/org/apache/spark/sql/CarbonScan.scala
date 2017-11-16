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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.optimizer.CarbonFilters

import org.apache.carbondata.core.scan.model._

case class CarbonScan(
    var attributesRaw: Seq[Attribute],
    relationRaw: CarbonRelation,
    dimensionPredicatesRaw: Seq[Expression],
    useUnsafeCoversion: Boolean = true) {
  val carbonTable = relationRaw.metaData.carbonTable
  val selectedDims = scala.collection.mutable.MutableList[QueryDimension]()
  val selectedMsrs = scala.collection.mutable.MutableList[QueryMeasure]()

  val attributesNeedToDecode = new java.util.LinkedHashSet[AttributeReference]()
  val unprocessedExprs = new ArrayBuffer[Expression]()

  val buildCarbonPlan: CarbonQueryPlan = {
    val plan: CarbonQueryPlan = new CarbonQueryPlan(relationRaw.databaseName, relationRaw.tableName)
    processFilterExpressions(plan)
    plan
  }

  def processFilterExpressions(plan: CarbonQueryPlan) {
    if (dimensionPredicatesRaw.nonEmpty) {
      val exps = CarbonFilters.preProcessExpressions(dimensionPredicatesRaw)
      val expressionVal = CarbonFilters.transformExpression(exps.head)
      plan.setFilterExpression(expressionVal)
    }
    processExtraAttributes(plan)
  }

  private def processExtraAttributes(plan: CarbonQueryPlan) {
    if (attributesNeedToDecode.size() > 0) {
      val attributeOut = new ArrayBuffer[Attribute]() ++ attributesRaw

      attributesNeedToDecode.asScala.foreach { attr =>
        if (!attributesRaw.exists(_.name.equalsIgnoreCase(attr.name))) {
          attributeOut += attr
        }
      }
      attributesRaw = attributeOut
    }

    val columns = carbonTable.getCreateOrderColumn(carbonTable.getTableName)
    val colAttr = new Array[Attribute](columns.size())
    attributesRaw.foreach { attr =>
    val column =
        carbonTable.getColumnByName(carbonTable.getTableName, attr.name)
      if(column != null) {
        colAttr(columns.indexOf(column)) = attr
       }
    }
    attributesRaw = colAttr.filter(f => f != null)

    var queryOrder: Integer = 0
    attributesRaw.foreach { attr =>
      val carbonColumn = carbonTable.getColumnByName(carbonTable.getTableName, attr.name)
      if (carbonColumn != null) {
        if (carbonColumn.isDimension()) {
          val dim = new QueryDimension(attr.name)
          dim.setQueryOrder(queryOrder)
          queryOrder = queryOrder + 1
          selectedDims += dim
         } else {
          val m1 = new QueryMeasure(attr.name)
          m1.setQueryOrder(queryOrder)
          queryOrder = queryOrder + 1
          selectedMsrs += m1
        }
      }
    }

    // Fill the selected dimensions & measures obtained from
    // attributes to query plan  for detailed query
    selectedDims.foreach(plan.addDimension)
    selectedMsrs.foreach(plan.addMeasure)
  }

}
