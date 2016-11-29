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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.scan.model._
import org.apache.carbondata.spark.CarbonFilters
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.hive.CarbonRelation

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

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
    plan.setSortedDimemsions(new java.util.ArrayList[QueryDimension])
    plan.setOutLocationPath(
      CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS))
    processFilterExpressions(plan)
    plan
  }

  def processFilterExpressions(plan: CarbonQueryPlan) {
    if (dimensionPredicatesRaw.nonEmpty) {
      val expressionVal = CarbonFilters.processExpression(
        dimensionPredicatesRaw,
        attributesNeedToDecode,
        unprocessedExprs,
        carbonTable)
      expressionVal match {
        case Some(ce) =>
          // adding dimension used in expression in querystats
          plan.setFilterExpression(ce)
        case _ =>
      }
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

    val dimensions = carbonTable.getDimensionByTableName(carbonTable.getFactTableName)
    val measures = carbonTable.getMeasureByTableName(carbonTable.getFactTableName)
    val dimAttr = new Array[Attribute](dimensions.size())
    val msrAttr = new Array[Attribute](measures.size())
    attributesRaw.foreach { attr =>
      val carbonDimension =
        carbonTable.getDimensionByName(carbonTable.getFactTableName, attr.name)
      if(carbonDimension != null) {
        dimAttr(dimensions.indexOf(carbonDimension)) = attr
      } else {
        val carbonMeasure =
          carbonTable.getMeasureByName(carbonTable.getFactTableName, attr.name)
        if(carbonMeasure != null) {
          msrAttr(measures.indexOf(carbonMeasure)) = attr
        }
      }
    }

    attributesRaw = dimAttr.filter(f => f != null) ++ msrAttr.filter(f => f != null)

    var queryOrder: Integer = 0
    attributesRaw.foreach { attr =>
      val carbonDimension =
        carbonTable.getDimensionByName(carbonTable.getFactTableName, attr.name)
      if (carbonDimension != null) {
        val dim = new QueryDimension(attr.name)
        dim.setQueryOrder(queryOrder)
        queryOrder = queryOrder + 1
        selectedDims += dim
      } else {
        val carbonMeasure =
          carbonTable.getMeasureByName(carbonTable.getFactTableName, attr.name)
        if (carbonMeasure != null) {
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
