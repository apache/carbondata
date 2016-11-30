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

import java.util.ArrayList

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.LeafNode
import org.apache.spark.sql.hive.CarbonMetastore

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.scan.model._
import org.apache.carbondata.spark.CarbonFilters
import org.apache.carbondata.spark.rdd.CarbonScanRDD

case class CarbonScan(
    var columnProjection: Seq[Attribute],
    relationRaw: CarbonRelation,
    dimensionPredicatesRaw: Seq[Expression],
    useUnsafeCoversion: Boolean = true)(@transient val ocRaw: SQLContext) extends LeafNode {
  val carbonTable = relationRaw.metaData.carbonTable
  val selectedDims = scala.collection.mutable.MutableList[QueryDimension]()
  val selectedMsrs = scala.collection.mutable.MutableList[QueryMeasure]()
  @transient val carbonCatalog = ocRaw.catalog.asInstanceOf[CarbonMetastore]

  val attributesNeedToDecode = new java.util.LinkedHashSet[AttributeReference]()
  val unprocessedExprs = new ArrayBuffer[Expression]()

  val buildCarbonPlan: CarbonQueryPlan = {
    val plan: CarbonQueryPlan = new CarbonQueryPlan(relationRaw.databaseName, relationRaw.tableName)

    plan.setSortedDimemsions(new ArrayList[QueryDimension])

    plan.setOutLocationPath(
      CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS))
    plan.setQueryId(ocRaw.getConf("queryId", System.nanoTime() + ""))
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
      val attributeOut = new ArrayBuffer[Attribute]() ++ columnProjection

      attributesNeedToDecode.asScala.foreach { attr =>
        if (!columnProjection.exists(_.name.equalsIgnoreCase(attr.name))) {
          attributeOut += attr
        }
      }
      columnProjection = attributeOut
    }

    val dimensions = carbonTable.getDimensionByTableName(carbonTable.getFactTableName)
    val measures = carbonTable.getMeasureByTableName(carbonTable.getFactTableName)
    val dimAttr = new Array[Attribute](dimensions.size())
    val msrAttr = new Array[Attribute](measures.size())
    columnProjection.foreach { attr =>
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

    columnProjection = dimAttr.filter(f => f != null) ++ msrAttr.filter(f => f != null)

    var queryOrder: Integer = 0
    columnProjection.foreach { attr =>
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

  def inputRdd: CarbonScanRDD[Array[Any]] = {
    new CarbonScanRDD(
      ocRaw.sparkContext,
      columnProjection,
      buildCarbonPlan.getFilterExpression,
      carbonTable.getAbsoluteTableIdentifier,
      carbonTable
    )
  }

  override def outputsUnsafeRows: Boolean =
    (attributesNeedToDecode.size() == 0) && useUnsafeCoversion

  override def doExecute(): RDD[InternalRow] = {
    val outUnsafeRows: Boolean = (attributesNeedToDecode.size() == 0) && useUnsafeCoversion
    inputRdd.mapPartitions { iter =>
      val unsafeProjection = UnsafeProjection.create(output.map(_.dataType).toArray)
      new Iterator[InternalRow] {
        override def hasNext: Boolean = iter.hasNext

        override def next(): InternalRow = {
          val value = iter.next
          if (outUnsafeRows) {
            unsafeProjection(new GenericMutableRow(value))
          } else {
            new GenericMutableRow(value)
          }
        }
      }
    }
  }

  def output: Seq[Attribute] = {
    columnProjection
  }

}
