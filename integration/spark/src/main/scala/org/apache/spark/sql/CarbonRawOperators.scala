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

import java.util
import java.util.ArrayList

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.LeafNode
import org.apache.spark.sql.hive.CarbonMetastoreCatalog
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.UTF8String

import org.carbondata.core.carbon.{AbsoluteTableIdentifier}
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.carbondata.query.carbon.model._
import org.carbondata.query.carbon.result.BatchRawResult
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper
import org.carbondata.spark.{CarbonFilters, RawKeyVal, RawKeyValImpl}
import org.carbondata.spark.rdd.CarbonRawQueryRDD


case class CarbonRawTableScan(
    var attributesRaw: Seq[Attribute],
    relationRaw: CarbonRelation,
    dimensionPredicatesRaw: Seq[Expression],
    aggExprsRaw: Option[Seq[Expression]],
    useBinaryAggregator: Boolean)(@transient val ocRaw: SQLContext) extends LeafNode
{
  val carbonTable = relationRaw.metaData.carbonTable
  val selectedDims = scala.collection.mutable.MutableList[QueryDimension]()
  val selectedMsrs = scala.collection.mutable.MutableList[QueryMeasure]()
  @transient val carbonCatalog = ocRaw.catalog.asInstanceOf[CarbonMetastoreCatalog]

  val attributesNeedToDecode = new util.HashSet[AttributeReference]()
  val unprocessedExprs = new ArrayBuffer[Expression]()

  val buildCarbonPlan: CarbonQueryPlan = {
    val plan: CarbonQueryPlan = new CarbonQueryPlan(relationRaw.schemaName, relationRaw.tableName)

    val dimensions = carbonTable.getDimensionByTableName(carbonTable.getFactTableName)
    val measures = carbonTable.getMeasureByTableName(carbonTable.getFactTableName)
    val dimAttr = new Array[Attribute](dimensions.size())
    val msrAttr = new Array[Attribute](measures.size())
    attributesRaw.map { attr =>
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

    attributesRaw = (dimAttr.filter(f => f != null)) ++ (msrAttr.filter(f => f != null))

    var queryOrder: Integer = 0
    attributesRaw.map { attr =>
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
    // Just find out that any aggregation functions are present on dimensions.
    aggExprsRaw match {
      case Some(aggExprs) =>
        aggExprs.foreach {
          case Alias(agg: AggregateExpression1, name) =>
            agg.collect {
              case attr: AttributeReference =>
                val dims = selectedDims.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
                if(dims.nonEmpty) {
                  plan.addAggDimAggInfo(dims.head.getColumnName,
                    dims.head.getAggregateFunction,
                    dims.head.getQueryOrder)
                }
            }
          case _ =>
        }
      case _ =>
    }

    // Fill the selected dimensions & measures obtained from
    // attributes to query plan  for detailed query
    selectedDims.foreach(plan.addDimension)
    selectedMsrs.foreach(plan.addMeasure)

    plan.setSortedDimemsions(new ArrayList[QueryDimension])

    plan.setRawDetailQuery(true)
    plan.setOutLocationPath(
      CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS))
    plan.setQueryId(System.nanoTime() + "")
    processFilterExpressions(plan)
    plan
  }

  def processFilterExpressions(plan: CarbonQueryPlan) {
    if (dimensionPredicatesRaw.nonEmpty) {
      val expressionVal = CarbonFilters
        .processExpression(dimensionPredicatesRaw, attributesNeedToDecode, unprocessedExprs)
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

      attributesNeedToDecode.asScala.map { attr =>
        val dims = plan.getDimensions.asScala.filter(f => f.getColumnName.equals(attr.name))
        val msrs = plan.getMeasures.asScala.filter(f => f.getColumnName.equals(attr.name))
        var order = plan.getDimensions.size() + plan.getMeasures.size()
        if (dims.isEmpty && msrs.isEmpty) {
          val dimension = carbonTable.getDimensionByName(carbonTable.getFactTableName, attr.name)
          if (dimension != null) {
            val qDim = new QueryDimension(dimension.getColName)
            qDim.setQueryOrder(order)
            plan.addDimension(qDim)
            attributeOut += attr
            order += 1
          } else {
            val measure = carbonTable.getMeasureByName(carbonTable.getFactTableName, attr.name)
            if (measure != null) {
              val qMsr = new QueryMeasure(measure.getColName)
              qMsr.setQueryOrder(order)
              plan.addMeasure(qMsr)
              order += 1
              attributeOut += attr
            }
          }
        }
      }
      attributesRaw = attributeOut
    }
  }


  def inputRdd: CarbonRawQueryRDD[BatchRawResult, Any] = {

    val conf = new Configuration()
    val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
    buildCarbonPlan.getDimAggregatorInfos.clear()
    val model = QueryModel.createModel(
      absoluteTableIdentifier, buildCarbonPlan, carbonTable)
    val kv: RawKeyVal[BatchRawResult, Any] = new RawKeyValImpl()
    // setting queryid
    buildCarbonPlan.setQueryId(ocRaw.getConf("queryId", System.nanoTime() + ""))

    val cubeCreationTime = carbonCatalog
      .getCubeCreationTime(relationRaw.schemaName, relationRaw.tableName)
    val schemaLastUpdatedTime = carbonCatalog
      .getSchemaLastUpdatedTime(relationRaw.schemaName, relationRaw.tableName)
    val big = new CarbonRawQueryRDD(
      ocRaw.sparkContext,
      model,
      buildCarbonPlan.getFilterExpression,
      kv,
      conf,
      cubeCreationTime,
      schemaLastUpdatedTime,
      carbonCatalog.storePath)
    big
  }

  override def doExecute(): RDD[InternalRow] = {
    def toType(obj: Any): Any = {
      obj match {
        case s: String => UTF8String.fromString(s)
        case _ => obj
      }
    }

    if (useBinaryAggregator) {
      inputRdd.map { row =>
        //      val dims = row._1.map(toType)
        new CarbonRawMutableRow(row._1.getAllRows, row._1.getQuerySchemaInfo)
      }
    } else {
      inputRdd.flatMap { row =>
        val buffer = new ArrayBuffer[GenericMutableRow]()
        while (row._1.hasNext) {
          buffer += new GenericMutableRow(row._1.next().map(toType))
        }
        buffer
      }
    }
  }

  def output: Seq[Attribute] = {
    attributesRaw
  }

}

class CarbonRawMutableRow(values: Array[Array[Object]],
    val schema: QuerySchemaInfo) extends GenericMutableRow(values.asInstanceOf[Array[Any]]) {

  val dimsLen = schema.getQueryDimensions.length - 1
  val order = schema.getQueryOrder
  var counter = 0
  val size = {
    if (values.nonEmpty) {
      values.head.length
    } else {
      0
    }
  }

  def getKey: ByteArrayWrapper = values.head(counter).asInstanceOf[ByteArrayWrapper]

  def parseKey(key: ByteArrayWrapper, aggData: Array[Object], order: Array[Int]): Array[Object] = {
    BatchRawResult.parseData(key, aggData, schema, order)
  }

  def hasNext: Boolean = {
    counter < size
  }

  def next(): Unit = {
    counter += 1
  }

  override def numFields: Int = dimsLen + schema.getQueryMeasures.length

  override def anyNull: Boolean = true

  override def get(ordinal: Int, dataType: DataType): AnyRef = {
    values(order(ordinal) - dimsLen)(counter)
      .asInstanceOf[AnyRef]
  }

  override def getUTF8String(ordinal: Int): UTF8String = {
    UTF8String
      .fromString(values(
        order(ordinal) - dimsLen)(counter)
        .asInstanceOf[String])
  }

  override def getDouble(ordinal: Int): Double = {
    values(order(ordinal) - dimsLen)(counter)
      .asInstanceOf[Double]
  }

  override def getFloat(ordinal: Int): Float = {
    values(order(ordinal) - dimsLen)(counter)
      .asInstanceOf[Float]
  }

  override def getLong(ordinal: Int): Long = {
    values(order(ordinal) - dimsLen)(counter)
      .asInstanceOf[Long]
  }

  override def getByte(ordinal: Int): Byte = {
    values(order(ordinal) - dimsLen)(counter)
      .asInstanceOf[Byte]
  }

  override def getDecimal(ordinal: Int,
      precision: Int,
      scale: Int): Decimal = {
    values(order(ordinal) - dimsLen)(counter).asInstanceOf[Decimal]
  }

  override def getBoolean(ordinal: Int): Boolean = {
    values(order(ordinal) - dimsLen)(counter)
      .asInstanceOf[Boolean]
  }

  override def getShort(ordinal: Int): Short = {
    values(order(ordinal) - dimsLen)(counter)
      .asInstanceOf[Short]
  }

  override def getInt(ordinal: Int): Int = {
    values(order(ordinal) - dimsLen)(counter)
      .asInstanceOf[Int]
  }

  override def isNullAt(ordinal: Int): Boolean = values(order(ordinal) - dimsLen)(counter) == null
}
