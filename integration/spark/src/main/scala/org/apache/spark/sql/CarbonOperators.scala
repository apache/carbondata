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
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.agg.{CarbonAverage, CarbonCount}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, _}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Max, Min, Sum}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.execution.LeafNode
import org.apache.spark.sql.hive.CarbonMetastoreCatalog
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.UTF8String

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.carbon.AbsoluteTableIdentifier
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.carbondata.hadoop.CarbonInputFormat
import org.carbondata.query.aggregator.MeasureAggregator
import org.carbondata.query.aggregator.impl.avg.AbstractAvgAggregator
import org.carbondata.query.aggregator.impl.count.CountAggregator
import org.carbondata.query.aggregator.impl.max.{MaxAggregator, MaxBigDecimalAggregator, MaxLongAggregator}
import org.carbondata.query.aggregator.impl.min.{MinAggregator, MinBigDecimalAggregator, MinLongAggregator}
import org.carbondata.query.aggregator.impl.sum.{SumBigDecimalAggregator, SumDoubleAggregator, SumLongAggregator}
import org.carbondata.query.carbon.model.{CarbonQueryPlan, QueryDimension, QueryMeasure, QueryModel, SortOrderType}
import org.carbondata.query.carbon.result.RowResult
import org.carbondata.query.expression.{ColumnExpression => CarbonColumnExpression, Expression => CarbonExpression, LiteralExpression => CarbonLiteralExpression}
import org.carbondata.query.expression.arithmetic.{AddExpression, DivideExpression, MultiplyExpression, SubstractExpression}
import org.carbondata.query.expression.conditional._
import org.carbondata.query.expression.logical.{AndExpression, OrExpression}
import org.carbondata.query.scanner.impl.{CarbonKey, CarbonValue}
import org.carbondata.spark.{KeyVal, KeyValImpl}
import org.carbondata.spark.rdd.CarbonQueryRDD
import org.carbondata.spark.util.{CarbonScalaUtil, QueryPlanUtil}

case class CarbonTableScan(
    var attributes: Seq[Attribute],
    relation: CarbonRelation,
    dimensionPredicates: Seq[Expression],
    aggExprs: Option[Seq[Expression]],
    sortExprs: Option[Seq[SortOrder]],
    limitExpr: Option[Expression],
    isGroupByPresent: Boolean,
    detailQuery: Boolean = false)(@transient val oc: SQLContext)
  extends LeafNode {

  val cubeName = relation.cubeName
  val carbonTable = relation.metaData.carbonTable
  val selectedDims = scala.collection.mutable.MutableList[QueryDimension]()
  val selectedMsrs = scala.collection.mutable.MutableList[QueryMeasure]()
  var outputColumns = scala.collection.mutable.MutableList[Attribute]()
  var extraPreds: Seq[Expression] = Nil
  val allDims = new scala.collection.mutable.HashSet[String]()
  @transient val carbonCatalog = sqlContext.catalog.asInstanceOf[CarbonMetastoreCatalog]

  def processAggregateExpr(plan: CarbonQueryPlan,
      currentAggregate: AggregateExpression,
      queryOrder: Int,
      aggCount: Int): Int = {
    currentAggregate match {
      case AggregateExpression(Sum(p@PositionLiteral(attr: AttributeReference, _)), _, false) =>
        outputColumns += attr
        val msrs = selectedMsrs.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
        if (msrs.nonEmpty) {
          val m1 = new QueryMeasure(attr.name)
          m1.setAggregateFunction(CarbonCommonConstants.SUM)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
          if (dims.nonEmpty) {
            val d1 = new QueryDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getColumnName, "sum", d1.getQueryOrder)
          }
        }
        p.setPosition(queryOrder + aggCount)
        queryOrder + 1

      case AggregateExpression(
        Sum(Cast(p@PositionLiteral(attr: AttributeReference, _), _)), _, false) =>
        outputColumns += attr
        val msrs = selectedMsrs.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
        if (msrs.nonEmpty) {
          val m1 = new QueryMeasure(attr.name)
          m1.setAggregateFunction(CarbonCommonConstants.SUM)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
          if (dims.nonEmpty) {
            val d1 = new QueryDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getColumnName, "sum", d1.getQueryOrder)
          }
        }
        p.setPosition(queryOrder + aggCount)
        queryOrder + 1

      case AggregateExpression(
        CarbonCount(p@PositionLiteral(attr: AttributeReference, _), None), _, false) =>
        outputColumns += attr
        val msrs = selectedMsrs.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
        if (msrs.nonEmpty) {
          val m1 = new QueryMeasure(attr.name)
          m1.setAggregateFunction(CarbonCommonConstants.COUNT)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
          if (dims.nonEmpty) {
            val d1 = new QueryDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getColumnName, "count", d1.getQueryOrder)
          }
        }
        p.setPosition(queryOrder + aggCount)
        queryOrder + 1

      case AggregateExpression(
        CarbonCount(lt: Literal, Some(p@PositionLiteral(attr: AttributeReference, _))), _, false)
        if lt.value == "*" || lt.value == 1 =>
        outputColumns += attr
        val m1 = new QueryMeasure("count(*)")
        m1.setAggregateFunction(CarbonCommonConstants.COUNT)
        m1.setQueryOrder(queryOrder)
        plan.addMeasure(m1)
        plan.setCountStartQuery(true)
        p.setPosition(queryOrder + aggCount)
        queryOrder + 1

      case AggregateExpression(
        CarbonAverage(p@PositionLiteral(attr: AttributeReference, _)), _, false) =>
        outputColumns += attr
        val msrs = selectedMsrs.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
        if (msrs.nonEmpty) {
          val m1 = new QueryMeasure(attr.name)
          m1.setAggregateFunction(CarbonCommonConstants.AVERAGE)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
          if (dims.nonEmpty) {
            val d1 = new QueryDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getColumnName, "avg", d1.getQueryOrder)
          }
        }
        p.setPosition(queryOrder + aggCount)
        queryOrder + 1

      case AggregateExpression(
        CarbonAverage(Cast(p@PositionLiteral(attr: AttributeReference, _), _)), _, false) =>
        outputColumns += attr
        val msrs = selectedMsrs.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
        if (msrs.nonEmpty) {
          val m1 = new QueryMeasure(attr.name)
          m1.setAggregateFunction(CarbonCommonConstants.AVERAGE)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
          if (dims.nonEmpty) {
            val d1 = new QueryDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getColumnName, "avg", d1.getQueryOrder)
          }
        }
        p.setPosition(queryOrder + aggCount)
        queryOrder + 1

      case AggregateExpression(Min(p@PositionLiteral(attr: AttributeReference, _)), _, false) =>
        outputColumns += attr
        val msrs = selectedMsrs.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
        if (msrs.nonEmpty) {
          val m1 = new QueryMeasure(attr.name)
          m1.setAggregateFunction(CarbonCommonConstants.MIN)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
          if (dims != null) {
            val d1 = new QueryDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getColumnName, "min", d1.getQueryOrder)
          }
        }
        p.setPosition(queryOrder + aggCount)
        queryOrder + 1

      case AggregateExpression(
        Min(Cast(p@PositionLiteral(attr: AttributeReference, _), _)), _, false) =>
        outputColumns += attr
        val msrs = selectedMsrs.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
        if (msrs.nonEmpty) {
          val m1 = new QueryMeasure(attr.name)
          m1.setAggregateFunction(CarbonCommonConstants.MIN)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
          if (dims != null) {
            val d1 = new QueryDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getColumnName, "min", d1.getQueryOrder)
          }
        }
        p.setPosition(queryOrder + aggCount)
        queryOrder + 1

      case AggregateExpression(Max(p@PositionLiteral(attr: AttributeReference, _)), _, false) =>
        outputColumns += attr
        val msrs = selectedMsrs.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
        if (msrs.nonEmpty) {
          val m1 = new QueryMeasure(attr.name)
          m1.setAggregateFunction(CarbonCommonConstants.MAX)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
          if (dims.nonEmpty) {
            val d1 = new QueryDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getColumnName, "max", d1.getQueryOrder)
          }
        }
        p.setPosition(queryOrder + aggCount)
        queryOrder + 1

      case AggregateExpression(
        Max(Cast(p@PositionLiteral(attr: AttributeReference, _), _)), _, false) =>
        outputColumns += attr
        val msrs = selectedMsrs.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
        if (msrs.nonEmpty) {
          val m1 = new QueryMeasure(attr.name)
          m1.setAggregateFunction(CarbonCommonConstants.MAX)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
          if (dims.nonEmpty) {
            val d1 = new QueryDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getColumnName, "max", d1.getQueryOrder)
          }
        }
        p.setPosition(queryOrder + aggCount)
        queryOrder + 1

      case _ => throw new
          Exception("Some Aggregate functions cannot be pushed, force to detailequery")
    }
  }

  val buildCarbonPlan: CarbonQueryPlan = {
    val plan: CarbonQueryPlan = new CarbonQueryPlan(relation.schemaName, relation.cubeName)


    var forceDetailedQuery = detailQuery
    var queryOrder: Integer = 0
    attributes.map(
      attr => {
        val carbonDimension = carbonTable.getDimensionByName(carbonTable.getFactTableName
          , attr.name)
        if (carbonDimension != null) {
          allDims += attr.name
          val dim = new QueryDimension(attr.name)
          dim.setQueryOrder(queryOrder)
          queryOrder = queryOrder + 1
          selectedDims += dim
        } else {
          val carbonMeasure = carbonTable.getMeasureByName(carbonTable.getFactTableName
            , attr.name)
          if (carbonMeasure != null) {
            val m1 = new QueryMeasure(attr.name)
            m1.setQueryOrder(queryOrder)
            queryOrder = queryOrder + 1
            selectedMsrs += m1
          }
        }
      })
    queryOrder = 0

    // It is required to calculate as spark aggregators uses joined row with the current aggregates.
    val aggCount = aggExprs match {
      case Some(a: Seq[Expression]) =>
        a.map {
          case Alias(AggregateExpression(CarbonAverage(_), _, _), name) => 2
          case Alias(agg: AggregateExpression, name) => 1
          case _ => 0
        }.reduceLeftOption((left, right) => left + right).getOrElse(0)
      case _ => 0
    }
    // Separately handle group by columns, known or unknown partial aggregations and other
    // expressions. All single column & known aggregate expressions will use native aggregates for
    // measure and dimensions
    // Unknown aggregates & Expressions will use custom aggregator

    aggExprs match {
      case Some(a: Seq[Expression]) if !forceDetailedQuery =>
        a.foreach {
          case attr@AttributeReference(_, _, _, _) => // Add all the references to carbon query
            addCarbonColumn(attr)
            outputColumns += attr
          case al@ Alias(agg: AggregateExpression, name) =>
            queryOrder = processAggregateExpr(plan, agg, queryOrder, aggCount)
          case _ => forceDetailedQuery = true
        }
      case _ => forceDetailedQuery = true
    }

    def addCarbonColumn(attr: Attribute): Unit = {
      val carbonDimension = selectedDims
        .filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
      if (carbonDimension.nonEmpty) {
        val dim = new QueryDimension(attr.name)
        dim.setQueryOrder(queryOrder)
        plan.addDimension(dim)
        queryOrder = queryOrder + 1
      } else {
        val carbonMeasure = selectedMsrs
          .filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
        if (carbonMeasure.nonEmpty) {
          // added by vishal as we are adding for dimension so need to add to measure list
          // Carbon does not support group by on measure column so throwing exception to
          // make it detail query
          throw new
              Exception("Some Aggregate functions cannot be pushed, force to detailequery")
        }
        else {
          // Some unknown attribute name is found. this may be a derived column.
          // So, let's fall back to detailed query flow
          throw new Exception(
            "Some attributes referred looks derived columns. So, force to detailequery " +
            attr.name)
        }
      }
    }

    if (forceDetailedQuery) {
      // First clear the model if Msrs, Expressions and AggDimAggInfo filled
      plan.getDimensions.clear()
      plan.getMeasures.clear()
      plan.getDimAggregatorInfos.clear()

      // Fill the selected dimensions & measures obtained from
      // attributes to query plan  for detailed query
      selectedDims.foreach(plan.addDimension)
      selectedMsrs.foreach(plan.addMeasure)
    }
    else {
      attributes.foreach { attr =>
        if (!outputColumns.exists(_.name.equals(attr.name))) {
          addCarbonColumn(attr)
          outputColumns += attr
        }
      }
      attributes = outputColumns
    }

    val orderList = new ArrayList[QueryDimension]()

    var allSortExprPushed = true
    sortExprs match {
      case Some(a: Seq[SortOrder]) =>
        a.foreach {
          case SortOrder(Sum(attr: AttributeReference), order) => plan.getMeasures
            .asScala.filter(m => m.getColumnName.equalsIgnoreCase(attr.name)).head
            .setSortOrder(getSortDirection(order))
          case SortOrder(CarbonCount(attr: AttributeReference, _), order) => plan.getMeasures
            .asScala.filter(m => m.getColumnName.equalsIgnoreCase(attr.name)).head
            .setSortOrder(getSortDirection(order))
          case SortOrder(CarbonAverage(attr: AttributeReference), order) => plan.getMeasures
            .asScala.filter(m => m.getColumnName.equalsIgnoreCase(attr.name)).head
            .setSortOrder(getSortDirection(order))
          case SortOrder(attr: AttributeReference, order) =>
            val dim = plan.getDimensions
              .asScala.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
            if (dim.nonEmpty) {
              dim.head.setSortOrder(getSortDirection(order))
              orderList.add(dim.head)
            } else {
              allSortExprPushed = false
            }
          case _ => allSortExprPushed = false;
        }
      case _ =>
    }

    plan.setSortedDimemsions(orderList)

    // limit can be pushed down only if sort is not present or all sort expressions are pushed
    if (sortExprs.isEmpty && forceDetailedQuery) {
      limitExpr match {
        case Some(IntegerLiteral(limit)) =>
          // if (plan.getMeasures.size() == 0 && plan.getDimAggregatorInfos.size() == 0) {
          plan.setLimit(limit)
        // }
        case _ =>
      }
    }
    plan.setDetailQuery(forceDetailedQuery)
    plan.setOutLocationPath(
      CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS))
    plan.setQueryId(System.nanoTime() + "")
    if (dimensionPredicates.nonEmpty) {
      val exps = preProcessExpressions(dimensionPredicates)
      val expressionVal = transformExpression(exps.head)
      // adding dimension used in expression in querystats
      expressionVal.getChildren.asScala.filter { x => x.isInstanceOf[CarbonColumnExpression] }
        .map { y => allDims += y.asInstanceOf[CarbonColumnExpression].getColumnName }
      plan.setFilterExpression(expressionVal)
    }
    plan
  }

  def preProcessExpressions(expressions: Seq[Expression]): Seq[Expression] = {
    expressions match {
      case left :: right :: rest => preProcessExpressions(List(And(left, right)) ::: rest)
      case List(left, right) => List(And(left, right))

      case _ => expressions
    }
  }

  def transformExpression(expr: Expression): CarbonExpression = {
    expr match {
      case Or(left, right) => new
          OrExpression(transformExpression(left), transformExpression(right))
      case And(left, right) => new
          AndExpression(transformExpression(left), transformExpression(right))
      case EqualTo(left, right) => new
          EqualToExpression(transformExpression(left), transformExpression(right))
      case Not(EqualTo(left, right)) => new
          NotEqualsExpression(transformExpression(left), transformExpression(right))
      case IsNotNull(child) => new
          NotEqualsExpression(transformExpression(child), transformExpression(Literal(null)))
      case Not(In(left, right)) => new NotInExpression(transformExpression(left),
        new ListExpression(right.map(transformExpression).asJava))
      case In(left, right) => new InExpression(transformExpression(left),
        new ListExpression(right.map(transformExpression).asJava))
      case Add(left, right) => new
          AddExpression(transformExpression(left), transformExpression(right))
      case Subtract(left, right) => new
          SubstractExpression(transformExpression(left), transformExpression(right))
      case Multiply(left, right) => new
          MultiplyExpression(transformExpression(left), transformExpression(right))
      case Divide(left, right) => new
          DivideExpression(transformExpression(left), transformExpression(right))
      case GreaterThan(left, right) => new
          GreaterThanExpression(transformExpression(left), transformExpression(right))
      case LessThan(left, right) => new
          LessThanExpression(transformExpression(left), transformExpression(right))
      case GreaterThanOrEqual(left, right) => new
          GreaterThanEqualToExpression(transformExpression(left), transformExpression(right))
      case LessThanOrEqual(left, right) => new
          LessThanEqualToExpression(transformExpression(left), transformExpression(right))
      // convert StartWith('abc') or like(col 'abc%') to col >= 'abc' and col < 'abd'
      case StartsWith(left, right @ Literal(pattern, dataType)) if (pattern.toString.size > 0) =>
        val l = new GreaterThanEqualToExpression(
          transformExpression(left), transformExpression(right))
        val value = pattern.toString
        val maxValueLimit = value.substring(0, value.length - 1) +
          (value.charAt(value.length - 1).toInt + 1).toChar
        val r = new LessThanExpression(
          transformExpression(left),
            new CarbonLiteralExpression(maxValueLimit,
              CarbonScalaUtil.convertSparkToCarbonDataType(dataType)))
        new AndExpression(l, r)
      case AttributeReference(name, dataType, _, _) => new CarbonColumnExpression(name.toString,
        CarbonScalaUtil.convertSparkToCarbonDataType(dataType))
      case Literal(name, dataType) => new
          CarbonLiteralExpression(name, CarbonScalaUtil.convertSparkToCarbonDataType(dataType))
      case Cast(left, right) if !left.isInstanceOf[Literal] => transformExpression(left)
      case aggExpr: AggregateExpression =>
          throw new UnsupportedOperationException(s"Cannot evaluate expression: $aggExpr")
      case _ =>
        new SparkUnknownExpression(expr.transform {
          case AttributeReference(name, dataType, _, _) =>
            CarbonBoundReference(new CarbonColumnExpression(name.toString,
              CarbonScalaUtil.convertSparkToCarbonDataType(dataType)), dataType, expr.nullable)
        })
    }
  }

  private def getSortDirection(sort: SortDirection) = {
    sort match {
      case Ascending => SortOrderType.ASC
      case Descending => SortOrderType.DSC
    }
  }


  def addPushdownFilters(keys: Seq[Expression], filters: Array[Array[Expression]],
      conditions: Option[Expression]) {

    // TODO Values in the IN filter is duplicate. replace the list with set
    val buffer = new ArrayBuffer[Expression]
    keys.zipWithIndex.foreach { a =>
      buffer += In(a._1, filters(a._2)).asInstanceOf[Expression]
    }

    // Let's not pushdown condition. Only filter push down is sufficient.
    // Conditions can be applied on hash join result.
    val cond = if (buffer.size > 1) {
      val e = buffer.remove(0)
      buffer.fold(e)(And(_, _))
    } else {
      buffer.asJava.get(0)
    }

    extraPreds = Seq(cond)
  }

  def inputRdd: CarbonQueryRDD[CarbonKey, CarbonValue] = {
    val LOG = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    // Update the FilterExpressions with extra conditions added through join pushdown
    if (extraPreds.nonEmpty) {attributes
      val exps = preProcessExpressions(extraPreds)
      val expressionVal = transformExpression(exps.head)
      val oldExpressionVal = buildCarbonPlan.getFilterExpression
      if (null == oldExpressionVal) {
        buildCarbonPlan.setFilterExpression(expressionVal)
      } else {
        buildCarbonPlan.setFilterExpression(new AndExpression(oldExpressionVal, expressionVal))
      }
    }

    val conf = new Configuration()
    val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier

    val model = QueryModel.createModel(
      absoluteTableIdentifier, buildCarbonPlan, carbonTable)
    val kv: KeyVal[CarbonKey, CarbonValue] = new KeyValImpl()
    // setting queryid
    buildCarbonPlan.setQueryId(oc.getConf("queryId", System.nanoTime() + ""))

    LOG.info("Selected Table to Query ****** "
             + model.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableName)

    val cubeCreationTime = carbonCatalog.getCubeCreationTime(relation.schemaName, cubeName)
    val schemaLastUpdatedTime =
      carbonCatalog.getSchemaLastUpdatedTime(relation.schemaName, cubeName)
    val big = new CarbonQueryRDD(
      oc.sparkContext,
      model,
      buildCarbonPlan.getFilterExpression,
      kv,
      conf,
      cubeCreationTime,
      schemaLastUpdatedTime,
      carbonCatalog.storePath)
    big
  }


  override def outputsUnsafeRows: Boolean = false

  def doExecute(): RDD[InternalRow] = {
    def toType(obj: Any): Any = {
      obj match {
        case s: String => UTF8String.fromString(s)
        case avg: AbstractAvgAggregator =>
          if (avg.isFirstTime) {
            null
          } else {
            new GenericArrayData(avg.getAvgState.asInstanceOf[Array[Any]])
          }
        case c: CountAggregator => c.getLongValue
        case s: SumDoubleAggregator => s.getDoubleValue
        case s: SumBigDecimalAggregator => Decimal(s.getBigDecimalValue)
        case s: SumLongAggregator => s.getLongValue
        case m: MaxBigDecimalAggregator => Decimal(m.getBigDecimalValue)
        case m: MaxLongAggregator => m.getLongValue
        case m: MaxAggregator => toType(m.getValueObject)
        case m: MinBigDecimalAggregator => Decimal(m.getBigDecimalValue)
        case m: MinLongAggregator => m.getLongValue
        case m: MinAggregator => toType(m.getValueObject)
        case m: MeasureAggregator => toType(m.getValueObject)
        case _ => obj
      }
    }

//    val unsafeProjection = UnsafeProjection.create(attributes.map(_.dataType).toArray)
    // count(*) query executed in driver by querying from Btree
    if (isCountQuery) {
      val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
      val (carbonInputFormat: CarbonInputFormat[RowResult], job: Job) =
        QueryPlanUtil.createCarbonInputFormat(absoluteTableIdentifier)
      // get row count
      val rowCount = carbonInputFormat.getRowCount(job)
      val countAgg = new CountAggregator()
      countAgg.setNewValue(rowCount)
      sparkContext.parallelize(
        Seq(new GenericMutableRow(Seq(countAgg.getLongValue).toArray.asInstanceOf[Array[Any]]))
      )
    } else {
      // all the other queries are sent to executor
      inputRdd.mapPartitions { iter =>
        new Iterator[InternalRow] {
//          val unsafeProjection = UnsafeProjection.create(attributes.map(_.dataType).toArray)
          override def hasNext: Boolean = iter.hasNext

          override def next(): InternalRow = {
            new GenericMutableRow(iter.next()._1.getKey.map(toType))
          }
        }
      }
    }
  }

  /**
   * return true if query is count queryUtils
 *
   * @return
   */
  def isCountQuery: Boolean = {
    if (buildCarbonPlan.isCountStarQuery() && null == buildCarbonPlan.getFilterExpression &&
        buildCarbonPlan.getDimensions.size() < 1 && buildCarbonPlan.getMeasures.size() < 2 &&
        buildCarbonPlan.getDimAggregatorInfos.size() < 1) {
      true
    } else {
      false
    }
  }

  def output: Seq[Attribute] = {
    attributes
  }
}

