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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.LeafNode
import org.apache.spark.sql.hive.CarbonMetastoreCatalog
import org.apache.spark.unsafe.types.UTF8String

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.carbondata.hadoop.CarbonInputFormat
import org.carbondata.query.aggregator.impl.CountAggregator
import org.carbondata.query.carbon.model.{QueryDimension, QueryMeasure, SortOrderType}
import org.carbondata.query.carbon.result.RowResult
import org.carbondata.query.expression.{ColumnExpression => CarbonColumnExpression}
import org.carbondata.query.expression.{Expression => CarbonExpression}
import org.carbondata.query.expression.{LiteralExpression => CarbonLiteralExpression}
import org.carbondata.query.expression.arithmetic.{AddExpression, DivideExpression, MultiplyExpression, SubstractExpression}
import org.carbondata.query.expression.conditional._
import org.carbondata.query.expression.logical.{AndExpression, OrExpression}
import org.carbondata.query.scanner.impl.{CarbonKey, CarbonValue}
import org.carbondata.spark.agg._
import org.carbondata.spark.KeyVal
import org.carbondata.spark.KeyValImpl
import org.carbondata.spark.query.CarbonQueryPlan
import org.carbondata.spark.rdd.CarbonQueryRDD
import org.carbondata.spark.util.{CarbonQueryUtil, CarbonScalaUtil, QueryPlanUtil}

case class CarbonCubeScan(
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
  // val carbonTable = CarbonMetadata.getInstance().getCarbonTable(cubeName)
  @transient val carbonCatalog = sqlContext.catalog.asInstanceOf[CarbonMetastoreCatalog]

  def processAggregateExpr(plan: CarbonQueryPlan, currentAggregate: AggregateExpression1,
      queryOrder: Int): Int = {

    currentAggregate match {
      case SumCarbon(posLiteral@PositionLiteral(attr: AttributeReference, _), _) =>
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
        posLiteral.setPosition(queryOrder)
        queryOrder + 1

      case CountCarbon(posLiteral@PositionLiteral(attr: AttributeReference, _)) =>
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
        posLiteral.setPosition(queryOrder)
        queryOrder + 1

      case CountCarbon(posLiteral@PositionLiteral(Literal(star, _), _)) if star == "*" =>
        val m1 = new QueryMeasure("count(*)")
        m1.setAggregateFunction(CarbonCommonConstants.COUNT)
        m1.setQueryOrder(queryOrder)
        posLiteral.setPosition(queryOrder)
        plan.addMeasure(m1)
        plan.setCountStartQuery(true)
        posLiteral.setPosition(queryOrder)
        queryOrder + 1

      case curr@CountCarbon(posLiteral@PositionLiteral(one, _)) =>
        val m1 = new QueryMeasure("count(*)")
        m1.setAggregateFunction(CarbonCommonConstants.COUNT)
        m1.setQueryOrder(queryOrder)
        posLiteral.setPosition(queryOrder)
        plan.addMeasure(m1)
        plan.setCountStartQuery(true)
        posLiteral.setPosition(queryOrder)
        queryOrder + 1
      case CountDistinctCarbon(posLiteral@PositionLiteral(attr: AttributeReference, _)) =>
        val msrs = selectedMsrs.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
        if (msrs.nonEmpty) {
          val m1 = new QueryMeasure(attr.name)
          m1.setAggregateFunction(CarbonCommonConstants.DISTINCT_COUNT)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
          if (dims.nonEmpty) {
            val d1 = new QueryDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getColumnName, "distinct-count", d1.getQueryOrder)
          }
        }
        posLiteral.setPosition(queryOrder)
        queryOrder + 1

      case AverageCarbon(posLiteral@PositionLiteral(attr: AttributeReference, _), _) =>
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
        posLiteral.setPosition(queryOrder)
        queryOrder + 1

      case MinCarbon(posLiteral@PositionLiteral(attr: AttributeReference, _), _) =>
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
        posLiteral.setPosition(queryOrder)
        queryOrder + 1

      case MaxCarbon(posLiteral@PositionLiteral(attr: AttributeReference, _), _) =>
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
        posLiteral.setPosition(queryOrder)
        queryOrder + 1

      case SumDistinctCarbon(posLiteral@PositionLiteral(attr: AttributeReference, _), _) =>
        val msrs = selectedMsrs.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
        if (msrs.nonEmpty) {
          val m1 = new QueryMeasure(attr.name)
          m1.setAggregateFunction(CarbonCommonConstants.SUM_DISTINCT)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
          if (dims != null) {
            //            plan.removeDimensionFromDimList(dims(0));
            val d1 = new QueryDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getColumnName, "sum-distinct", queryOrder)
          }
        }
        posLiteral.setPosition(queryOrder)
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
          // TODO if we can add ordina in carbonDimension, it will be good
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
    // Separately handle group by columns, known or unknown partial aggregations and other
    // expressions. All single column & known aggregate expressions will use native aggregates for
    // measure and dimensions
    // Unknown aggregates & Expressions will use custom aggregator
    aggExprs match {
      case Some(a: Seq[Expression]) if !forceDetailedQuery =>
        a.foreach {
          case attr@AttributeReference(_, _, _, _) => // Add all the references to carbon query
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
            outputColumns += attr
          case par: Alias if par.children.head.isInstanceOf[AggregateExpression1] =>
            outputColumns += par.toAttribute
            queryOrder = processAggregateExpr(plan,
              par.children.head.asInstanceOf[AggregateExpression1], queryOrder)

          case _ => forceDetailedQuery = true
        }
      case _ => forceDetailedQuery = true
    }

    if (forceDetailedQuery) {
      // First clear the model if Msrs, Expressions and AggDimAggInfo filled
      plan.getDimensions.clear()
      plan.getMeasures.clear()
      plan.getDimAggregatorInfos.clear()
      plan.getExpressions.clear()

      // Fill the selected dimensions & measures obtained from
      // attributes to query plan  for detailed query
      selectedDims.foreach(plan.addDimension)
      selectedMsrs.foreach(plan.addMeasure)
    }
    else {
      attributes = outputColumns
    }

    val orderList = new ArrayList[QueryDimension]()

    var allSortExprPushed = true
    sortExprs match {
      case Some(a: Seq[SortOrder]) =>
        a.foreach {
          case SortOrder(SumCarbon(attr: AttributeReference, _), order) => plan.getMeasures
            .asScala.filter(m => m.getColumnName.equalsIgnoreCase(attr.name)).head
            .setSortOrder(getSortDirection(order))
          case SortOrder(CountCarbon(attr: AttributeReference), order) => plan.getMeasures
            .asScala.filter(m => m.getColumnName.equalsIgnoreCase(attr.name)).head
            .setSortOrder(getSortDirection(order))
          case SortOrder(CountDistinctCarbon(attr: AttributeReference), order) => plan.getMeasures
            .asScala.filter(m => m.getColumnName.equalsIgnoreCase(attr.name)).head
            .setSortOrder(getSortDirection(order))
          case SortOrder(AverageCarbon(attr: AttributeReference, _), order) => plan.getMeasures
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
        val maxValueLimit = value.substring(0, value.size - 1) + value.charAt(value.size - 1) + 1
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
    if (extraPreds.nonEmpty) {
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
    val absoluteTableIdentifier = new AbsoluteTableIdentifier(carbonCatalog.storePath,
      new CarbonTableIdentifier(carbonTable.getDatabaseName, carbonTable.getFactTableName))

    val model = CarbonQueryUtil.createQueryModel(
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

  def doExecute(): RDD[InternalRow] = {
    def toType(obj: Any): Any = {
      obj match {
        case s: String => UTF8String.fromString(s)
        case _ => obj
      }
    }
    // count(*) query executed in driver by querying from Btree
    if (buildCarbonPlan.isCountStarQuery && null == buildCarbonPlan.getFilterExpression) {
      val absoluteTableIdentifier = new AbsoluteTableIdentifier(carbonCatalog.storePath,
        new CarbonTableIdentifier(carbonTable.getDatabaseName, carbonTable.getFactTableName)
      )
      val (carbonInputFormat: CarbonInputFormat[RowResult], job: Job) =
        QueryPlanUtil.createCarbonInputFormat(absoluteTableIdentifier)
      // get row count
      val rowCount = carbonInputFormat.getRowCount(job)
      val countAgg = new CountAggregator()
      countAgg.setNewValue(rowCount)
      sparkContext.parallelize(
        Seq(new GenericMutableRow(Seq(countAgg).toArray.asInstanceOf[Array[Any]]))
      )
    } else {
      // all the other queries are sent to executor
      inputRdd.map { row =>
        val dims = row._1.getKey.map(toType)
        val values = dims
        new GenericMutableRow(values.asInstanceOf[Array[Any]])
      }
    }
  }

  def output: Seq[Attribute] = {
    attributes
  }

}

