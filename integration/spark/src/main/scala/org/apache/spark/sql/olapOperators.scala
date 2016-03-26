/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql

import java.util.ArrayList

import org.carbondata.core.constants.MolapCommonConstants
import org.carbondata.core.util.MolapProperties
import org.carbondata.integration.spark.{KeyValImpl, KeyVal}
import org.carbondata.integration.spark.agg._
import org.carbondata.integration.spark.query.MolapQueryPlan
import org.carbondata.integration.spark.query.metadata.{SortOrderType, MolapMeasure, MolapDimension}
import org.carbondata.integration.spark.rdd.MolapDataRDD
import org.carbondata.integration.spark.util.{MolapScalaUtil, MolapQueryUtil}
import org.carbondata.query.executer.MolapQueryExecutorModel
import org.carbondata.query.expression.arithmetic.{AddExpression, DivideExpression, MultiplyExpression, SubstractExpression}
import org.carbondata.query.expression.conditional.{EqualToExpression, NotEqualsExpression, _}
import org.carbondata.query.expression.logical.{AndExpression, OrExpression}
import org.carbondata.query.expression.ColumnExpression
import org.carbondata.query.expression.{ColumnExpression => MolapColumnExpression, Expression => MolapExpression, LiteralExpression => MolapLiteralExpression}
import org.carbondata.query.querystats.{QueryDetail, QueryStatsCollector}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.LeafNode
import org.apache.spark.unsafe.types.UTF8String
import org.carbondata.query.scanner.impl.{MolapKey, MolapValue}

import scala.collection.JavaConversions.{asScalaBuffer, bufferAsJavaList}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


case class OlapCubeScan(
    var attributes: Seq[Attribute],
    relation: OlapRelation,
    dimensionPredicates: Seq[Expression],
    aggExprs: Option[Seq[Expression]],
    sortExprs: Option[Seq[SortOrder]],
    limitExpr: Option[Expression],
    isGroupByPresent: Boolean,
    detailQuery: Boolean = false)(@transient val oc: SQLContext)
  extends LeafNode {

  val cubeName = relation.cubeName
  val cube = relation.metaData.cube
  val selectedDims = scala.collection.mutable.MutableList[MolapDimension]()
  val selectedMsrs = scala.collection.mutable.MutableList[MolapMeasure]()
  var outputColumns = scala.collection.mutable.MutableList[Attribute]()
  var extraPreds: Seq[Expression] = Nil
  val allDims = new scala.collection.mutable.HashSet[String]()

  def processAggregateExpr(plan: MolapQueryPlan, currentAggregate: AggregateExpression1, queryOrder: Int): Int = {

    currentAggregate match {
      case SumMolap(posLiteral@PositionLiteral(attr: AttributeReference, _), _) =>
        val msrs = selectedMsrs.filter(m => m.getMeasure().equalsIgnoreCase(attr.name))
        if (msrs.length > 0) {
          val m1 = new MolapMeasure(attr.name)
          m1.setAggregatorType(MolapMeasure.AggregatorType.SUM)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getDimensionUniqueName.equalsIgnoreCase(attr.name))
          if (dims.length > 0) {
            val d1 = new MolapDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getDimensionUniqueName, "sum", d1.getQueryOrder)
          }
        }
        posLiteral.setPosition(queryOrder)
        queryOrder + 1

      case CountMolap(posLiteral@PositionLiteral(attr: AttributeReference, _)) =>
        val msrs = selectedMsrs.filter(m => m.getMeasure().equalsIgnoreCase(attr.name))
        if (msrs.length > 0) {
          val m1 = new MolapMeasure(attr.name)
          m1.setAggregatorType(MolapMeasure.AggregatorType.COUNT)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getDimensionUniqueName.equalsIgnoreCase(attr.name))
          if (dims.length > 0) {
            val d1 = new MolapDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getDimensionUniqueName, "count", d1.getQueryOrder)
          }
        }
        posLiteral.setPosition(queryOrder)
        queryOrder + 1

      case CountMolap(posLiteral@PositionLiteral(Literal(star, _), _)) if star == "*" =>
        val m1 = new MolapMeasure("count(*)")
        m1.setAggregatorType(MolapMeasure.AggregatorType.COUNT)
        m1.setQueryOrder(queryOrder)
        posLiteral.setPosition(queryOrder)
        plan.addMeasure(m1)
        plan.setCountStartQuery(true)
        posLiteral.setPosition(queryOrder)
        queryOrder + 1

      case curr@CountMolap(posLiteral@PositionLiteral(one, _)) =>
        val m1 = new MolapMeasure("count(*)")
        m1.setAggregatorType(MolapMeasure.AggregatorType.COUNT)
        m1.setQueryOrder(queryOrder)
        posLiteral.setPosition(queryOrder)
        plan.addMeasure(m1)
        plan.setCountStartQuery(true)
        posLiteral.setPosition(queryOrder)
        queryOrder + 1
      case CountDistinctMolap(posLiteral@PositionLiteral(attr: AttributeReference, _)) =>
        val msrs = selectedMsrs.filter(m => m.getMeasure().equalsIgnoreCase(attr.name))
        if (msrs.length > 0) {
          val m1 = new MolapMeasure(attr.name)
          m1.setAggregatorType(MolapMeasure.AggregatorType.DISTINCT_COUNT)
          m1.setQueryOrder(queryOrder)
          m1.setQueryDistinctCount(true)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getDimensionUniqueName.equalsIgnoreCase(attr.name))
          if (dims.length > 0) {
            val d1 = new MolapDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getDimensionUniqueName, "distinct-count", d1.getQueryOrder)
          }
        }
        posLiteral.setPosition(queryOrder)
        queryOrder + 1

      case AverageMolap(posLiteral@PositionLiteral(attr: AttributeReference, _), _) =>
        val msrs = selectedMsrs.filter(m => m.getMeasure().equalsIgnoreCase(attr.name))
        if (msrs.length > 0) {
          val m1 = new MolapMeasure(attr.name)
          m1.setAggregatorType(MolapMeasure.AggregatorType.AVG)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getDimensionUniqueName.equalsIgnoreCase(attr.name))
          if (dims.length > 0) {
            val d1 = new MolapDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getDimensionUniqueName, "avg", d1.getQueryOrder)
          }
        }
        posLiteral.setPosition(queryOrder)
        queryOrder + 1

      case MinMolap(posLiteral@PositionLiteral(attr: AttributeReference, _), _) =>
        val msrs = selectedMsrs.filter(m => m.getMeasure().equalsIgnoreCase(attr.name))
        if (msrs.length > 0) {
          val m1 = new MolapMeasure(attr.name)
          m1.setAggregatorType(MolapMeasure.AggregatorType.MIN)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getDimensionUniqueName.equalsIgnoreCase(attr.name))
          if (dims != null) {
            val d1 = new MolapDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getDimensionUniqueName, "min", d1.getQueryOrder)
          }
        }
        posLiteral.setPosition(queryOrder)
        queryOrder + 1

      case MaxMolap(posLiteral@PositionLiteral(attr: AttributeReference, _), _) =>
        val msrs = selectedMsrs.filter(m => m.getMeasure().equalsIgnoreCase(attr.name))
        if (msrs.length > 0) {
          val m1 = new MolapMeasure(attr.name)
          m1.setAggregatorType(MolapMeasure.AggregatorType.MAX)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getDimensionUniqueName.equalsIgnoreCase(attr.name))
          if (dims.length > 0) {
            val d1 = new MolapDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getDimensionUniqueName, "max", d1.getQueryOrder)
          }
        }
        posLiteral.setPosition(queryOrder)
        queryOrder + 1

      case SumDistinctMolap(posLiteral@PositionLiteral(attr: AttributeReference, _), _) =>
        val msrs = selectedMsrs.filter(m => m.getMeasure().equalsIgnoreCase(attr.name))
        if (msrs.length > 0) {
          val m1 = new MolapMeasure(attr.name)
          m1.setAggregatorType(MolapMeasure.AggregatorType.SUM_DISTINCT)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getDimensionUniqueName.equalsIgnoreCase(attr.name))
          if (dims != null) {
            //            plan.removeDimensionFromDimList(dims(0));
            val d1 = new MolapDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getDimensionUniqueName, "sum-distinct", queryOrder)
          }
        }
        posLiteral.setPosition(queryOrder)
        queryOrder + 1

      case _ => throw new Exception("Some Aggregate functions cannot be pushed, force to detailequery")
    }
  }

  val buildMolapPlan: MolapQueryPlan = {
    val plan: MolapQueryPlan = new MolapQueryPlan(relation.schemaName, relation.cubeName)


    var forceDetailedQuery = detailQuery
    var queryOrder: Integer = 0
    attributes.map(
      attr => {
        val molapDimension = MolapQueryUtil.getMolapDimension(cube.getDimensions(cube.getFactTableName()), attr.name);
        if (molapDimension != null) {
          //TO-DO if we can add ordina in molapDimension, it will be good
          allDims += attr.name
          val dim = new MolapDimension(attr.name)
          dim.setQueryOrder(queryOrder);
          queryOrder = queryOrder + 1
          selectedDims += dim
        } else {
          val molapMeasure = MolapQueryUtil.getMolapMeasure(attr.name, cube.getMeasures(cube.getFactTableName()));
          if (molapMeasure != null) {
            val m1 = new MolapMeasure(attr.name)
            m1.setQueryOrder(queryOrder);
            queryOrder = queryOrder + 1
            selectedMsrs += m1
          }
        }
      })
    queryOrder = 0
    // Separately handle group by columns, known or unknown partial aggregations and other expressions
    // All single column & known aggregate expressions will use native aggregates for measure and dimensions 
    // Unknown aggregates & Expressions will use custom aggregator
    aggExprs match {
      case Some(a: Seq[Expression]) if (!forceDetailedQuery) =>
        a.foreach {
          case attr@AttributeReference(_, _, _, _) => // Add all the references to molap query
            val molapDimension = selectedDims.filter(m => m.getDimensionUniqueName.equalsIgnoreCase(attr.name))
            if (molapDimension.size > 0) {
              val dim = new MolapDimension(attr.name)
              dim.setQueryOrder(queryOrder);
              plan.addDimension(dim);
              queryOrder = queryOrder + 1
            } else {
              val molapMeasure = selectedMsrs.filter(m => m.getMeasure().equalsIgnoreCase(attr.name))
              if (molapMeasure.size > 0) {
                // added by vishal as we are adding for dimension so need to add to measure list  
                // Molap does not support group by on measure column so throwing exception to make it detail query 
                throw new Exception("Some Aggregate functions cannot be pushed, force to detailequery")
              }
              else {
                //Some unknown attribute name is found. this may be a derived column. So, let's fall back to detailed query flow
                throw new Exception("Some attributes referred looks derived columns. So, force to detailequery " + attr.name)
              }
            }
            outputColumns += attr
          case par: Alias if par.children(0).isInstanceOf[AggregateExpression1] => {
            outputColumns += par.toAttribute
            queryOrder = processAggregateExpr(plan, par.children(0).asInstanceOf[AggregateExpression1], queryOrder)
          }

          case _ => forceDetailedQuery = true
        }
      case _ => forceDetailedQuery = true
    }

    if (forceDetailedQuery) {
      //First clear the model if Msrs, Expressions and AggDimAggInfo filled
      plan.getDimensions().clear();
      plan.getMeasures().clear();
      plan.getDimAggregatorInfos().clear();
      plan.getExpressions().clear()

      // Fill the selected dimensions & measures obtained from attributes to query plan  for detailed query 
      selectedDims.foreach(plan.addDimension(_))
      selectedMsrs.foreach(plan.addMeasure(_))
    }
    else {
      attributes = outputColumns.toSeq;
    }

    val orderList = new ArrayList[MolapDimension]()

    var allSortExprPushed = true;
    sortExprs match {
      case Some(a: Seq[SortOrder]) =>
        a.foreach {
          case SortOrder(SumMolap(attr: AttributeReference, _), order) => plan.getMeasures().filter(m => m.getMeasure().equalsIgnoreCase(attr.name))(0).setSortOrderType(getSortDirection(order))
          case SortOrder(CountMolap(attr: AttributeReference), order) => plan.getMeasures().filter(m => m.getMeasure().equalsIgnoreCase(attr.name))(0).setSortOrderType(getSortDirection(order))
          case SortOrder(CountDistinctMolap(attr: AttributeReference), order) => plan.getMeasures().filter(m => m.getMeasure().equalsIgnoreCase(attr.name))(0).setSortOrderType(getSortDirection(order))
          case SortOrder(AverageMolap(attr: AttributeReference, _), order) => plan.getMeasures().filter(m => m.getMeasure().equalsIgnoreCase(attr.name))(0).setSortOrderType(getSortDirection(order))
          case SortOrder(attr: AttributeReference, order) =>
            val dim = plan.getDimensions.filter(m => m.getDimensionUniqueName.equalsIgnoreCase(attr.name))
            if (!dim.isEmpty) {
              dim(0).setSortOrderType(getSortDirection(order))
              orderList.append(dim(0))
            } else {
              allSortExprPushed = false;
            }
          case _ => allSortExprPushed = false;
        }
      case _ =>
    }

    plan.setSortedDimemsions(orderList)

    //limit can be pushed down only if sort is not present or all sort expressions are pushed
    if (allSortExprPushed) limitExpr match {
      case Some(IntegerLiteral(limit)) =>  
        if(plan.getMeasures.size() == 0 && plan.getDimAggregatorInfos.size() == 0)
          plan.setLimit(limit)
      case _ =>
    }
    plan.setDetailQuery(forceDetailedQuery);
    plan.setOutLocationPath(MolapProperties.getInstance().getProperty(MolapCommonConstants.STORE_LOCATION_HDFS));
    plan.setQueryId(System.nanoTime() + "");
    if (!dimensionPredicates.isEmpty) {
      val exps = preProcessExpressions(dimensionPredicates)
      val expressionVal = transformExpression(exps.head)
      //adding dimension used in expression in querystats
      expressionVal.getChildren.filter { x => x.isInstanceOf[ColumnExpression] }.map { y => allDims += y.asInstanceOf[ColumnExpression].getColumnName }
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

  def transformExpression(expr: Expression): MolapExpression = {
    expr match {
      case Or(left, right) => new OrExpression(transformExpression(left), transformExpression(right))
      case And(left, right) => new AndExpression(transformExpression(left), transformExpression(right))
      case EqualTo(left, right) => new EqualToExpression(transformExpression(left), transformExpression(right))
      case Not(EqualTo(left, right)) => new NotEqualsExpression(transformExpression(left), transformExpression(right))
      case IsNotNull(child) => new NotEqualsExpression(transformExpression(child), transformExpression(Literal(null)))
      case Not(In(left, right)) => new NotInExpression(transformExpression(left), new ListExpression(right.map(transformExpression).asJava))
      case In(left, right) => new InExpression(transformExpression(left), new ListExpression(right.map(transformExpression).asJava))
      case Add(left, right) => new AddExpression(transformExpression(left), transformExpression(right))
      case Subtract(left, right) => new SubstractExpression(transformExpression(left), transformExpression(right))
      case Multiply(left, right) => new MultiplyExpression(transformExpression(left), transformExpression(right))
      case Divide(left, right) => new DivideExpression(transformExpression(left), transformExpression(right))
      case GreaterThan(left, right) => new GreaterThanExpression(transformExpression(left), transformExpression(right))
      case LessThan(left, right) => new LessThanExpression(transformExpression(left), transformExpression(right))
      case GreaterThanOrEqual(left, right) => new GreaterThanEqualToExpression(transformExpression(left), transformExpression(right))
      case LessThanOrEqual(left, right) => new LessThanEqualToExpression(transformExpression(left), transformExpression(right))
      case AttributeReference(name, dataType, _, _) => new MolapColumnExpression(name.toString, MolapScalaUtil.convertSparkToMolapDataType(dataType))
      case Literal(name, dataType) => new MolapLiteralExpression(name, MolapScalaUtil.convertSparkToMolapDataType(dataType))
      case Cast(left, right) if (!left.isInstanceOf[Literal]) => transformExpression(left)
      case _ =>
        new SparkUnknownExpression(expr.transform {
          case AttributeReference(name, dataType, _, _) =>
            MolapBoundReference(new MolapColumnExpression(name.toString, MolapScalaUtil.convertSparkToMolapDataType(dataType)), dataType, expr.nullable)
        })
    }
  }

  def getSortDirection(sort: SortDirection) = {
    sort match {
      case Ascending => SortOrderType.ASC
      case Descending => SortOrderType.DSC
    }
  }


  def addPushdownFilters(keys: Seq[Expression], filters: Array[Array[Expression]], conditions: Option[Expression]) {

    //TODO Values in the IN filter is duplicate. replace the list with set  
    val buffer = new ArrayBuffer[Expression]
    keys.zipWithIndex.foreach { a =>
      buffer += In(a._1, filters(a._2)).asInstanceOf[Expression]
    }

    //Let's not pushdown condition. Only filter push down is sufficient. Conditions can be applied on hash join result.
    val cond = if (buffer.size > 1) {
      val e = buffer.remove(0)
      buffer.fold(e)(And(_, _))
    } else {
      buffer.get(0)
    }

    extraPreds = Seq(cond)
  }

  def inputRdd: MolapDataRDD[MolapKey, MolapValue] = {
    //Update the FilterExpressions with extra conditions added through join pushdown
    if (!extraPreds.isEmpty) {
      val exps = preProcessExpressions(extraPreds.toSeq)
      val expressionVal = transformExpression(exps.head)
      val oldExpressionVal = buildMolapPlan.getFilterExpression()
      if (null == oldExpressionVal) {
        buildMolapPlan.setFilterExpression(expressionVal);
      } else {
        buildMolapPlan.setFilterExpression(new AndExpression(oldExpressionVal, expressionVal));
      }
    }

    val conf = new Configuration();
    val model = MolapQueryUtil.createModel(buildMolapPlan, relation.cubeMeta.schema, relation.metaData.cube, relation.cubeMeta.dataPath, relation.cubeMeta.partitioner.partitionCount) //parseQuery(buildMolapPlan, relation.getSchemaPath)
    val splits = MolapQueryUtil.getTableSplits(relation.schemaName, cubeName, buildMolapPlan, relation.cubeMeta.partitioner)
    val kv: KeyVal[MolapKey, MolapValue] = new KeyValImpl()
    //setting queryid
    buildMolapPlan.setQueryId(oc.getConf("queryId", System.nanoTime() + ""))
    handleQueryStats(model)
    MolapQueryUtil.updateMolapExecuterModelWithLoadMetadata(model)
    MolapQueryUtil.setPartitionColumn(model, relation.cubeMeta.partitioner.partitionColumn)
    println("Selected Table to Query ****** " + model.getFactTable())
    
    val catalog = CarbonEnv.getInstance(oc).carbonCatalog
    val cubeCreationTime = catalog.getCubeCreationTime(relation.schemaName, cubeName)
    val schemaLastUpdatedTime = catalog.getSchemaLastUpdatedTime(relation.schemaName, cubeName)
    val big = new MolapDataRDD(
        oc.sparkContext,
        model,
        relation.cubeMeta.schema, 
        relation.cubeMeta.dataPath, 
        kv, 
        conf,
        splits,
        true, 
        cubeCreationTime,
        schemaLastUpdatedTime,
        catalog.metadataPath)
    big
  }

  /**
    * Adding few parameter like accumulator: to get details from executor and queryid to track the query at executor
    */
  def handleQueryStats(model: MolapQueryExecutorModel) {
    val queryStats: QueryDetail = QueryStatsCollector.getInstance.getQueryStats(buildMolapPlan.getQueryId)

    //registering accumulator
    val queryStatsCollector = QueryStatsCollector.getInstance
    val partAcc = oc.sparkContext.accumulator(queryStatsCollector.getInitialPartitionAccumulatorValue)(queryStatsCollector.getPartitionAccumulatorParam)
    model.setPartitionAccumulator(partAcc)
    // querystats will be there only when user do <dataframe>.collect
    //TO-DO need to check for all queries
    if (null != queryStats) {
      val metaPath: String = relation.metaData.cube.getMetaDataFilepath
      queryStats.setMetaPath(metaPath)
      queryStats.setCubeName(cubeName)
      queryStats.setSchemaName(relation.schemaName)
      queryStats.setGroupBy(isGroupByPresent)
      queryStats.setFactTableName(cube.getFactTableName)
      queryStats.setDimOrdinals(MolapQueryUtil.getDimensionOrdinal(cube.getDimensions(cube.getFactTableName), allDims.toArray))
      //check if query has limit parameter
      val limt: Int = buildMolapPlan.getLimit
      if (limt != -1) {
        queryStats.setLimitPassed(true)
      }
      if (!dimensionPredicates.isEmpty) {
        queryStats.setFilterQuery(true)
      }
      queryStats.setPartitionsDetail(partAcc)
    }


  }

  def doExecute() = {
    def toType(obj: Any): Any = obj match {
      case s: String => UTF8String.fromString(s)
      case _ => obj
    }

    inputRdd.map { row =>
      val dims = row._1.getKey.map(toType).toArray
      val values = dims
      new GenericMutableRow(values.asInstanceOf[Array[Any]])
    }
  }

  def output = {
    attributes
  }

}

