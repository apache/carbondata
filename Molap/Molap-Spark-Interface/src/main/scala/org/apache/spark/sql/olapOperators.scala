package org.apache.spark.sql

import scala.Array.canBuildFrom
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.MutableList
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.BinaryNode
import org.apache.spark.sql.execution.LeafNode
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryNode
import org.apache.spark.sql.execution.joins.BuildSide
import org.apache.spark.sql.execution.joins.HashJoin
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.unsafe.types.UTF8String
import com.huawei.datasight.molap.query.MolapQueryPlan
import com.huawei.datasight.molap.query.metadata.MolapDimension
import com.huawei.datasight.molap.query.metadata.MolapDimensionFilter
import com.huawei.datasight.molap.query.metadata.MolapLikeFilter
import com.huawei.datasight.molap.query.metadata.MolapMeasure
import com.huawei.datasight.molap.query.metadata.MolapMeasureFilter
import com.huawei.datasight.molap.query.metadata.SortOrderType
import com.huawei.datasight.molap.spark.util.MolapQueryUtil
import com.huawei.datasight.spark.KeyVal
import com.huawei.datasight.spark.KeyValImpl
import com.huawei.datasight.spark.agg.AverageMolap
import com.huawei.datasight.spark.agg.CountDistinctMolap
import com.huawei.datasight.spark.agg.CountMolap
import com.huawei.datasight.spark.agg.SumMolap
import com.huawei.datasight.spark.processors.MolapScalaUtil
import com.huawei.datasight.spark.processors.SparkTopNProcessor
import com.huawei.datasight.spark.rdd.MolapDataRDD
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator
import com.huawei.unibi.molap.engine.aggregator.impl.SumAggregator
import com.huawei.unibi.molap.engine.executer.impl.topn.TopNModel.MolapTopNType
import com.huawei.unibi.molap.engine.expression.{ColumnExpression => MolapColumnExpression}
import com.huawei.unibi.molap.engine.expression.{Expression => MolapExpression}
import com.huawei.unibi.molap.engine.expression.{LiteralExpression => MolapLiteralExpression}
import com.huawei.unibi.molap.engine.expression.arithmetic.AddExpression
import com.huawei.unibi.molap.engine.expression.arithmetic.DivideExpression
import com.huawei.unibi.molap.engine.expression.arithmetic.MultiplyExpression
import com.huawei.unibi.molap.engine.expression.arithmetic.SubstractExpression
import com.huawei.unibi.molap.engine.expression.conditional.EqualToExpression
import com.huawei.unibi.molap.engine.expression.conditional.NotEqualsExpression
import com.huawei.unibi.molap.engine.expression.logical.AndExpression
import com.huawei.unibi.molap.engine.expression.logical.NotExpression
import com.huawei.unibi.molap.engine.expression.logical.OrExpression
import com.huawei.unibi.molap.engine.molapfilterinterface.RowImpl
import com.huawei.unibi.molap.engine.scanner.impl.MolapKey
import com.huawei.unibi.molap.engine.scanner.impl.MolapValue
import com.huawei.unibi.molap.engine.expression.conditional._
import scala.collection.JavaConverters._
import com.huawei.unibi.molap.util.MolapProperties
import com.huawei.unibi.molap.constants.MolapCommonConstants
import com.huawei.datasight.spark.agg.PositionLiteral
import org.apache.spark.sql.catalyst.InternalRow
import com.huawei.datasight.spark.agg.MinMolap
import com.huawei.datasight.spark.agg.MaxMolap
import com.huawei.datasight.molap.query.metadata.MolapQueryExpression
import com.huawei.unibi.molap.engine.aggregator.impl.CountAggregator
import com.huawei.datasight.molap.query.metadata.MolapColumn
import com.huawei.unibi.molap.engine.querystats.QueryDetail
import com.huawei.unibi.molap.engine.querystats.QueryStatsCollector
import com.huawei.unibi.molap.engine.expression.ColumnExpression
import com.huawei.datasight.spark.agg.AverageMolap
import com.huawei.datasight.spark.agg.CountMolap
import com.huawei.datasight.spark.agg.SumMolap
import com.huawei.datasight.spark.agg.CountDistinctMolap
import com.huawei.datasight.spark.agg.PositionLiteral
import com.huawei.datasight.spark.agg.SumDistinctMolap
import org.apache.spark.sql.execution.joins.HashedRelation
import java.util.ArrayList
import com.huawei.unibi.molap.engine.executer.MolapQueryExecutorModel
import org.apache.spark.sql.CarbonEnv


/**
  * Created by w00228970 on 2014/5/24.
  * Modified by r00900208
  */
case class OlapCubeScan(
                         var attributes: Seq[Attribute],
                         relation: OlapRelation,
                         dimensionPredicates: Seq[Expression],
                         aggExprs: Option[Seq[Expression]],
                         sortExprs: Option[Seq[SortOrder]],
                         limitExpr: Option[Expression],
                         isGroupByPresent: Boolean,
                         detailQuery: Boolean = false
                       )(
                         @transient val oc: SQLContext) // guolv chaozuo zai limian ma?
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
          //Following case is not expected
          //          case par: AggregateExpression1 =>
          //            {
          //              queryOrder = processAggregateExpr(plan, par, queryOrder)
          //              outputColumns += par.flatMap(_.references)(0)
          //            }

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
      //println("Tranform" + transformExpression(dimensionPredicates.head))
      val exps = preProcessExpressions(dimensionPredicates)
      val expressionVal = transformExpression(exps.head)
      //adding dimension used in expression in querystats
      expressionVal.getChildren.filter { x => x.isInstanceOf[ColumnExpression] }.map { y => allDims += y.asInstanceOf[ColumnExpression].getColumnName }
      plan.setFilterExpression(expressionVal);
      //      println(expressionVal.getString())
      //      expressionVal.evaluate(new RowImpl())
      //      println(expressionVal.getString())
    }

    //    if (!msrPredicates.isEmpty) {
    //      addPredicates(plan,msrPredicates,true)
    //    }
    //    extraPreds match {
    //      case Some(exps: Seq[Expression]) =>
    //      case _=>
    //    }
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
    //   val cond = conditions match {
    //   case Some(e:Expression) => buffer.fold(e)(And(_,_))
    //   case _=>
    //   if(buffer.size > 1) {
    //       val e = buffer.remove(0)
    //               buffer.fold(e)(And(_,_))
    //   } else {
    //       buffer.get(0)
    //   }
    //   }

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

    import oc._
    def toType(obj: Any): Any = obj match {
      case s: String => UTF8String.fromString(s)
      case _ => obj
    }

    inputRdd.map { row =>
      val dims = row._1.getKey.map(toType).toArray
      //       val meas = if(detailQuery)row._2.getValues.map(_.getValue) else row._2.getValues
      val values = dims
      new GenericMutableRow(values.asInstanceOf[Array[Any]])
    }
  }

  def output = {
    //    val dims = MutableList[Attribute]()
    //    val msrs = MutableList[Attribute]()

    //    attributes.map(
    //    attr => {
    //      val molapDimension = MolapQueryUtil.getMolapDimension(cube.getDimensions(cube.getFactTableName()), attr.name);
    //      if(molapDimension != null){
    //        dims += attr
    //      }
    //      else
    //      {
    //        val molapMeasure = MolapQueryUtil.getMolapMeasure(attr.name, cube.getMeasures(cube.getFactTableName()));
    //        if(molapMeasure != null)
    //        {
    //        	msrs += attr
    //        }
    //      }
    //    })
    //    dims++msrs
    attributes
  }

}


//case class TopN(count:Int,topOrBottom:Int,dim:Attribute,msr:Attribute,
//    child: SparkPlan)(@transient sc: SparkContext)
//  extends UnaryNode {
//
//  override def otherCopyArgs = sc :: Nil
//  override def doExecute() = sc.makeRDD(executeCollect(), 1)
//  
//  override def executeCollect() = {
//    val out =  child.output
//    val dims = new MutableList[Attribute]()
//    val msrs = new MutableList[Attribute]()
//    val dimPositions = new MutableList[Int]()
//    val msrPositions = new MutableList[Int]()
//    val first = child.execute().first
//    for(j<-0 until first.numFields)
//    {
//      first.apply(j) match {
//    	      case m:MeasureAggregator => { msrPositions += j ;msrs += out(j) }
//    	      case m1:Double => { msrPositions += j ;msrs += out(j) }
//    	      case others => {dimPositions += j; dims += out(j) }
//      }
//    }
//    
//    val s = child.execute()
//      .mapPartitions(
//        iterator => iterator.map{x=>
//          val key = new MutableList[Object]()
//          val value = new MutableList[MeasureAggregator]()
//          for(j<-0 until x.numFields)
//    	  {
//    	    x.apply(j) match {
//    	      case m:MeasureAggregator => value += m
//    	      case m1:Double => {
//    	        val sumAggregator = new SumAggregator()
//    	        sumAggregator.setNewValue(m1)
//    	        value += sumAggregator
//    	        }
//    	      case others => key += x.apply(j).asInstanceOf[Object]
//    	    }
//    	  }
//          (new MolapKey(key.toArray),new MolapValue(value.toArray))
//        },
//        preservesPartitioning = true)
//        var topType = MolapTopNType.TOP
//        if(topOrBottom == 1)
//        {
//          topType = MolapTopNType.BOTTOM
//        }
//        var dimIndex = 0;
//        var msrIndex = 0;
//        var index = 0;
//        dims.map{x=>
//          if(x.name.equals(dim.name))
//          {
//            dimIndex = index
//          }
//          index = index+1
//        }
//        index = 0;
//        msrs.map{x=>  
//          if(x.name.equals(msr.name))
//          {
//            msrIndex = index
//          }
//          index = index+1
//        }
//        val result = new SparkTopNProcessor().process(s, count,dimIndex,msrIndex,topType)
//        result.map{k => 
//                 val finaldims = k._1.getKey.toArray
//                 val finalmsrs = k._2.getValues.map(_.getValue).toArray
//                 
//                 val values = new Array[Any](dimPositions.length+msrPositions.length) 
//                   
//                 for(i <- 0 until dims.length){
//                   values(dimPositions(i)) = finaldims(i)
//                 }
//                 for(i <- 0 until msrs.length){
//                   values(msrPositions(i)) = finalmsrs(i)
//                 }
//                 new GenericRow(values)
//          }
//  }
//
//  override def output = child.output
//}
