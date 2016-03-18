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

/**
  *
  */
package com.huawei.datasight.spark.processors

import java.util.List

import org.apache.spark.sql.hive.OlapMetaData

import scala.Array.canBuildFrom
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.asScalaSet
import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.MutableList

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.Column
import org.apache.spark.sql.OlapContext
import org.apache.spark.sql.OlapRelation
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.CountDistinct
import org.apache.spark.sql.catalyst.expressions.Descending
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GreaterThan
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual
import org.apache.spark.sql.catalyst.expressions.In
import org.apache.spark.sql.catalyst.expressions.LessThan
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual
import org.apache.spark.sql.catalyst.expressions.Like
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Not
import org.apache.spark.sql.catalyst.expressions.SortOrder

import com.huawei.datasight.spark.agg.AverageMolap
import com.huawei.datasight.spark.agg.CountDistinctMolap
import com.huawei.datasight.spark.agg.CountMolap
import com.huawei.datasight.spark.agg.FlatAggregatorsExpr
import com.huawei.datasight.spark.agg.FlattenExpr
import com.huawei.datasight.spark.agg.MaxMolap
import com.huawei.datasight.spark.agg.MinMolap
import com.huawei.datasight.spark.agg.SumMolap
import com.huawei.datasight.spark.dsl.interpreter.EvaluatorAttributeModel
import com.huawei.datasight.spark.dsl.interpreter.MolapSparkInterpreter
import com.huawei.datasight.spark.rdd.AddColumnExpression
import com.huawei.datasight.spark.rdd.GroupbyExpression
import com.huawei.unibi.molap.engine.directinterface.impl.MolapQueryModel
import com.huawei.unibi.molap.engine.filters.metadata.ContentMatchFilterInfo
import com.huawei.unibi.molap.engine.filters.measurefilter.MeasureFilterModel
import com.huawei.unibi.molap.filter.MolapFilterInfo
import com.huawei.unibi.molap.metadata.CalculatedMeasure
import com.huawei.unibi.molap.metadata.MolapMetadata
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube
import com.huawei.unibi.molap.query.metadata.DSLTransformation

/**
  * @author R00900208
  *
  */
trait Transform {
  def transform(trans: TransformHolder): TransformHolder

  def getAggregateExpression(aggName: String, name: String, expr: Expression)(olapContext: OlapContext) = {
    import olapContext._

    aggName match {
      case "sum" => Alias(SumMolap(expr).asInstanceOf[Expression], name)()
      case "count" => Alias(CountMolap(expr).asInstanceOf[Expression], name)()
      case "max" => Alias(MaxMolap(expr).asInstanceOf[Expression], name)()
      case "min" => Alias(MinMolap(expr).asInstanceOf[Expression], name)()
      case "distinct-count" => Alias(CountDistinctMolap(expr).asInstanceOf[Expression], name)()
      case "avg" => Alias(AverageMolap(expr).asInstanceOf[Expression], name)()
      case _ => Alias(SumMolap(expr).asInstanceOf[Expression], name)()
    }
  }
}

case class AddProjection(olapContext: OlapContext, queryModel: MolapQueryModel, molapDerivedInfo: MolapDerivedInfo) extends Transform {
  def transform(trans: TransformHolder): TransformHolder = {
    import olapContext._
    val dims: Buffer[UnresolvedAttribute] = queryModel.getQueryDims.map(x => UnresolvedAttribute(x.getName()))
    val msrs: Buffer[UnresolvedAttribute] = queryModel.getMsrs.map(x => UnresolvedAttribute(x.getName()))
    val molapTransformations: List[DSLTransformation] = queryModel.getMolapTransformations()
    val filterDims: Buffer[UnresolvedAttribute] = queryModel.getConstraints().keySet().map(x => UnresolvedAttribute(x.getName())).toBuffer
    val extraDimsFromFilter = filterDims.filterNot(p => dims.contains(p))

    val extraDimAttribs: Buffer[UnresolvedAttribute] = molapDerivedInfo.extraDimColNames.map(x => UnresolvedAttribute(x))
    val extraMsrAttribs: Buffer[UnresolvedAttribute] = molapDerivedInfo.extraMsrColNames.map(x => UnresolvedAttribute(x))
    val calcMsrs: Buffer[UnresolvedAttribute] = queryModel.getCalcMsrs().map(x => UnresolvedAttribute(x.asInstanceOf[CalculatedMeasure].getDistCountDim().getName()))

    val all: Buffer[UnresolvedAttribute] = dims ++ extraDimAttribs ++ msrs ++ extraMsrAttribs ++ (calcMsrs -- dims) ++ extraDimsFromFilter

    val select = trans.rdd.asInstanceOf[SchemaRDD].select(all.map(new Column(_)): _*)
    TransformHolder(select, trans.mataData)
  }
}

case class AddPredicate(olapContext: OlapContext, queryModel: MolapQueryModel) extends Transform {
  def transform(trans: TransformHolder): TransformHolder = {
    import olapContext._
    val itr = queryModel.getConstraints().iterator
    var where: SchemaRDD = trans.rdd.asInstanceOf[SchemaRDD]
    while (itr.hasNext) {
      val tuple = itr.next
      tuple._2 match {
        case t: ContentMatchFilterInfo =>
          if (t.getIncludedMembers().size() > 0)
            where = where.where(new Column(In(UnresolvedAttribute(tuple._1.getName()), t.getIncludedMembers().map(x => Literal(x)))))
          if (t.getExcludedMembers().size() > 0)
            where = where.where(new Column(Not(In(UnresolvedAttribute(tuple._1.getName()), t.getExcludedMembers().map(x => Literal(x))))))
          if (t.getIncludedContentMatchMembers().size() > 0)
            where = where.where(new Column(Like(UnresolvedAttribute(tuple._1.getName()), Literal(t.getIncludedContentMatchMembers.get(0)))))
          if (t.getExcludedContentMatchMembers().size() > 0)
            where = where.where(new Column(Not(Like(UnresolvedAttribute(tuple._1.getName()), Literal(t.getExcludedContentMatchMembers.get(0))))))
        case t: MolapFilterInfo =>
          if (t.getIncludedMembers().size() > 0)
            where = where.where(new Column(In(UnresolvedAttribute(tuple._1.getName()), t.getIncludedMembers().map(x => Literal(x)))))
          if (t.getExcludedMembers().size() > 0)
            where = where.where(new Column(Not(In(UnresolvedAttribute(tuple._1.getName()), t.getExcludedMembers().map(x => Literal(x))))))
      }
    }
    val msrIter = queryModel.getMsrFilter().iterator
    while (msrIter.hasNext) {
      val tuple = msrIter.next
      tuple._2.foreach(x => where = where.where(x.getFilterType() match {
        case MeasureFilterModel.MeasureFilterType.GREATER_THAN =>
          new Column(GreaterThan(UnresolvedAttribute(tuple._1.getName()), Literal(x.getFilterValue())))
        case MeasureFilterModel.MeasureFilterType.GREATER_THAN_EQUAL =>
          new Column(GreaterThanOrEqual(UnresolvedAttribute(tuple._1.getName()), Literal(x.getFilterValue())))
        case MeasureFilterModel.MeasureFilterType.LESS_THAN =>
          new Column(LessThan(UnresolvedAttribute(tuple._1.getName()), Literal(x.getFilterValue())))
        case MeasureFilterModel.MeasureFilterType.GREATER_THAN =>
          new Column(LessThanOrEqual(UnresolvedAttribute(tuple._1.getName()), Literal(x.getFilterValue())))
      }
      ))
    }
    TransformHolder(where, trans.mataData)
  }
}

case class AddGroupBy(olapContext: OlapContext, queryModel: MolapQueryModel, molapDerivedInfo: MolapDerivedInfo) extends Transform {
  def transform(trans: TransformHolder): TransformHolder = {
    import olapContext._
    val dims: Buffer[Expression] = queryModel.getQueryDims.map(x => UnresolvedAttribute(x.getName()))
    val countMsrDims: Buffer[Expression] = queryModel.getCalcMsrs.map(x => UnresolvedAttribute(x.asInstanceOf[CalculatedMeasure].getDistCountDim().getName()))
    val msrs: Buffer[Expression] = queryModel.getMsrs.filter { x => !(x.isInstanceOf[CalculatedMeasure]) }.map(x => UnresolvedAttribute(x.getName()))
    val extraDimAttribs: Buffer[UnresolvedAttribute] = molapDerivedInfo.extraDimColNames.map(x => UnresolvedAttribute(x))
    val extraMsrGroupingExprs: Buffer[Expression] = molapDerivedInfo.extraMsrColNames.map(x => Alias(SumMolap(UnresolvedAttribute(x)).asInstanceOf[Expression], x)())

    var i = -1;
    val groupingExprs: Buffer[Expression] = (msrs).map { x =>
      i = i + 1
      getAggregateExpression(queryModel.getMsrs.get(i).getAggName(), queryModel.getMsrs.get(i).getName(), x)(olapContext)
    }
    //     SumMolap(x).as(Symbol(queryModel.getMsrs.get(i).getName()))}
    val allDims = dims ++ (countMsrDims -- dims -- extraDimAttribs) ++ extraDimAttribs
    //Vinod to recheck functionality
    val groupingColumn = (allDims ++ groupingExprs ++ extraMsrGroupingExprs).map(new Column(_))

    val group = trans.rdd.asInstanceOf[SchemaRDD].groupBy(allDims.map(new Column(_)): _*).agg(groupingColumn.head, groupingColumn.tail: _*)
    TransformHolder(group, trans.mataData)
  }
}

case class AddFinalSelectionGroupOrderBy(olapContext: OlapContext, queryModel: MolapQueryModel) extends Transform {
  def transform(trans: TransformHolder): TransformHolder = {
    import olapContext._

    val dims: Buffer[Expression] = queryModel.getQueryDims.map(x => UnresolvedAttribute(x.getName()))
    val countMsrDims = queryModel.getCalcMsrs.map(x => UnresolvedAttribute(x.getName()))
    val msrs: Buffer[Expression] = queryModel.getMsrs.map(x => UnresolvedAttribute(x.getName()))
    var allDims: Buffer[Expression] = dims
    if (queryModel.getMolapTransformations() != null) {
      val trans = queryModel.getMolapTransformations().filter(_.isAddAsColumn()).map(x => UnresolvedAttribute(x.getNewColumnName()))
      allDims = allDims ++ trans
    }

    var allMeasures: Buffer[Expression] = new ArrayBuffer[Expression]
    var i = -1;
    val groupingExprs: Buffer[Expression] = msrs.map { x =>
      i = i + 1



      allMeasures.add(Alias(FlatAggregatorsExpr(x), queryModel.getCalcMsrs.get(i).getName())())
      //     SumMolap(x).as(Symbol(queryModel.getMsrs.get(i).getName()))
      getAggregateExpression(queryModel.getMsrs.get(i).getAggName(), queryModel.getMsrs.get(i).getName(), x)(olapContext)
    }
    i = -1;
    val countGroupingExprs: Buffer[Expression] = countMsrDims.map { x =>
      i = i + 1
      allMeasures.add(Alias(FlatAggregatorsExpr(x), queryModel.getCalcMsrs.get(i).getName())())

      Alias(CountDistinct(MutableList(x)), queryModel.getCalcMsrs.get(i).getName())()
    }

    //Vinod To check the functionality
    val groupingcolumn = (allDims ++ groupingExprs ++ countGroupingExprs).map {
      new Column(_)
    }
    var group = trans.rdd.asInstanceOf[SchemaRDD].groupBy(allDims.map(new Column(_)): _*).agg(groupingcolumn.head, groupingcolumn.tail: _*)
      .select((allDims ++ allMeasures).map(new Column(_)): _*)

    val sortDims: Buffer[Expression] = queryModel.getGlobalQueryDims().map(x => UnresolvedAttribute(x.getName()))

    var k: Int = 0;
    val arr = queryModel.getGlobalDimSortTypes();
    //	val sortExprs:Buffer[SortOrder] = sortDims.map(x=>if(arr(k) == 0) {k=k+1;x.asc } else {k=k+1;x.desc})
    //    var sortExprs:Buffer[SortOrder] = sortDims.map(x=>if(arr(k) == 0) {k=k+1;x.asc } else if(arr(k) == 1) {k=k+1;x.desc} else {k=k+1;null})
    var sortExprs: Buffer[SortOrder] = sortDims.map(x => {
      val ret = arr(k) match {
        case 0 => SortOrder(x, Ascending)
        case 1 => SortOrder(x, Descending)
        case _ => null
      }
      k += 1
      ret
    })

    if (queryModel.getSortModel() != null && queryModel.getSortModel().getMeasure() != null) {
      var msrExpr = FlattenExpr(UnresolvedAttribute(queryModel.getSortModel().getMeasure().getName()));
      sortExprs.append(
        queryModel.getSortModel().getSortOrder() match {
          case 0 => SortOrder(msrExpr, Ascending)
          case 1 => SortOrder(msrExpr, Descending)
          case _ => null
        })
    }

    sortExprs = sortExprs.filter(_ != null)

    var order = group.asInstanceOf[SchemaRDD]
    if (!sortExprs.isEmpty) {
      // val group = trans.rdd.asInstanceOf[SchemaRDD].groupBy(allDims : _*)(allDims++groupingExprs++countGroupingExprs : _*)
      order = order.orderBy(sortExprs.map(new Column(_)): _*)
    }

    TransformHolder(order, trans.mataData)
  }

}

case class AddDSL(olapContext: OlapContext, queryModel: MolapQueryModel) extends Transform {
  def transform(trans: TransformHolder): TransformHolder = {
    import olapContext._
    val molapTransformations: List[DSLTransformation] = queryModel.getMolapTransformations()
    var transUpdate = trans;
    if (molapTransformations != null && molapTransformations.size > 0) {
      val attr = new EvaluatorAttributeModel(sc, olapContext, transUpdate)
      val strBuffer = new StringBuffer()
      molapTransformations.toArray(Array[DSLTransformation]()).map(x => strBuffer.append(addAlias(x.getDslExpression())).append("\n"))
      transUpdate = new MolapSparkInterpreter().evaluate(strBuffer.toString(), attr)
    }
    TransformHolder(transUpdate.rdd, transUpdate.mataData)
  }

  def addAlias(dsl: String): String = "curr = {" + dsl + "}" + "\n curr = curr.as('curr)"
}

case class AddCount(olapContext: OlapContext, queryModel: MolapQueryModel) extends Transform {
  def transform(trans: TransformHolder): TransformHolder = {
    import olapContext._
    if (queryModel.getCalcMsrs.size > 0) {
      val dims: Buffer[Expression] = queryModel.getQueryDims.map(x => UnresolvedAttribute(x.getName()))
      val countMsrDims = queryModel.getCalcMsrs.map(x => UnresolvedAttribute(x.asInstanceOf[CalculatedMeasure].getDistCountDim().getName()))
      val msrs: Buffer[Expression] = queryModel.getMsrs.map(x => UnresolvedAttribute(x.getName()))
      var allDims: Buffer[Expression] = dims
      if (queryModel.getMolapTransformations() != null) {
        val trans = queryModel.getMolapTransformations().filter(_.isAddAsColumn()).map(x => UnresolvedAttribute(x.getNewColumnName()))
        allDims = allDims ++ trans
      }
      var i = -1;
      val groupingExprs: Buffer[Expression] = msrs.map { x =>
        i = i + 1
        //	     SumMolap(x).as(Symbol(queryModel.getMsrs.get(i).getName()))
        getAggregateExpression(queryModel.getMsrs.get(i).getAggName(), queryModel.getMsrs.get(i).getName(), x)(olapContext)
      }
      i = -1;
      val countGroupingExprs: Buffer[Expression] = countMsrDims.map { x =>
        i = i + 1
        Alias(CountDistinct(MutableList(x)), queryModel.getCalcMsrs.get(i).getName())()
      }
      // CountDistinct(MutableList(x)).as(Symbol(queryModel.getCalcMsrs.get(i).getName()))}
      //Vinod to recheck functionality
      val groupingColumn = (allDims ++ groupingExprs ++ countGroupingExprs).map(new Column(_))
      val group = trans.rdd.asInstanceOf[SchemaRDD].groupBy(allDims.map(new Column(_)): _*).agg(groupingColumn.head, groupingColumn: _*)


      return TransformHolder(group, trans.mataData)
    }
    return trans
  }
}

case class AddTopN(olapContext: OlapContext, queryModel: MolapQueryModel) extends Transform {
  def transform(trans: TransformHolder): TransformHolder = {
    import olapContext._
    val topN = queryModel.getTopNModel()
    if (topN != null) {
      val topRDD = trans.rdd.asInstanceOf[SchemaRDD].topN(topN.getCount(), UnresolvedAttribute(topN.getDimension().getName()), UnresolvedAttribute(topN.getMeasure().getName()))
      return TransformHolder(topRDD, trans.mataData)
    }
    return trans
  }
}

case class DoOperation(olapContext: OlapContext, rdd: SchemaRDD, exprType: Any, columnName: String) {
  def apply() = {
    import olapContext._
    exprType match {
      case group: GroupbyExpression => {
        group.getGroupByColumn match {
          case d: UnresolvedAttribute => rdd.addColumn(group.getGroupByColumn)
        }
      }
      case add: AddColumnExpression => rdd.addColumn(Alias(add.getExpression, columnName)())
    }
  }
}

case class TransformHolder(rdd: Any, mataData: OlapMetaData)

case class MolapDerivedInfo(extraDimColNames: Buffer[String], extraMsrColNames: Buffer[String])


object OlapUtil {
  def createSparkMeta(cube: Cube): OlapMetaData = {
    val dimensionsAttr = cube.getDimensions(cube.getFactTableName).map(x => x.getName()) // wf : may be problem
    val measureAttr = cube.getMeasures(cube.getFactTableName).map(x => x.getName())
    OlapMetaData(dimensionsAttr, measureAttr, cube)
  }

  def createBaseRDD(olapContext: OlapContext, cube: Cube): TransformHolder = {
    val olapCube = MolapMetadata.getInstance().getCube(cube.getCubeName())
    val relation = CarbonEnv.getInstance(olapContext).carbonCatalog.lookupRelation1(Option(cube.getSchemaName()),
      cube.getOnlyCubeName(), None)(olapContext).asInstanceOf[OlapRelation]
    var rdd = new SchemaRDD(olapContext, relation)
    rdd.registerTempTable(cube.getOnlyCubeName())
    TransformHolder(rdd, createSparkMeta(cube))
  }

}

