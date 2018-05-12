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
package org.apache.carbondata.mv.datamap

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, Expression, NamedExpression, ScalaUDF}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.execution.command.{Field, TableModel, TableNewProcessor}
import org.apache.spark.sql.execution.command.table.CarbonCreateTableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.parser.CarbonSpark2SqlParser

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.schema.table.{DataMapSchema, DataMapSchemaStorageProvider, RelationIdentifier}
import org.apache.carbondata.mv.plans.modular
import org.apache.carbondata.mv.plans.modular.{GroupBy, Matchable, ModularPlan, Select}
import org.apache.carbondata.mv.rewrite.{DefaultMatchMaker, QueryRewrite}
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Utility for MV datamap operations.
 */
object MVHelper {

  def createMVDataMap(sparkSession: SparkSession,
      dataMapSchema: DataMapSchema,
      queryString: String,
      ifNotExistsSet: Boolean = false): Unit = {
    val dmProperties = dataMapSchema.getProperties.asScala
    val updatedQuery = new CarbonSpark2SqlParser().addPreAggFunction(queryString)
    val logicalPlan = sparkSession.sql(updatedQuery).drop("preAgg").queryExecution.analyzed
    val fields = logicalPlan.output.map { attr =>
      val name = updateColumnName(attr)
      val rawSchema = '`' + name + '`' + ' ' + attr.dataType.typeName
      if (attr.dataType.typeName.startsWith("decimal")) {
        val (precision, scale) = CommonUtil.getScaleAndPrecision(attr.dataType.catalogString)
        Field(column = name,
          dataType = Some(attr.dataType.typeName),
          name = Some(name),
          children = None,
          precision = precision,
          scale = scale,
          rawSchema = rawSchema)
      } else {
        Field(column = name,
          dataType = Some(attr.dataType.typeName),
          name = Some(name),
          children = None,
          rawSchema = rawSchema)
      }
    }
    val tableProperties = mutable.Map[String, String]()
    dmProperties.foreach(t => tableProperties.put(t._1, t._2))

    val selectTables = getTables(logicalPlan)

    // TODO inherit the table properties like sort order, sort scope and block size from parent
    // tables to mv datamap table
    // TODO Use a proper DB
    val tableIdentifier =
    TableIdentifier(dataMapSchema.getDataMapName + "_table",
      selectTables.head.identifier.database)
    // prepare table model of the collected tokens
    val tableModel: TableModel = new CarbonSpark2SqlParser().prepareTableModel(
      ifNotExistPresent = ifNotExistsSet,
      new CarbonSpark2SqlParser().convertDbNameToLowerCase(tableIdentifier.database),
      tableIdentifier.table.toLowerCase,
      fields,
      Seq(),
      tableProperties,
      None,
      isAlterFlow = false,
      None)

    val tablePath = if (dmProperties.contains("path")) {
      dmProperties("path")
    } else {
      CarbonEnv.getTablePath(tableModel.databaseNameOp, tableModel.tableName)(sparkSession)
    }
    CarbonCreateTableCommand(TableNewProcessor(tableModel),
      tableModel.ifNotExistsSet, Some(tablePath), isVisible = false).run(sparkSession)

    dataMapSchema.setCtasQuery(queryString)
    dataMapSchema
      .setRelationIdentifier(new RelationIdentifier(tableIdentifier.database.get,
        tableIdentifier.table,
        ""))

    val parentIdents = selectTables.map { table =>
      new RelationIdentifier(table.database, table.identifier.table, "")
    }
    dataMapSchema.setParentTables(new util.ArrayList[RelationIdentifier](parentIdents.asJava))
    DataMapStoreManager.getInstance().saveDataMapSchema(dataMapSchema)
  }

  def updateColumnName(attr: Attribute): String = {
    val name = attr.name.replace("(", "_").replace(")", "").replace(" ", "_").replace("=", "")
    attr.qualifier.map(qualifier => qualifier + "_" + name).getOrElse(name)
  }

  def getTables(logicalPlan: LogicalPlan): Seq[CatalogTable] = {
    logicalPlan.collect {
      case l: LogicalRelation => l.catalogTable.get
    }
  }

  def dropDummFuc(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case p@Project(exps, child) =>
        Project(dropDummyExp(exps), child)
      case Aggregate(grp, aggExp, child) =>
        Aggregate(
          grp,
          dropDummyExp(aggExp),
          child)
    }
  }

  private def dropDummyExp(exps: Seq[NamedExpression]) = {
    exps.map {
      case al@Alias(udf: ScalaUDF, name) if name.equalsIgnoreCase("preAgg") => None
      case attr: AttributeReference if attr.name.equalsIgnoreCase("preAgg") => None
      case other => Some(other)
    }.filter(_.isDefined).map(_.get)
  }

  def getAttributeMap(subsumer: Seq[NamedExpression],
      subsume: Seq[NamedExpression]): Map[AttributeKey, NamedExpression] = {
    if (subsumer.length == subsume.length) {
      subsume.zip(subsumer).flatMap { case (left, right) =>
        var tuples = left collect {
          case attr: AttributeReference =>
            (AttributeKey(attr), createAttrReference(right, attr.name))
        }
        left match {
          case a: Alias =>
            tuples = Seq((AttributeKey(a.child), createAttrReference(right, a.name))) ++ tuples
          case _ =>
        }
        Seq((AttributeKey(left), createAttrReference(right, left.name))) ++ tuples
      }.toMap
    } else {
      throw new UnsupportedOperationException("Cannot create mapping with unequal sizes")
    }
  }

  def createAttrReference(ref: NamedExpression, name: String): Alias = {
    Alias(ref, name)(exprId = ref.exprId, qualifier = None)
  }

  case class AttributeKey(exp: Expression) {

    override def equals(other: Any): Boolean = other match {
      case attrKey: AttributeKey =>
        exp.semanticEquals(attrKey.exp)
      case _ => false
    }

    override def hashCode: Int = exp.hashCode

  }

  /**
   * Updates the expressions as per the subsumer output expressions. It is needed to update the
   * expressions as per the datamap table relation
   *
   * @param expressions        expressions which are needed to update
   * @param aliasName          table alias name
   * @return Updated expressions
   */
  def updateSubsumeAttrs(
      expressions: Seq[Expression],
      attrMap: Map[AttributeKey, NamedExpression],
      aliasName: Option[String],
      keepAlias: Boolean = false): Seq[Expression] = {

    def getAttribute(exp: Expression) = {
      exp match {
        case Alias(agg: AggregateExpression, name) =>
          agg.aggregateFunction.collect {
            case attr: AttributeReference =>
              AttributeReference(attr.name, attr.dataType, attr.nullable, attr
                .metadata)(attr.exprId,
                aliasName,
                attr.isGenerated)
          }.head
        case Alias(child, name) =>
          child
        case other => other
      }
    }

    expressions.map {
        case alias@Alias(agg: AggregateExpression, name) =>
          attrMap.get(AttributeKey(alias)).map { exp =>
            Alias(getAttribute(exp), name)(alias.exprId,
              alias.qualifier,
              alias.explicitMetadata,
              alias.isGenerated)
          }.getOrElse(alias)

        case attr: AttributeReference =>
          val uattr = attrMap.get(AttributeKey(attr)).map{a =>
            if (keepAlias) {
              AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(a.exprId,
                attr.qualifier,
                a.isGenerated)
            } else {
              a
            }
          }.getOrElse(attr)
          uattr
        case expression: Expression =>
          val uattr = attrMap.getOrElse(AttributeKey(expression), expression)
          uattr
    }
  }

  def updateOutPutList(
      subsumerOutputList: Seq[NamedExpression],
      dataMapRltn: Select,
      aliasMap: Map[AttributeKey, NamedExpression],
      keepAlias: Boolean): Seq[NamedExpression] = {
    var outputSel =
      updateSubsumeAttrs(
        subsumerOutputList,
        aliasMap,
        Some(dataMapRltn.aliasMap.values.head),
        keepAlias).asInstanceOf[Seq[NamedExpression]]
    outputSel.zip(subsumerOutputList).map{ case (l, r) =>
      l match {
        case attr: AttributeReference =>
          Alias(attr, r.name)(r.exprId, None)
        case a@Alias(attr: AttributeReference, name) =>
          Alias(attr, r.name)(r.exprId, None)
        case other => other
      }
    }

  }

  def updateSelectPredicates(
      predicates: Seq[Expression],
      attrMap: Map[AttributeKey, NamedExpression],
      keepAlias: Boolean): Seq[Expression] = {
    predicates.map { exp =>
      exp transform {
        case attr: AttributeReference =>
          val uattr = attrMap.get(AttributeKey(attr)).map{a =>
            if (keepAlias) {
              AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(a.exprId,
                attr.qualifier,
                a.isGenerated)
            } else {
              a
            }
          }.getOrElse(attr)
          uattr
      }
    }
  }

  /**
   * Update the modular plan as per the datamap table relation inside it.
   *
   * @param subsumer plan to be updated
   * @return Updated modular plan.
   */
  def updateDataMap(subsumer: ModularPlan, rewrite: QueryRewrite): ModularPlan = {
    subsumer match {
      case s: Select if s.dataMapTableRelation.isDefined =>
        val relation = s.dataMapTableRelation.get.asInstanceOf[Select]
        val mappings = s.outputList zip relation.outputList
        val oList = for ((o1, o2) <- mappings) yield {
          if (o1.name != o2.name) Alias(o2, o1.name)(exprId = o1.exprId) else o2
        }
        relation.copy(outputList = oList).setRewritten()
      case g: GroupBy if g.dataMapTableRelation.isDefined =>
        val relation = g.dataMapTableRelation.get.asInstanceOf[Select]
        val in = relation.asInstanceOf[Select].outputList
        val mappings = g.outputList zip relation.outputList
        val oList = for ((left, right) <- mappings) yield {
          left match {
            case Alias(agg@AggregateExpression(fun@Sum(child), _, _, _), name) =>
              val uFun = fun.copy(child = right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@Max(child), _, _, _), name) =>
              val uFun = fun.copy(child = right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@Min(child), _, _, _), name) =>
              val uFun = fun.copy(child = right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@Count(Seq(child)), _, _, _), name) =>
              val uFun = Sum(right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case _ =>
              if (left.name != right.name) Alias(right, left.name)(exprId = left.exprId) else right
          }
        }
        val updatedPredicates = g.predicateList.map { f =>
          mappings.find{ case (k, y) =>
            k match {
              case a: Alias if f.isInstanceOf[Alias] =>
                a.child.semanticEquals(f.children.head)
              case a: Alias => a.child.semanticEquals(f)
              case other => other.semanticEquals(f)
            }
          } match {
            case Some(r) => r._2
            case _ => f
          }
        }
        g.copy(outputList = oList,
          inputList = in,
          predicateList = updatedPredicates,
          child = relation,
          dataMapTableRelation = None).setRewritten()

      case select: Select =>
        select.children match {
          case Seq(s: Select) if s.dataMapTableRelation.isDefined =>
            val relation = s.dataMapTableRelation.get.asInstanceOf[Select]
            val child = updateDataMap(s, rewrite).asInstanceOf[Select]
            val aliasMap = getAttributeMap(relation.outputList, s.outputList)
            var outputSel =
              updateOutPutList(select.outputList, relation, aliasMap, keepAlias = true)
            val pred = updateSelectPredicates(select.predicateList, aliasMap, true)
            select.copy(outputList = outputSel,
              inputList = child.outputList,
              predicateList = pred,
              children = Seq(child)).setRewritten()

          case Seq(g: GroupBy) if g.dataMapTableRelation.isDefined =>
            val relation = g.dataMapTableRelation.get.asInstanceOf[Select]
            val aliasMap = getAttributeMap(relation.outputList, g.outputList)

            val outputSel =
              updateOutPutList(select.outputList, relation, aliasMap, keepAlias = false)
            val child = updateDataMap(g, rewrite).asInstanceOf[Matchable]
            // TODO Remove the unnecessary columns from selection.
            // Only keep columns which are required by parent.
            val inputSel = child.outputList
            select.copy(
              outputList = outputSel,
              inputList = inputSel,
              children = Seq(child)).setRewritten()

          case _ => select
        }

      case other => other
    }
  }
}

