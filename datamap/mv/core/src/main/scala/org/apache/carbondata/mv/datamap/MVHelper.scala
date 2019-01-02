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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{CarbonEnv, CarbonToSparkAdapter, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, Expression, NamedExpression, ScalaUDF, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.execution.command.{Field, TableModel, TableNewProcessor}
import org.apache.spark.sql.execution.command.table.CarbonCreateTableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.parser.CarbonSpark2SqlParser

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.core.metadata.schema.table.{DataMapSchema, RelationIdentifier}
import org.apache.carbondata.datamap.DataMapManager
import org.apache.carbondata.mv.plans.modular.{GroupBy, Matchable, ModularPlan, Select}
import org.apache.carbondata.mv.rewrite.{MVPlanWrapper, QueryRewrite, SummaryDatasetCatalog}
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
    val query = sparkSession.sql(updatedQuery)
    val logicalPlan = MVHelper.dropDummFuc(query.queryExecution.analyzed)
    validateMVQuery(sparkSession, logicalPlan)
    val fullRebuild = isFullReload(logicalPlan)
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
    selectTables.foreach { selectTable =>
      val mainCarbonTable = try {
        Some(CarbonEnv.getCarbonTable(selectTable.identifier.database,
          selectTable.identifier.table)(sparkSession))
      } catch {
        // Exception handling if it's not a CarbonTable
        case ex : Exception => None
      }

      if (!mainCarbonTable.isEmpty && mainCarbonTable.get.isStreamingSink) {
        throw new MalformedCarbonCommandException(
          s"Streaming table does not support creating MV datamap")
      }
    }

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
      isPreAggFlow = false,
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
    dataMapSchema.getProperties.put("full_refresh", fullRebuild.toString)
    DataMapStoreManager.getInstance().saveDataMapSchema(dataMapSchema)
  }

  private def validateMVQuery(sparkSession: SparkSession,
      logicalPlan: LogicalPlan): Unit = {
    val dataMapProvider = DataMapManager.get().getDataMapProvider(null,
      new DataMapSchema("", DataMapClassProvider.MV.getShortName), sparkSession)
    var catalog = DataMapStoreManager.getInstance().getDataMapCatalog(dataMapProvider,
      DataMapClassProvider.MV.getShortName).asInstanceOf[SummaryDatasetCatalog]
    if (catalog == null) {
      catalog = new SummaryDatasetCatalog(sparkSession)
    }
    val modularPlan =
      catalog.mvSession.sessionState.modularizer.modularize(
        catalog.mvSession.sessionState.optimizer.execute(logicalPlan)).next().semiHarmonized

    // Only queries which can be select , predicate , join, group by and having queries.
    if (!modularPlan.isSPJGH)  {
      throw new UnsupportedOperationException("MV is not supported for this query")
    }
    val isValid = modularPlan match {
      case g: GroupBy =>
        // Make sure all predicates are present in projections.
        g.predicateList.forall{p =>
          g.outputList.exists{
            case a: Alias =>
              a.semanticEquals(p) || a.child.semanticEquals(p)
            case other => other.semanticEquals(p)
          }
        }
      case _ => true
    }
    if (!isValid) {
      throw new UnsupportedOperationException("Group by columns must be present in project columns")
    }
    if (catalog.isMVWithSameQueryPresent(logicalPlan)) {
      throw new UnsupportedOperationException("MV with same query present")
    }
  }

  def updateColumnName(attr: Attribute): String = {
    val name =
      attr.name.replace("(", "_")
        .replace(")", "")
        .replace(" ", "_")
        .replace("=", "")
        .replace(",", "")
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


  /**
   * Check if we can do incremental load on the mv table. Some cases like aggregation functions
   * which are present inside other expressions like sum(a)+sum(b) cannot be incremental loaded.
   */
  private def isFullReload(logicalPlan: LogicalPlan): Boolean = {
    var isFullReload = false
    logicalPlan.transformAllExpressions {
      case a: Alias => a
      case agg: AggregateExpression =>
        // If average function present then go for full refresh
        var reload = agg.aggregateFunction match {
          case avg: Average => true
          case _ => false
        }
        isFullReload = reload || isFullReload
        agg
      case c: Cast =>
        isFullReload = c.child.find {
          case agg: AggregateExpression => false
          case _ => false
        }.isDefined || isFullReload
        c
      case exp: Expression =>
        // Check any aggregation function present inside other expression.
        isFullReload = exp.find {
          case agg: AggregateExpression => true
          case _ => false
        }.isDefined || isFullReload
        exp
    }
    isFullReload
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

    // Basically we want to use it as simple linked list so hashcode is hardcoded.
    override def hashCode: Int = 1

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
              CarbonToSparkAdapter.createAttributeReference(attr.name,
                attr.dataType,
                attr.nullable,
                attr.metadata,
                attr.exprId,
                aliasName,
                attr)
          }.head
        case Alias(child, name) =>
          child
        case other => other
      }
    }

    expressions.map {
        case alias@Alias(agg: AggregateExpression, name) =>
          attrMap.get(AttributeKey(agg)).map { exp =>
            CarbonToSparkAdapter.createAliasRef(
              getAttribute(exp),
              name,
              alias.exprId,
              alias.qualifier,
              alias.explicitMetadata,
              Some(alias))
          }.getOrElse(alias)

        case attr: AttributeReference =>
          val uattr = attrMap.get(AttributeKey(attr)).map{a =>
            if (keepAlias) {
              CarbonToSparkAdapter.createAttributeReference(a.name,
                a.dataType,
                a.nullable,
                a.metadata,
                a.exprId,
                attr.qualifier,
                a)
            } else {
              a
            }
          }.getOrElse(attr)
          uattr
        case alias@Alias(expression: Expression, name) =>
          attrMap.get(AttributeKey(expression)).map { exp =>
            CarbonToSparkAdapter
              .createAliasRef(getAttribute(exp), name, alias.exprId, alias.qualifier,
                alias.explicitMetadata, Some(alias))
          }.getOrElse(alias)
        case expression: Expression =>
          val uattr = attrMap.get(AttributeKey(expression))
          uattr.getOrElse(expression)
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
              CarbonToSparkAdapter
                .createAttributeReference(a.name,
                  a.dataType,
                  a.nullable,
                  a.metadata,
                  a.exprId,
                  attr.qualifier,
                  a)
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
        val relation =
          s.dataMapTableRelation.get.asInstanceOf[MVPlanWrapper].plan.asInstanceOf[Select]
        val mappings = s.outputList zip relation.outputList
        val oList = for ((o1, o2) <- mappings) yield {
          if (o1.name != o2.name) Alias(o2, o1.name)(exprId = o1.exprId) else o2
        }
        relation.copy(outputList = oList).setRewritten()
      case g: GroupBy if g.dataMapTableRelation.isDefined =>
        val relation =
          g.dataMapTableRelation.get.asInstanceOf[MVPlanWrapper].plan.asInstanceOf[Select]
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
          case Seq(g: GroupBy) if g.dataMapTableRelation.isDefined =>
            val relation =
              g.dataMapTableRelation.get.asInstanceOf[MVPlanWrapper].plan.asInstanceOf[Select]
            val aliasMap = getAttributeMap(relation.outputList, g.outputList)
            // Update the flagspec as per the mv table attributes.
            val updatedFlagSpec: Seq[Seq[Any]] = updateFlagSpec(
              keepAlias = false,
              select,
              relation,
              aliasMap)
            if (isFullRefresh(g.dataMapTableRelation.get.asInstanceOf[MVPlanWrapper])) {
              val mappings = g.outputList zip relation.outputList
              val oList = for ((o1, o2) <- mappings) yield {
                if (o1.name != o2.name) Alias(o2, o1.name)(exprId = o1.exprId) else o2
              }

              val outList = select.outputList.map{ f =>
                oList.find(_.name.equals(f.name)).get
              }
              // Directly keep the relation as child.
              select.copy(
                outputList = outList,
                children = Seq(relation),
                aliasMap = relation.aliasMap,
                flagSpec = updatedFlagSpec).setRewritten()
            } else {
              // First find the indices from the child outlist.
              val indices = select.outputList.map{c =>
                g.outputList.indexWhere{
                  case al : Alias if c.isInstanceOf[Alias] =>
                    al.child.semanticEquals(c.asInstanceOf[Alias].child)
                  case al: Alias if al.child.semanticEquals(c) => true
                  case other if c.isInstanceOf[Alias] =>
                    other.semanticEquals(c.asInstanceOf[Alias].child)
                  case other =>
                    other.semanticEquals(c) || other.toAttribute.semanticEquals(c)
                }
              }
              val child = updateDataMap(g, rewrite).asInstanceOf[Matchable]
              // Get the outList from converted child outList using already selected indices
              val outputSel =
                indices.map(child.outputList(_)).zip(select.outputList).map { case (l, r) =>
                  l match {
                    case a: Alias if r.isInstanceOf[Alias] =>
                      Alias(a.child, r.name)(exprId = r.exprId)
                    case a: Alias => a
                    case other if r.isInstanceOf[Alias] =>
                      Alias(other, r.name)(exprId = r.exprId)
                    case other => other
                  }
              }
              // TODO Remove the unnecessary columns from selection.
              // Only keep columns which are required by parent.
              val inputSel = child.outputList
              select.copy(
                outputList = outputSel,
                inputList = inputSel,
                flagSpec = updatedFlagSpec,
                children = Seq(child)).setRewritten()
            }

          case _ => select
        }

      case other => other
    }
  }

  /**
   * Updates the flagspec of given select plan with attributes of relation select plan
   */
  private def updateFlagSpec(keepAlias: Boolean,
      select: Select,
      relation: Select,
      aliasMap: Map[AttributeKey, NamedExpression]): Seq[Seq[Any]] = {
    val updatedFlagSpec = select.flagSpec.map { f =>
      f.map {
        case list: ArrayBuffer[_] =>
          list.map { case s: SortOrder =>
            val expressions =
              updateOutPutList(
                Seq(s.child.asInstanceOf[Attribute]),
                relation,
                aliasMap,
                keepAlias = false)
            SortOrder(expressions.head, s.direction, s.sameOrderExpressions)
          }
        // In case of limit it goes to other.
        case other => other
      }
    }
    updatedFlagSpec
  }

  /**
   * It checks whether full referesh for the table is required. It means we no need to apply
   * aggregation function or group by functions on the mv table.
   */
  private def isFullRefresh(mvPlanWrapper: MVPlanWrapper): Boolean = {
    val fullRefesh = mvPlanWrapper.dataMapSchema.getProperties.get("full_refresh")
    if (fullRefesh != null) {
      fullRefesh.toBoolean
    } else {
      false
    }
  }

  // Create the aliases using two plan outputs mappings.
  def createAliases(mappings: Seq[(NamedExpression, NamedExpression)]): Seq[NamedExpression] = {
    mappings.map{ case (o1, o2) =>
      o2 match {
        case al: Alias if o1.name == o2.name && o1.exprId != o2.exprId =>
          Alias(al.child, o1.name)(exprId = o1.exprId)
        case other =>
          if (o1.name != o2.name || o1.exprId != o2.exprId) {
            Alias(o2, o1.name)(exprId = o1.exprId)
          } else {
            o2
          }
      }
    }
  }

  /**
   * Rewrite the updated mv query with corresponding MV table.
   */
  def rewriteWithMVTable(rewrittenPlan: ModularPlan, rewrite: QueryRewrite): ModularPlan = {
    if (rewrittenPlan.find(_.rewritten).isDefined) {
      val updatedDataMapTablePlan = rewrittenPlan transform {
        case s: Select =>
          MVHelper.updateDataMap(s, rewrite)
        case g: GroupBy =>
          MVHelper.updateDataMap(g, rewrite)
      }
      // TODO Find a better way to set the rewritten flag, it may fail in some conditions.
      val mapping =
        rewrittenPlan.collect { case m: ModularPlan => m } zip
        updatedDataMapTablePlan.collect { case m: ModularPlan => m }
      mapping.foreach(f => if (f._1.rewritten) f._2.setRewritten())

      updatedDataMapTablePlan
    } else {
      rewrittenPlan
    }
  }
}

