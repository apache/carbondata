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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, Coalesce, Expression, NamedExpression, ScalaUDF, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, Limit, LogicalPlan, Project}
import org.apache.spark.sql.execution.command.{Field, PartitionerField, TableModel, TableNewProcessor}
import org.apache.spark.sql.execution.command.table.{CarbonCreateTableCommand, CarbonDropTableCommand}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}
import org.apache.spark.util.{DataMapUtil, PartitionUtils}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema, RelationIdentifier}
import org.apache.carbondata.datamap.DataMapManager
import org.apache.carbondata.mv.plans.modular._
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
    if (dmProperties.contains("streaming") && dmProperties("streaming").equalsIgnoreCase("true")) {
      throw new MalformedCarbonCommandException(
        s"MV datamap does not support streaming"
      )
    }
    val mvUtil = new MVUtil
    mvUtil.validateDMProperty(dmProperties)
    val updatedQuery = new CarbonSpark2SqlParser().addPreAggFunction(queryString)
    val query = sparkSession.sql(updatedQuery)
    val logicalPlan = MVHelper.dropDummFuc(query.queryExecution.analyzed)
    // if there is limit in MV ctas query string, throw exception, as its not a valid usecase
    logicalPlan match {
      case Limit(_, _) =>
        throw new MalformedCarbonCommandException("MV datamap does not support the query with " +
                                                  "limit")
      case _ =>
    }
    val selectTables = getTables(logicalPlan)
    if (selectTables.isEmpty) {
      throw new MalformedCarbonCommandException(
        s"Non-Carbon table does not support creating MV datamap")
    }
    val updatedQueryWithDb = validateMVQuery(sparkSession, logicalPlan)
    val fullRebuild = isFullReload(logicalPlan)
    var counter = 0
    // the ctas query can have duplicate columns, so we should take distinct and create fields,
    // so that it won't fail during create mv table
    val fields = logicalPlan.output.map { attr =>
      if (attr.dataType.isInstanceOf[ArrayType] || attr.dataType.isInstanceOf[StructType] ||
          attr.dataType.isInstanceOf[MapType]) {
        throw new UnsupportedOperationException(
          s"MV datamap is unsupported for ComplexData type column: " + attr.name)
      }
      val name = updateColumnName(attr, counter)
      counter += 1
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
    }.distinct

    val tableProperties = mutable.Map[String, String]()
    val parentTables = new util.ArrayList[String]()
    val parentTablesList = new util.ArrayList[CarbonTable](selectTables.size)
    selectTables.foreach { selectTable =>
      val mainCarbonTable = try {
        Some(CarbonEnv.getCarbonTable(selectTable.identifier.database,
          selectTable.identifier.table)(sparkSession))
      } catch {
        // Exception handling if it's not a CarbonTable
        case ex: Exception =>
          throw new MalformedCarbonCommandException(
            s"Non-Carbon table does not support creating MV datamap")
      }
      if (!mainCarbonTable.get.getTableInfo.isTransactionalTable) {
        throw new MalformedCarbonCommandException("Unsupported operation on NonTransactional table")
      }
      if (mainCarbonTable.get.isChildTable || mainCarbonTable.get.isChildDataMap) {
        throw new MalformedCarbonCommandException(
          "Cannot create Datamap on child table " + mainCarbonTable.get.getTableUniqueName)
      }
      parentTables.add(mainCarbonTable.get.getTableName)
      if (!mainCarbonTable.isEmpty && mainCarbonTable.get.isStreamingSink) {
        throw new MalformedCarbonCommandException(
          s"Streaming table does not support creating MV datamap")
      }
      parentTablesList.add(mainCarbonTable.get)
    }
    tableProperties.put(CarbonCommonConstants.DATAMAP_NAME, dataMapSchema.getDataMapName)
    tableProperties.put(CarbonCommonConstants.PARENT_TABLES, parentTables.asScala.mkString(","))

    val fieldRelationMap = mvUtil.getFieldsAndDataMapFieldsFromPlan(
      logicalPlan, queryString, sparkSession)
    // If dataMap is mapped to single main table, then inherit table properties from main table,
    // else, will use default table properties. If DMProperties contains table properties, then
    // table properties of datamap table will be updated
    if (parentTablesList.size() == 1) {
      DataMapUtil
        .inheritTablePropertiesFromMainTable(parentTablesList.get(0),
          fields,
          fieldRelationMap,
          tableProperties)
    }
    dmProperties.foreach(t => tableProperties.put(t._1, t._2))
    val usePartitioning = dmProperties.getOrElse("partitioning", "true").toBoolean
    var partitionerFields: Seq[PartitionerField] = Seq.empty
    // Inherit partition from parent table if datamap is mapped to single parent table
    if (parentTablesList.size() == 1) {
      val partitionInfo = parentTablesList.get(0).getPartitionInfo
      val parentPartitionColumns = if (!usePartitioning) {
        Seq.empty
      } else if (parentTablesList.get(0).isHivePartitionTable) {
        partitionInfo.getColumnSchemaList.asScala.map(_.getColumnName)
      } else {
        Seq()
      }
      partitionerFields = PartitionUtils
        .getPartitionerFields(parentPartitionColumns, fieldRelationMap)
    }

    var order = 0
    val columnOrderMap = new java.util.HashMap[Integer, String]()
    if (partitionerFields.nonEmpty) {
      fields.foreach { field =>
        columnOrderMap.put(order, field.column)
        order += 1
      }
    }

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
      partitionerFields,
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

    // Map list of main table columns mapped to datamap table and add to dataMapSchema
    val mainTableToColumnsMap = new java.util.HashMap[String, util.Set[String]]()
    val mainTableFieldIterator = fieldRelationMap.values.asJava.iterator()
    while (mainTableFieldIterator.hasNext) {
      val value = mainTableFieldIterator.next()
      value.columnTableRelationList.foreach {
        columnTableRelation =>
          columnTableRelation.foreach {
            mainTable =>
              if (null == mainTableToColumnsMap.get(mainTable.parentTableName)) {
                val columns = new util.HashSet[String]()
                columns.add(mainTable.parentColumnName)
                mainTableToColumnsMap.put(mainTable.parentTableName, columns)
              } else {
                mainTableToColumnsMap.get(mainTable.parentTableName)
                  .add(mainTable.parentColumnName)
              }
          }
      }
    }
    dataMapSchema.setMainTableColumnList(mainTableToColumnsMap)
    dataMapSchema.setColumnsOrderMap(columnOrderMap)
    dataMapSchema.setCtasQuery(updatedQueryWithDb)
    dataMapSchema
      .setRelationIdentifier(new RelationIdentifier(tableIdentifier.database.get,
        tableIdentifier.table,
        CarbonEnv.getCarbonTable(tableIdentifier)(sparkSession).getTableId))

    val parentIdents = selectTables.map { table =>
      val relationIdentifier = new RelationIdentifier(table.database, table.identifier.table, "")
      relationIdentifier.setTablePath(FileFactory.getUpdatedFilePath(table.location.toString))
      relationIdentifier
    }
    dataMapSchema.getRelationIdentifier.setTablePath(tablePath)
    dataMapSchema.setParentTables(new util.ArrayList[RelationIdentifier](parentIdents.asJava))
    dataMapSchema.getProperties.put("full_refresh", fullRebuild.toString)
    try {
      DataMapStoreManager.getInstance().saveDataMapSchema(dataMapSchema)
    } catch {
      case ex: Exception =>
        val dropTableCommand = CarbonDropTableCommand(true,
          new Some[String](dataMapSchema.getRelationIdentifier.getDatabaseName),
          dataMapSchema.getRelationIdentifier.getTableName,
          true)
        dropTableCommand.run(sparkSession)
        throw ex
    }
  }

  private def isValidSelect(isValidExp: Boolean,
      s: Select): Boolean = {
    // Make sure all predicates are present in projections.
    var predicateList: Seq[AttributeReference] = Seq.empty
    s.predicateList.map { f =>
      f.children.collect {
        case p: AttributeReference =>
          predicateList = predicateList.+:(p)
      }
    }
    if (predicateList.nonEmpty) {
      predicateList.forall { p =>
        s.outputList.exists {
          case a: Alias =>
            a.semanticEquals(p) || a.child.semanticEquals(p)
          case other => other.semanticEquals(p)
        }
      }
    } else {
      isValidExp
    }
  }

  private def validateMVQuery(sparkSession: SparkSession,
      logicalPlan: LogicalPlan): String = {
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
        val isValidExp = g.predicateList.forall{p =>
          g.outputList.exists{
            case a: Alias =>
              a.semanticEquals(p) || a.child.semanticEquals(p)
            case other => other.semanticEquals(p)
          }
        }
        g.child match {
          case s: Select =>
            isValidSelect(isValidExp, s)
          case m: ModularRelation => isValidExp
        }
      case s: Select =>
        isValidSelect(true, s)
      case _ => true
    }
    if (!isValid) {
      throw new UnsupportedOperationException(
        "Group by/Filter columns must be present in project columns")
    }
    if (catalog.isMVWithSameQueryPresent(logicalPlan)) {
      throw new UnsupportedOperationException("MV with same query present")
    }

    var expressionValid = true
    modularPlan.transformExpressions {
      case coal@Coalesce(_) if coal.children.exists(
        exp => exp.isInstanceOf[AggregateExpression]) =>
        expressionValid = false
        coal
    }

    if (!expressionValid) {
      throw new UnsupportedOperationException("MV doesn't support Coalesce")
    }
    modularPlan.asCompactSQL
  }

  def getUpdatedName(name: String, counter: Int): String = {
    var updatedName = name.replace("(", "_")
      .replace(")", "")
      .replace(" ", "_")
      .replace("=", "")
      .replace(",", "")
      .replace(".", "_")
      .replace("`", "")
    if (updatedName.length >= CarbonCommonConstants.MAXIMUM_CHAR_LENGTH) {
      updatedName = updatedName.substring(0, 110) + CarbonCommonConstants.UNDERSCORE + counter
    }
    updatedName
  }

  def updateColumnName(attr: Attribute, counter: Int): String = {
    val name = getUpdatedName(attr.name, counter)
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
    // TODO:- Remove this case when incremental datalaoding is supported for multiple tables
    logicalPlan.transformDown {
      case join@Join(l1, l2, jointype, condition) =>
        isFullReload = true
        join
    }
    isFullReload
  }

  def getAttributeMap(subsumer: Seq[NamedExpression],
      subsume: Seq[NamedExpression]): Map[AttributeKey, NamedExpression] = {
    // when datamap is created with duplicate columns like select sum(age),sum(age) from table,
    // the subsumee will have duplicate, so handle that case here
    if (subsumer.length == subsume.groupBy(_.name).size) {
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
        val outputList = getUpdatedOutputList(relation.outputList, s.dataMapTableRelation)
        // when the output list contains multiple projection of same column, but relation
        // contains distinct columns, mapping may go wrong with columns, so select distinct
        val mappings = s.outputList.distinct zip outputList
        val oList = for ((o1, o2) <- mappings) yield {
          if (o1.name != o2.name) Alias(o2, o1.name)(exprId = o1.exprId) else o2
        }
        relation.copy(outputList = oList).setRewritten()
      case g: GroupBy if g.dataMapTableRelation.isDefined =>
        val relation =
          g.dataMapTableRelation.get.asInstanceOf[MVPlanWrapper].plan.asInstanceOf[Select]
        val in = relation.asInstanceOf[Select].outputList
        val outputList = getUpdatedOutputList(relation.outputList, g.dataMapTableRelation)
        val mappings = g.outputList zip outputList
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
              val outputList = getUpdatedOutputList(relation.outputList, g.dataMapTableRelation)
              val mappings = g.outputList zip outputList
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
            SortOrder(expressions.head, s.direction)
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
      updatedDataMapTablePlan
    } else {
      rewrittenPlan
    }
  }

  private def getUpdatedOutputList(outputList: Seq[NamedExpression],
      dataMapTableRelation: Option[ModularPlan]): Seq[NamedExpression] = {
    dataMapTableRelation.collect {
      case mv: MVPlanWrapper =>
        val dataMapSchema = mv.dataMapSchema
        val columnsOrderMap = dataMapSchema.getColumnsOrderMap
        if (null != columnsOrderMap && !columnsOrderMap.isEmpty) {
          val updatedOutputList = new util.ArrayList[NamedExpression]()
          var i = 0
          while (i < columnsOrderMap.size()) {
            updatedOutputList
              .add(outputList.filter(f => f.name.equalsIgnoreCase(columnsOrderMap.get(i))).head)
            i = i + 1
          }
          updatedOutputList.asScala
        } else {
          outputList
        }
      case _ => outputList
    }.get
  }
}

