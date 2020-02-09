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
import org.apache.spark.sql.catalyst.{CarbonParserUtil, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, Coalesce, Expression, Literal, NamedExpression, ScalaUDF, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, Limit, LogicalPlan, Project}
import org.apache.spark.sql.execution.command.{Field, PartitionerField, TableModel, TableNewProcessor}
import org.apache.spark.sql.execution.command.table.{CarbonCreateTableCommand, CarbonDropTableCommand}
import org.apache.spark.sql.execution.command.timeseries.{TimeSeriesFunction, TimeSeriesUtil}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.parser.CarbonSparkSqlParserUtil
import org.apache.spark.sql.types.{ArrayType, DateType, MapType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{DataMapUtil, PartitionUtils}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema, RelationIdentifier}
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.datamap.DataMapManager
import org.apache.carbondata.mv.plans.modular._
import org.apache.carbondata.mv.plans.util.SQLBuilder
import org.apache.carbondata.mv.rewrite.{MVPlanWrapper, QueryRewrite, SummaryDatasetCatalog}
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Utility for MV datamap operations.
 */
object MVHelper {

  def createMVDataMap(sparkSession: SparkSession,
      dataMapSchema: DataMapSchema,
      queryString: String,
      ifNotExistsSet: Boolean = false,
      mainTable: CarbonTable): Unit = {
    val dmProperties = dataMapSchema.getProperties.asScala
    if (dmProperties.contains("streaming") && dmProperties("streaming").equalsIgnoreCase("true")) {
      throw new MalformedCarbonCommandException(
        s"MV datamap does not support streaming"
      )
    }
    val mvUtil = new MVUtil
    mvUtil.validateDMProperty(dmProperties)
    val logicalPlan = MVHelper.dropDummFuc(
      CarbonSparkSqlParserUtil.getMVPlan(queryString, sparkSession))
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
    val modularPlan = validateMVQuery(sparkSession, logicalPlan)
    val updatedQueryWithDb = modularPlan.asCompactSQL
    val (timeSeriesColumn, granularity): (String, String) = validateMVTimeSeriesQuery(
      logicalPlan,
      dataMapSchema)
    val fullRebuild = isFullReload(logicalPlan)
    var counter = 0
    // the ctas query can have duplicate columns, so we should take distinct and create fields,
    // so that it won't fail during create mv table
    val fields = logicalPlan.output.map { attr =>
      if (attr.dataType.isInstanceOf[ArrayType] || attr.dataType.isInstanceOf[StructType] ||
          attr.dataType.isInstanceOf[MapType]) {
        throw new UnsupportedOperationException(
          s"MV datamap is not supported for complex datatype columns and complex datatype return " +
          s"types of function :" + attr.name)
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
      if (mainCarbonTable.get.isChildTableForMV) {
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

    // Check if load is in progress in any of the parent table mapped to the datamap
    parentTablesList.asScala.foreach {
      parentTable =>
        if (SegmentStatusManager.isLoadInProgressInTable(parentTable)) {
          throw new UnsupportedOperationException(
            "Cannot create mv datamap table when insert is in progress on parent table: " +
            parentTable.getTableName)
        }
    }

    tableProperties.put(CarbonCommonConstants.DATAMAP_NAME, dataMapSchema.getDataMapName)
    tableProperties.put(CarbonCommonConstants.PARENT_TABLES, parentTables.asScala.mkString(","))

    val finalModularPlan = new SQLBuilder(modularPlan).SQLizer.execute(modularPlan)
    val fieldRelationMap = mvUtil.getFieldsAndDataMapFieldsFromPlan(finalModularPlan,
      getLogicalRelation(logicalPlan))
    // If dataMap is mapped to single main table, then inherit table properties from main table,
    // else, will use default table properties. If DMProperties contains table properties, then
    // table properties of datamap table will be updated
    if (parentTablesList.size() == 1) {
      DataMapUtil
        .inheritTablePropertiesFromMainTable(parentTablesList.get(0),
          fields,
          fieldRelationMap,
          tableProperties)
      if (granularity != null) {
        if (null != mainTable) {
          if (!mainTable.getTableName.equalsIgnoreCase(parentTablesList.get(0).getTableName)) {
            throw new MalformedCarbonCommandException(
              "Parent table name is different in Create and Select Statement")
          }
        }
        val timeSeriesDataType = parentTablesList
          .get(0)
          .getTableInfo
          .getFactTable
          .getListOfColumns
          .asScala
          .filter(columnSchema => columnSchema.getColumnName
            .equalsIgnoreCase(timeSeriesColumn))
          .head
          .getDataType
        if (timeSeriesDataType.equals(DataTypes.DATE) ||
            timeSeriesDataType.equals(DataTypes.TIMESTAMP)) {
          // if data type is of Date type, then check if given granularity is valid for date type
          if (timeSeriesDataType.equals(DataTypes.DATE)) {
            TimeSeriesUtil.validateTimeSeriesGranularityForDate(granularity)
          }
        } else {
          throw new MalformedCarbonCommandException(
            "TimeSeries Column must be of TimeStamp or Date type")
        }
      }
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
    val tableModel: TableModel = CarbonParserUtil.prepareTableModel(
      ifNotExistPresent = ifNotExistsSet,
      CarbonParserUtil.convertDbNameToLowerCase(tableIdentifier.database),
      tableIdentifier.table.toLowerCase,
      fields,
      partitionerFields,
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
                columns.add(mainTable.parentColumnName.toLowerCase())
                mainTableToColumnsMap.put(mainTable.parentTableName, columns)
              } else {
                mainTableToColumnsMap.get(mainTable.parentTableName)
                  .add(mainTable.parentColumnName.toLowerCase())
              }
          }
      }
    }
    dataMapSchema.setMainTableColumnList(mainTableToColumnsMap)
    dataMapSchema.setColumnsOrderMap(columnOrderMap)
    if (null != granularity && null != timeSeriesColumn) {
      dataMapSchema.setCtasQuery(queryString)
      dataMapSchema.setTimeSeries(true)
    } else {
      dataMapSchema.setCtasQuery(updatedQueryWithDb)
    }
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

  private def validateMVQuery(sparkSession: SparkSession,
      logicalPlan: LogicalPlan): ModularPlan = {
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
      throw new UnsupportedOperationException(
        "Group by columns must be present in project columns")
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
    modularPlan
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
    val value = attr.qualifier.map(qualifier => qualifier + "_" + name)
    if (value.nonEmpty) value.head else name
  }

  def getTables(logicalPlan: LogicalPlan): Seq[CatalogTable] = {
    logicalPlan.collect {
      case l: LogicalRelation => l.catalogTable.get
    }
  }

  def getLogicalRelation(logicalPlan: LogicalPlan): Seq[LogicalRelation] = {
    logicalPlan.collect {
      case l: LogicalRelation => l
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
      case al@Alias(udf: ScalaUDF, name) if name.equalsIgnoreCase(CarbonEnv.MV_SKIP_RULE_UDF) =>
        None
      case attr: AttributeReference if attr.name.equalsIgnoreCase(CarbonEnv.MV_SKIP_RULE_UDF) =>
        None
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
    CarbonToSparkAdapter.createAliasRef(ref, name, exprId = ref.exprId)
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
              CarbonToSparkAdapter.createAttributeReference(
                name = a.name,
                dataType = a.dataType,
                nullable = a.nullable,
                metadata = a.metadata,
                exprId = a.exprId,
                qualifier = attr.qualifier)
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
          CarbonToSparkAdapter.createAliasRef(attr, r.name, r.exprId)
        case a@Alias(attr: AttributeReference, name) =>
          CarbonToSparkAdapter.createAliasRef(attr, r.name, r.exprId)
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
                .createAttributeReference(
                  a.name,
                  a.dataType,
                  a.nullable,
                  a.metadata,
                  a.exprId,
                  attr.qualifier)
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
            case Alias(agg@AggregateExpression(fun@Corr(l, r), _, _, _), name) =>
              val uFun = Sum(right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@VariancePop(child), _, _, _), name) =>
              val uFun = Sum(right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@VarianceSamp(child), _, _, _), name) =>
              val uFun = Sum(right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@StddevSamp(child), _, _, _), name) =>
              val uFun = Sum(right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@StddevPop(child), _, _, _), name) =>
              val uFun = Sum(right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@CovPopulation(l, r), _, _, _), name) =>
              val uFun = Sum(right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@CovSample(l, r), _, _, _), name) =>
              val uFun = Sum(right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@Skewness(child), _, _, _), name) =>
              val uFun = Sum(right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@Kurtosis(child), _, _, _), name) =>
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
      var updatedMVTablePlan = rewrittenPlan transform {
        case s: Select =>
          MVHelper.updateDataMap(s, rewrite)
        case g: GroupBy =>
          MVHelper.updateDataMap(g, rewrite)
      }
      if (rewrittenPlan.isRolledUp) {
        // If the rewritten query is rolled up, then rewrite the query based on the original modular
        // plan. Make a new outputList based on original modular plan and wrap rewritten plan with
        // select & group-by nodes with new outputList.

        // For example:
        // Given User query:
        // SELECT timeseries(col,'day') from maintable group by timeseries(col,'day')
        // If plan is rewritten as per 'hour' granularity of datamap1,
        // then rewritten query will be like,
        // SELECT datamap1_table.`UDF:timeseries_projectjoindate_hour` AS `UDF:timeseries
        // (projectjoindate, hour)`
        // FROM
        // default.datamap1_table
        // GROUP BY datamap1_table.`UDF:timeseries_projectjoindate_hour`
        //
        // Now, rewrite the rewritten plan as per the 'day' granularity
        // SELECT timeseries(gen_subsumer_0.`UDF:timeseries(projectjoindate, hour)`,'day' ) AS
        // `UDF:timeseries(projectjoindate, day)`
        //  FROM
        //  (SELECT datamap2_table.`UDF:timeseries_projectjoindate_hour` AS `UDF:timeseries
        //  (projectjoindate, hour)`
        //  FROM
        //    default.datamap2_table
        //  GROUP BY datamap2_table.`UDF:timeseries_projectjoindate_hour`) gen_subsumer_0
        // GROUP BY timeseries(gen_subsumer_0.`UDF:timeseries(projectjoindate, hour)`,'day' )
        rewrite.modularPlan match {
          case select: Select =>
            val outputList = select.outputList
            val rolledUpOutputList = updatedMVTablePlan.asInstanceOf[Select].outputList
            var finalOutputList: Seq[NamedExpression] = Seq.empty
            val mapping = outputList zip rolledUpOutputList
            val newSubsume = rewrite.newSubsumerName()

            mapping.foreach { outputLists =>
              val name: String = getAliasName(outputLists._2)
              outputLists._1 match {
                case a@Alias(scalaUdf: ScalaUDF, aliasName) =>
                  if (scalaUdf.function.isInstanceOf[TimeSeriesFunction]) {
                    val newName = newSubsume + ".`" + name + "`"
                    val transformedUdf = transformTimeSeriesUdf(scalaUdf, newName)
                    finalOutputList = finalOutputList.:+(Alias(transformedUdf, aliasName)(a.exprId,
                      a.qualifier).asInstanceOf[NamedExpression])
                  }
                case Alias(attr: AttributeReference, _) =>
                  finalOutputList = finalOutputList.:+(
                    CarbonToSparkAdapter.createAttributeReference(attr, name, newSubsume))
                case attr: AttributeReference =>
                  finalOutputList = finalOutputList.:+(
                    CarbonToSparkAdapter.createAttributeReference(attr, name, newSubsume))
              }
            }
            val newChildren = new collection.mutable.ArrayBuffer[ModularPlan]()
            val newAliasMap = new collection.mutable.HashMap[Int, String]()

            val sel_plan = select.copy(outputList = finalOutputList,
              inputList = finalOutputList,
              predicateList = Seq.empty)
            newChildren += sel_plan
            newAliasMap += (newChildren.indexOf(sel_plan) -> newSubsume)
            updatedMVTablePlan = select.copy(outputList = finalOutputList,
              inputList = finalOutputList,
              aliasMap = newAliasMap.toMap,
              predicateList = Seq.empty,
              children = Seq(updatedMVTablePlan)).setRewritten()

          case groupBy: GroupBy =>
            updatedMVTablePlan match {
              case select: Select =>
                val selectOutputList = groupBy.outputList
                val rolledUpOutputList = updatedMVTablePlan.asInstanceOf[Select].outputList
                var finalOutputList: Seq[NamedExpression] = Seq.empty
                var predicateList: Seq[Expression] = Seq.empty
                val mapping = selectOutputList zip rolledUpOutputList
                val newSubsume = rewrite.newSubsumerName()

                mapping.foreach { outputLists =>
                  val aliasName: String = getAliasName(outputLists._2)
                  outputLists._1 match {
                    case a@Alias(scalaUdf: ScalaUDF, _) =>
                      if (scalaUdf.function.isInstanceOf[TimeSeriesFunction]) {
                        val newName = newSubsume + ".`" + aliasName + "`"
                        val transformedUdf = transformTimeSeriesUdf(scalaUdf, newName)
                        groupBy.predicateList.foreach {
                          case udf: ScalaUDF if udf.isInstanceOf[ScalaUDF] =>
                            predicateList = predicateList.:+(transformedUdf)
                          case attr: AttributeReference =>
                            predicateList = predicateList.:+(
                              CarbonToSparkAdapter.createAttributeReference(attr,
                                attr.name,
                                newSubsume))
                        }
                        finalOutputList = finalOutputList.:+(Alias(transformedUdf, a.name)(a
                          .exprId, a.qualifier).asInstanceOf[NamedExpression])
                      }
                    case attr: AttributeReference =>
                      finalOutputList = finalOutputList.:+(
                        CarbonToSparkAdapter.createAttributeReference(attr, aliasName, newSubsume))
                    case Alias(attr: AttributeReference, _) =>
                      finalOutputList = finalOutputList.:+(
                        CarbonToSparkAdapter.createAttributeReference(attr, aliasName, newSubsume))
                    case a@Alias(agg: AggregateExpression, name) =>
                      val newAgg = agg.transform {
                        case attr: AttributeReference =>
                          CarbonToSparkAdapter.createAttributeReference(attr, name, newSubsume)
                      }
                      finalOutputList = finalOutputList.:+(Alias(newAgg, name)(a.exprId,
                        a.qualifier).asInstanceOf[NamedExpression])
                    case other => other
                  }
                }
                val newChildren = new collection.mutable.ArrayBuffer[ModularPlan]()
                val newAliasMap = new collection.mutable.HashMap[Int, String]()

                val sel_plan = select.copy(outputList = finalOutputList,
                  inputList = finalOutputList,
                  predicateList = Seq.empty)
                newChildren += sel_plan
                newAliasMap += (newChildren.indexOf(sel_plan) -> newSubsume)
                updatedMVTablePlan = select.copy(outputList = finalOutputList,
                  inputList = finalOutputList,
                  aliasMap = newAliasMap.toMap,
                  children = Seq(updatedMVTablePlan)).setRewritten()
                updatedMVTablePlan = groupBy.copy(outputList = finalOutputList,
                  inputList = finalOutputList,
                  predicateList = predicateList,
                  alias = Some(newAliasMap.mkString),
                  child = updatedMVTablePlan).setRewritten()
                updatedMVTablePlan = select.copy(outputList = finalOutputList,
                  inputList = finalOutputList,
                  children = Seq(updatedMVTablePlan)).setRewritten()
            }
        }
      }
      updatedMVTablePlan
    } else {
      rewrittenPlan
    }
  }

  def getAliasName(exp: NamedExpression): String = {
    exp match {
      case Alias(_, name) =>
        name
      case attr: AttributeReference =>
        attr.name
    }
  }

  private def transformTimeSeriesUdf(scalaUdf: ScalaUDF, newName: String): Expression = {
    scalaUdf.transformDown {
      case attr: AttributeReference =>
        AttributeReference(newName, attr.dataType)(
          exprId = attr.exprId,
          qualifier = attr.qualifier)
      case l: Literal =>
        Literal(UTF8String.fromString("'" + l.toString() + "'"),
          org.apache.spark.sql.types.DataTypes.StringType)
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

  /**
   * Validate mv timeseries query for timeseries column and granularity.
   * TimeSeries udf function will contain data type as TimeStamp/cast as TimeStamp
   *
   * @param logicalPlan   to be validated
   * @param dataMapSchema to check if it is lazy/non-lazy datamap
   * @return
   */
  private def validateMVTimeSeriesQuery(logicalPlan: LogicalPlan,
      dataMapSchema: DataMapSchema): (String, String) = {
    var timeSeriesColumn: String = null
    var granularity: String = null
    logicalPlan.transformExpressions {
      case alias@Alias(udf: ScalaUDF, _) =>
        if (udf.function.isInstanceOf[TimeSeriesFunction]) {
          if (null == timeSeriesColumn && null == granularity) {
            udf.children.collect {
              case attr: AttributeReference =>
                timeSeriesColumn = attr.name
              case l: Literal =>
                granularity = l.value.toString
              case c: Cast =>
                c.child match {
                  case attribute: AttributeReference =>
                    if (attribute.dataType.isInstanceOf[DateType]) {
                      timeSeriesColumn = attribute.name
                    }
                  case _ =>
                }
            }
          } else {
            udf.children.collect {
              case attr: AttributeReference =>
                if (!attr.name.equalsIgnoreCase(timeSeriesColumn)) {
                  throw new MalformedCarbonCommandException(
                    "Multiple timeseries udf functions are defined in Select statement with " +
                    "different timestamp columns")
                }
              case l: Literal =>
                if (!granularity.equalsIgnoreCase(l.value.toString)) {
                  throw new MalformedCarbonCommandException(
                    "Multiple timeseries udf functions are defined in Select statement with " +
                    "different granularities")
                }
            }
          }
        }
        alias
    }
    // timeseries column and granularity is not null, then validate
    if (null != timeSeriesColumn && null != granularity) {
      if (dataMapSchema.isLazy) {
        throw new MalformedCarbonCommandException(
          "MV TimeSeries queries does not support Lazy Rebuild")
      }
      TimeSeriesUtil.validateTimeSeriesGranularity(granularity)
    } else if (null == timeSeriesColumn && null != granularity) {
      throw new MalformedCarbonCommandException(
        "MV TimeSeries is only supported on Timestamp/Date column")
    }
    (timeSeriesColumn, granularity)
  }
}

