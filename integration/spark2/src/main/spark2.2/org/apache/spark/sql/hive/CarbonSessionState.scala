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
package org.apache.spark.sql.hive


import scala.collection.generic.SeqFactory

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BoundReference, Exists, Expression, In, InterpretedPredicate, ListQuery, ScalarSubquery}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.ParserUtils.string
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.{AddTableColumnsContext, ChangeColumnContext, CreateHiveTableContext, CreateTableContext}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.schema.{CarbonAlterTableAddColumnCommand, CarbonAlterTableDataTypeChangeCommand}
import org.apache.spark.sql.execution.datasources.{FindDataSourceTable, LogicalRelation, PreWriteCheck, ResolveSQLOnFile, _}
import org.apache.spark.sql.execution.strategy.{CarbonLateDecodeStrategy, DDLStrategy, StreamingTableStrategy}
import org.apache.spark.sql.execution.{SparkOptimizer, SparkSqlAstBuilder}
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.{SQLConf, SessionState}
import org.apache.spark.sql.optimizer.{CarbonIUDRule, CarbonLateDecodeRule, CarbonUDFTransformRule}
import org.apache.spark.sql.parser.{CarbonHelperSqlAstBuilder, CarbonSpark2SqlParser, CarbonSparkSqlParser}
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.util.{CarbonProperties, ThreadLocalSessionInfo}
import org.apache.carbondata.spark.util.CarbonScalaUtil

/**
 * This class will have carbon catalog and refresh the relation from cache if the carbontable in
 * carbon catalog is not same as cached carbon relation's carbon table
 *
 * @param externalCatalog
 * @param globalTempViewManager
 * @param sparkSession
 * @param functionResourceLoader
 * @param functionRegistry
 * @param conf
 * @param hadoopConf
 */
class CarbonSessionCatalog(
    externalCatalog: HiveExternalCatalog,
    globalTempViewManager: GlobalTempViewManager,
    functionRegistry: FunctionRegistry,
    sparkSession: SparkSession,
    conf: SQLConf,
    hadoopConf: Configuration,
    parser: ParserInterface,
    functionResourceLoader: FunctionResourceLoader)
  extends HiveSessionCatalog(
    externalCatalog,
    globalTempViewManager,
    new HiveMetastoreCatalog(sparkSession),
    functionRegistry,
    conf,
    hadoopConf,
    parser,
    functionResourceLoader
  ) {

  lazy val carbonEnv = {
    val env = new CarbonEnv
    env.init(sparkSession)
    env
  }

  def getCarbonEnv() : CarbonEnv = {
    carbonEnv
  }

  // Initialize all listeners to the Operation bus.
  CarbonEnv.initListeners()




  override def lookupRelation(name: TableIdentifier): LogicalPlan = {
    val rtnRelation = super.lookupRelation(name)
    var toRefreshRelation = false
    rtnRelation match {
      case SubqueryAlias(_,
      LogicalRelation(_: CarbonDatasourceHadoopRelation, _, _)) =>
        toRefreshRelation = CarbonEnv.refreshRelationFromCache(name)(sparkSession)
      case LogicalRelation(_: CarbonDatasourceHadoopRelation, _, _) =>
        toRefreshRelation = CarbonEnv.refreshRelationFromCache(name)(sparkSession)
      case SubqueryAlias(_, relation) if
      relation.getClass.getName.equals("org.apache.spark.sql.catalyst.catalog.CatalogRelation") ||
      relation.getClass.getName.equals("org.apache.spark.sql.catalyst.catalog.HiveTableRelation") ||
      relation.getClass.getName.equals(
        "org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation") =>
        val catalogTable =
          CarbonReflectionUtils.getFieldOfCatalogTable(
            "tableMeta",
            relation).asInstanceOf[CatalogTable]
        toRefreshRelation =
          CarbonEnv.refreshRelationFromCache(catalogTable.identifier)(sparkSession)
      case _ =>
    }

    if (toRefreshRelation) {
      super.lookupRelation(name)
    } else {
      rtnRelation
    }
  }

  /**
   * returns hive client from HiveExternalCatalog
   *
   * @return
   */
  def getClient(): org.apache.spark.sql.hive.client.HiveClient = {
    sparkSession.asInstanceOf[CarbonSession].sharedState.externalCatalog
      .asInstanceOf[HiveExternalCatalog].client
  }

  override def createPartitions(
      tableName: TableIdentifier,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    try {
      val table = CarbonEnv.getCarbonTable(tableName)(sparkSession)
      // Get the properties from thread local
      val carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
      if (carbonSessionInfo != null) {
        val updatedParts = CarbonScalaUtil.updatePartitions(carbonSessionInfo, parts, table)
        super.createPartitions(tableName, updatedParts, ignoreIfExists)
      } else {
        super.createPartitions(tableName, parts, ignoreIfExists)
      }
    } catch {
      case e: Exception =>
        super.createPartitions(tableName, parts, ignoreIfExists)
    }
  }

  /**
   * This is alternate way of getting partition information. It first fetches all partitions from
   * hive and then apply filter instead of querying hive along with filters.
   * @param partitionFilters
   * @param sparkSession
   * @param identifier
   * @return
   */
  def getPartitionsAlternate(partitionFilters: Seq[Expression],
      sparkSession: SparkSession,
      identifier: TableIdentifier) = {
    val allPartitions = sparkSession.sessionState.catalog.listPartitions(identifier)
    ExternalCatalogUtils.prunePartitionsByFilter(
      sparkSession.sessionState.catalog.getTableMetadata(identifier),
      allPartitions,
      partitionFilters,
      sparkSession.sessionState.conf.sessionLocalTimeZone)
  }
}


class CarbonAnalyzer(catalog: SessionCatalog,
    conf: SQLConf,
    sparkSession: SparkSession,
    analyzer: Analyzer) extends Analyzer(catalog, conf) {
  override def execute(plan: LogicalPlan): LogicalPlan = {
    var logicalPlan = analyzer.execute(plan)
    logicalPlan = CarbonPreAggregateDataLoadingRules(sparkSession).apply(logicalPlan)
    CarbonPreAggregateQueryRules(sparkSession).apply(logicalPlan)
  }
}


/**
 * Session state implementation to override sql parser and adding strategies
 *
 * @param sparkSession
 */
class CarbonSessionStateBuilder(sparkSession: SparkSession,
    parentState: Option[SessionState] = None)
  extends HiveSessionStateBuilder(sparkSession, parentState) {

  override lazy val sqlParser: ParserInterface = new CarbonSparkSqlParser(conf, sparkSession)

  experimentalMethods.extraStrategies =
    Seq(new StreamingTableStrategy(sparkSession),
        new CarbonLateDecodeStrategy,
        new DDLStrategy(sparkSession)
    )
  experimentalMethods.extraOptimizations = Seq(new CarbonIUDRule,
    new CarbonUDFTransformRule,
    new CarbonLateDecodeRule)

  /**
   * Internal catalog for managing table and database states.
   */
  /**
   * Create a [[CarbonSessionCatalogBuild]].
   */
  override protected lazy val catalog: CarbonSessionCatalog = {
    val catalog = new CarbonSessionCatalog(
      externalCatalog,
      session.sharedState.globalTempViewManager,
      functionRegistry,
      sparkSession,
      conf,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      sqlParser,
      resourceLoader)
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }

  private def externalCatalog: HiveExternalCatalog =
    session.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog]

  /**
   * Create a Hive aware resource loader.
   */
  override protected lazy val resourceLoader: HiveSessionResourceLoader = {
    val client: HiveClient = externalCatalog.client.newSession()
    new HiveSessionResourceLoader(session, client)
  }

  override lazy val optimizer: Optimizer = new CarbonOptimizer(catalog, conf, experimentalMethods)

  override protected def analyzer: Analyzer = new CarbonAnalyzer(catalog, conf, sparkSession,
    new Analyzer(catalog, conf) {

      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
        new ResolveHiveSerdeTable(session) +:
        new FindDataSourceTable(session) +:
        new ResolveSQLOnFile(session) +:
        new CarbonIUDAnalysisRule(sparkSession) +:
        new CarbonPreInsertionCasts(sparkSession) +: customResolutionRules

      override val extendedCheckRules: Seq[LogicalPlan => Unit] =
      PreWriteCheck :: HiveOnlyCheck :: Nil

      override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
        new DetermineTableStats(session) +:
        RelationConversions(conf, catalog) +:
        PreprocessTableCreation(session) +:
        PreprocessTableInsertion(conf) +:
        DataSourceAnalysis(conf) +:
        HiveAnalysis +:
        customPostHocResolutionRules
    }
  )

  override protected def newBuilder: NewBuilder = new CarbonSessionStateBuilder(_, _)

}


class CarbonOptimizer(
    catalog: SessionCatalog,
    conf: SQLConf,
    experimentalMethods: ExperimentalMethods)
  extends SparkOptimizer(catalog, conf, experimentalMethods) {

  override def execute(plan: LogicalPlan): LogicalPlan = {
    // In case scalar subquery add flag in relation to skip the decoder plan in optimizer rule, And
    // optimize whole plan at once.
    val transFormedPlan = plan.transform {
      case filter: Filter =>
        filter.transformExpressions {
          case s: ScalarSubquery =>
            val tPlan = s.plan.transform {
              case lr: LogicalRelation
                if lr.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
                lr.relation.asInstanceOf[CarbonDatasourceHadoopRelation].isSubquery += true
                lr
            }
            ScalarSubquery(tPlan, s.children, s.exprId)
          case e: Exists =>
            val tPlan = e.plan.transform {
              case lr: LogicalRelation
                if lr.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
                lr.relation.asInstanceOf[CarbonDatasourceHadoopRelation].isSubquery += true
                lr
            }
            Exists(tPlan, e.children.map(_.canonicalized), e.exprId)

          case In(value, Seq(l@ListQuery(sub, _, exprId))) =>
            val tPlan = sub.transform {
              case lr: LogicalRelation
                if lr.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
                lr.relation.asInstanceOf[CarbonDatasourceHadoopRelation].isSubquery += true
                lr
            }
            In(value, Seq(ListQuery(tPlan, l.children , exprId)))
        }
    }
    super.execute(transFormedPlan)
  }
}

class CarbonSqlAstBuilder(conf: SQLConf, parser: CarbonSpark2SqlParser, sparkSession: SparkSession)
  extends SparkSqlAstBuilder(conf) {

  val helper = new CarbonHelperSqlAstBuilder(conf, parser, sparkSession)

  override def visitCreateHiveTable(ctx: CreateHiveTableContext): LogicalPlan = {
    val fileStorage = helper.getFileStorage(ctx.createFileFormat)

    if (fileStorage.equalsIgnoreCase("'carbondata'") ||
        fileStorage.equalsIgnoreCase("'org.apache.carbondata.format'")) {
      helper.createCarbonTable(
        tableHeader = ctx.createTableHeader,
        skewSpecContext = ctx.skewSpec,
        bucketSpecContext = ctx.bucketSpec,
        partitionColumns = ctx.partitionColumns,
        columns = ctx.columns,
        tablePropertyList = ctx.tablePropertyList,
        locationSpecContext = ctx.locationSpec(),
        tableComment = Option(ctx.STRING()).map(string),
        ctas = ctx.AS,
        query = ctx.query)
    } else {
      super.visitCreateHiveTable(ctx)
    }
  }

  override def visitChangeColumn(ctx: ChangeColumnContext): LogicalPlan = {

    val newColumn = visitColType(ctx.colType)
    if (!ctx.identifier.getText.equalsIgnoreCase(newColumn.name)) {
      throw new MalformedCarbonCommandException(
        "Column names provided are different. Both the column names should be same")
    }

    val (typeString, values) : (String, Option[List[(Int, Int)]]) = newColumn.dataType match {
      case d:DecimalType => ("decimal", Some(List((d.precision, d.scale))))
      case _ => (newColumn.dataType.typeName.toLowerCase, None)
    }

    val alterTableChangeDataTypeModel =
      AlterTableDataTypeChangeModel(new CarbonSpark2SqlParser().parseDataType(typeString, values),
        new CarbonSpark2SqlParser()
          .convertDbNameToLowerCase(Option(ctx.tableIdentifier().db).map(_.getText)),
        ctx.tableIdentifier().table.getText.toLowerCase,
        ctx.identifier.getText.toLowerCase,
        newColumn.name.toLowerCase)

    CarbonAlterTableDataTypeChangeCommand(alterTableChangeDataTypeModel)
  }


  override def visitAddTableColumns(ctx: AddTableColumnsContext): LogicalPlan = {

    val cols = Option(ctx.columns).toSeq.flatMap(visitColTypeList)
    val fields = parser.getFields(cols)
    val tblProperties = scala.collection.mutable.Map.empty[String, String]
    val tableModel = new CarbonSpark2SqlParser().prepareTableModel (false,
      new CarbonSpark2SqlParser().convertDbNameToLowerCase(Option(ctx.tableIdentifier().db)
        .map(_.getText)),
      ctx.tableIdentifier.table.getText.toLowerCase,
      fields,
      Seq.empty,
      tblProperties,
      None,
      true)

    val alterTableAddColumnsModel = AlterTableAddColumnsModel(
      Option(ctx.tableIdentifier().db).map(_.getText),
      ctx.tableIdentifier.table.getText,
      tblProperties.toMap,
      tableModel.dimCols,
      tableModel.msrCols,
      tableModel.highcardinalitydims.getOrElse(Seq.empty))

    CarbonAlterTableAddColumnCommand(alterTableAddColumnsModel)
  }

  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = {
    super.visitCreateTable(ctx)
  }
}
