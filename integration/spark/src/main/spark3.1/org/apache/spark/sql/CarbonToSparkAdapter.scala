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

import java.net.URI
import java.time.ZoneId

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.antlr.v4.runtime.tree.TerminalNode
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.carbondata.execution.datasources.CarbonFileIndexReplaceRule
import org.apache.spark.sql.catalyst.{CarbonParserUtil, InternalRow, QueryPlanningTracker, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, ExternalCatalogWithListener, SessionCatalog}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, AttributeSeq, AttributeSet, Expression, ExpressionSet, ExprId, NamedExpression, ScalaUDF, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide, Optimizer}
import org.apache.spark.sql.catalyst.parser.ParserUtils.operationNotAllowed
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.{BucketSpecContext, ColTypeListContext, CreateTableHeaderContext, LocationSpecContext, PartitionFieldListContext, QueryContext, SkewSpecContext, TablePropertyListContext}
import org.apache.spark.sql.catalyst.plans.{logical, JoinType}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, InsertIntoStatement, Join, JoinHint, LogicalPlan, OneRowRelation, QualifiedColType, Statistics, SubqueryAlias}
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.execution.{ExplainMode, QueryExecution, ShuffledRowRDD, SimpleMode, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.command.{ExplainCommand, Field, PartitionerField, RefreshTableCommand, TableModel, TableNewProcessor}
import org.apache.spark.sql.execution.command.table.{CarbonCreateTableAsSelectCommand, CarbonCreateTableCommand}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.SQLShuffleWriteMetricsReporter
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.internal.{SessionState, SharedState}
import org.apache.spark.sql.optimizer.{CarbonIUDRule, CarbonUDFTransformRule, MVRewriteRule}
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.parser.CarbonSparkSqlParserUtil.{checkIfDuplicateColumnExists, convertDbNameToLowerCase, validateStreamingProperty}
import org.apache.spark.sql.secondaryindex.optimizer.CarbonSITransformationRule
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{AbstractDataType, CharType, DataType, Metadata, StructField, VarcharType}
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.common.exceptions.DeprecatedFeatureException
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.SchemaReader
import org.apache.carbondata.core.util.{CarbonProperties, ThreadLocalSessionInfo}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.CarbonOption
import org.apache.carbondata.spark.util.CarbonScalaUtil

object CarbonToSparkAdapter {

  def addSparkSessionListener(sparkSession: SparkSession): Unit = {
    sparkSession.sparkContext.addSparkListener(new SparkListener {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        CarbonEnv.carbonEnvMap.remove(sparkSession)
        ThreadLocalSessionInfo.unsetAll()
      }
    })
  }

  def addSparkListener(sparkContext: SparkContext): Unit = {
    sparkContext.addSparkListener(new SparkListener {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        SparkSession.setDefaultSession(null)
      }
    })
  }

  def createAttributeReference(
      name: String,
      dataType: DataType,
      nullable: Boolean,
      metadata: Metadata,
      exprId: ExprId,
      qualifier: Option[String],
      attrRef : NamedExpression = null): AttributeReference = {
    val qf = if (qualifier.nonEmpty) Seq(qualifier.get) else Seq.empty
    AttributeReference(
      name,
      dataType,
      nullable,
      metadata)(exprId, qf)
  }

  def createAttributeReference(
      name: String,
      dataType: DataType,
      nullable: Boolean,
      metadata: Metadata,
      exprId: ExprId,
      qualifier: Seq[String]): AttributeReference = {
    AttributeReference(
      name,
      dataType,
      nullable,
      metadata)(exprId, qualifier)
  }

  def lowerCaseAttribute(expression: Expression): Expression = expression.transform {
    case attr: AttributeReference =>
      CarbonToSparkAdapter.createAttributeReference(
        attr.name.toLowerCase,
        attr.dataType,
        attr.nullable,
        attr.metadata,
        attr.exprId,
        attr.qualifier)
  }

  def createAttributeReference(attr: AttributeReference,
      attrName: String,
      newSubsume: String): AttributeReference = {
    AttributeReference(attrName, attr.dataType)(
      exprId = attr.exprId,
      qualifier = newSubsume.split("\n").map(_.trim))
  }

  def createScalaUDF(s: ScalaUDF, reference: AttributeReference): ScalaUDF = {
    s.copy(children = Seq(reference))
  }

  def createExprCode(code: String, isNull: String, value: String, dataType: DataType): ExprCode = {
    ExprCode(
      code"$code",
      JavaCode.isNullVariable(isNull),
      JavaCode.variable(value, dataType))
  }

  def createAliasRef(
      child: Expression,
      name: String,
      exprId: ExprId = NamedExpression.newExprId,
      qualifier: Seq[String] = Seq.empty,
      explicitMetadata: Option[Metadata] = None,
      namedExpr: Option[NamedExpression] = None) : Alias = {
    Alias(child, name)(exprId, qualifier, explicitMetadata)
  }

  def createAliasRef(
      child: Expression,
      name: String,
      exprId: ExprId,
      qualifier: Option[String]) : Alias = {
    Alias(child, name)(exprId,
      if (qualifier.isEmpty) Seq.empty else Seq(qualifier.get),
      None)
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

  def getTheLastQualifier(attribute: Attribute): String = {
    attribute.qualifier.reverse.head
  }

  def getExplainCommandObj(logicalPlan: LogicalPlan = OneRowRelation(),
      mode: Option[String]) : ExplainCommand = {
    ExplainCommand(logicalPlan, ExplainMode.fromString(mode.getOrElse(SimpleMode.name)))
  }

  def getExplainCommandObj(mode: Option[String]) : ExplainCommand = {
    ExplainCommand(OneRowRelation(), ExplainMode.fromString(mode.getOrElse(SimpleMode.name)))
  }

  /**
   * As a part of SPARK-24085 Hive tables supports scala subquery for
   * the partitioned tables,so Carbon also needs to supports
   * @param partitionSet
   * @param filterPredicates
   * @return
   */
  def getPartitionFilter(
      partitionSet: AttributeSet,
      filterPredicates: Seq[Expression]): Seq[Expression] = {
    filterPredicates
      .filterNot(SubqueryExpression.hasSubquery)
      .filter { filter =>
        filter.references.nonEmpty && filter.references.subsetOf(partitionSet)
      }
  }

  def getDataFilter(partitionSet: AttributeSet, filter: Seq[Expression]): Seq[Expression] = {
    filter
  }

  // As per SPARK-22520 OptimizeCodegen is removed in 2.3.1
  def getOptimizeCodegenRule(): Seq[Rule[LogicalPlan]] = {
    Seq.empty
  }

  def getUpdatedStorageFormat(storageFormat: CatalogStorageFormat,
      map: Map[String, String],
      tablePath: String): CatalogStorageFormat = {
    storageFormat.copy(properties = map, locationUri = Some(new URI(tablePath)))
  }

  def getOutput(subQueryAlias: SubqueryAlias): Seq[Attribute] = {
    val newAlias = Seq(subQueryAlias.identifier.name)
    subQueryAlias.child.output.map(_.withQualifier(newAlias))
  }

  def getHiveExternalCatalog(sparkSession: SparkSession): HiveExternalCatalog = {
    sparkSession.sessionState.catalog.externalCatalog
      .asInstanceOf[ExternalCatalogWithListener]
      .unwrapped
      .asInstanceOf[HiveExternalCatalog]
  }

  def createFilePartition(index: Int, files: ArrayBuffer[PartitionedFile]): FilePartition = {
    FilePartition(index, files.toArray)
  }

  def stringToTimestamp(timestamp: String): Option[Long] = {
    DateTimeUtils.stringToTimestamp(UTF8String.fromString(timestamp), ZoneId.systemDefault())
  }

  def stringToTime(value: String): java.util.Date = {
    DateTimeUtils.toJavaDate(DateTimeUtils.stringToDate(UTF8String.fromString(value),
      ZoneId.systemDefault()).get)
  }

  def timeStampToString(timeStamp: Long): String = {
    TimestampFormatter.getFractionFormatter(ZoneId.systemDefault()).format(timeStamp)
  }

  def dateToString(date: Int): String = {
    DateTimeUtils.daysToMicros(date, ZoneId.systemDefault()).toString
  }

  def getProcessingTime: String => Trigger = {
    Trigger.ProcessingTime
  }

  def addTaskCompletionListener[U](f: => U) {
    TaskContext.get().addTaskCompletionListener[Unit] { context =>
      f
    }
  }

  def getTableIdentifier(u: UnresolvedRelation): Some[TableIdentifier] = {
    val tableName = u.tableName.split("\\.")
    if (tableName.size == 2) {
      Some(TableIdentifier(tableName(1), Option(tableName(0))))
    } else {
      val currentDatabase = SparkSQLUtil.getSparkSession.sessionState.catalog.getCurrentDatabase
      Some(TableIdentifier(tableName(0), Option(currentDatabase)))
    }
  }

  // TODO: check if this is correct
  def getTableIdentifier(parts: Seq[String]): TableIdentifier = {
    TableIdentifier(parts(1), Option(parts.head))
  }

  def createShuffledRowRDD(sparkContext: SparkContext, localTopK: RDD[InternalRow],
      child: SparkPlan, serializer: Serializer): ShuffledRowRDD = {
    val writeMetrics = SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
    new ShuffledRowRDD(
      ShuffleExchangeExec.prepareShuffleDependency(
        localTopK, child.output, SinglePartition, serializer, writeMetrics), writeMetrics)
  }

  def getInsertIntoCommand(table: LogicalPlan,
      partition: Map[String, Option[String]],
      query: LogicalPlan,
      overwrite: Boolean,
      ifPartitionNotExists: Boolean): InsertIntoStatement = {
    InsertIntoStatement(
      table,
      partition,
      Nil,
      query,
      overwrite,
      ifPartitionNotExists)
  }

  def invokeAnalyzerExecute(analyzer: Analyzer,
      plan: LogicalPlan): LogicalPlan = {
    analyzer.executeAndCheck(plan, QueryPlanningTracker.get.getOrElse(new QueryPlanningTracker))
  }

  def normalizeExpressions[T](r: T, attrs: AttributeSeq): T = {
    CarbonToSparkAdapter.normalizeExpressions(r, attrs)
  }

  def getBuildRight: BuildSide = {
    BuildRight
  }

  def getBuildLeft: BuildSide = {
    BuildLeft
  }

  def withNewExecutionId[T](sparkSession: SparkSession, queryExecution: QueryExecution): T => T = {
    SQLExecution.withNewExecutionId(queryExecution, None)(_)
  }

  def createJoinNode(child: LogicalPlan,
      targetTable: LogicalPlan,
      joinType: JoinType,
      condition: Option[Expression]): Join = {
    Join(child, targetTable, joinType, condition, JoinHint.NONE)
  }

  def getPartitionsFromInsert(x: InsertIntoStatementWrapper): Map[String, Option[String]] = {
    x.partitionSpec
  }

  type CarbonBuildSideType = BuildSide
  type InsertIntoStatementWrapper = InsertIntoStatement

  def getStatisticsObj(outputList: Seq[NamedExpression],
      plan: LogicalPlan, stats: Statistics,
      aliasMap: Option[AttributeMap[Attribute]] = None): Statistics = {
    val output = outputList.map(_.toAttribute)
    val mapSeq = plan.collect { case n: logical.LeafNode => n }.map {
      table => AttributeMap(table.output.zip(output))
    }
    val rewrites = mapSeq.head
    val attributes: AttributeMap[ColumnStat] = stats.attributeStats
    var attributeStats = AttributeMap(attributes.iterator
      .map { pair => (rewrites(pair._1), pair._2) }.toSeq)
    if (aliasMap.isDefined) {
      attributeStats = AttributeMap(
        attributeStats.map(pair => (aliasMap.get(pair._1), pair._2)).toSeq)
    }
    Statistics(stats.sizeInBytes, stats.rowCount, attributeStats)
  }

  def createRefreshTableCommand(tableIdentifier: TableIdentifier): RefreshTableCommand = {
    RefreshTableCommand(tableIdentifier)
  }

  type RefreshTables = RefreshTableCommand

  /**
   * Validates the partition columns and return's A tuple of partition columns and partitioner
   * fields.
   *
   * @param partitionColumns        An instance of ColTypeListContext having parser rules for
   *                                column.
   * @param colNames                <Seq[String]> Sequence of Table column names.
   * @param tableProperties         <Map[String, String]> Table property map.
   * @param partitionByStructFields Seq[StructField] Sequence of partition fields.
   * @return <Seq[PartitionerField]> A Seq of partitioner fields.
   */
  def validatePartitionFields(
    partitionColumns: PartitionFieldListContext,
    colNames: Seq[String],
    tableProperties: mutable.Map[String, String],
    partitionByStructFields: Seq[StructField]): Seq[PartitionerField] = {

    val partitionerFields = partitionByStructFields.map { structField =>
      PartitionerField(structField.name, Some(structField.dataType.toString), null)
    }
    // validate partition clause
    if (partitionerFields.nonEmpty) {
      // partition columns should not be part of the schema
      val badPartCols = partitionerFields.map(_.partitionColumn.toLowerCase).toSet
        .intersect(colNames.map(_.toLowerCase).toSet)
      if (badPartCols.nonEmpty) {
        operationNotAllowed(s"Partition columns should not be specified in the schema: " +
          badPartCols.map("\"" + _ + "\"").mkString("[", ",", "]")
          , partitionColumns: PartitionFieldListContext)
      }
    }
    partitionerFields
  }

  /**
   * The method validates the create table command and returns the create table or
   * ctas table LogicalPlan.
   *
   * @param createTableTuple a tuple of (CreateTableHeaderContext, SkewSpecContext,
   *                         BucketSpecContext, PartitionFieldListContext, ColTypeListContext,
   *                         TablePropertyListContext,
   *                         LocationSpecContext, Option[String], TerminalNode, QueryContext,
   *                         String)
   * @param extraTableTuple  A tuple of (Seq[StructField], Boolean, TableIdentifier, Boolean,
   *                         Seq[String],
   *                         Option[String], mutable.Map[String, String], Map[String, String],
   *                         Seq[StructField],
   *                         Seq[PartitionerField], CarbonSpark2SqlParser, SparkSession,
   *                         Option[LogicalPlan])
   * @return <LogicalPlan> of create table or ctas table
   *
   */
  def createCarbonTable(createTableTuple: (CreateTableHeaderContext, SkewSpecContext,
    BucketSpecContext, PartitionFieldListContext, ColTypeListContext, TablePropertyListContext,
    LocationSpecContext, Option[String], TerminalNode, QueryContext, String),
    extraTableTuple: (Seq[StructField], Boolean, TableIdentifier, Boolean, Seq[String],
    Option[String], mutable.Map[String, String], Map[String, String], Seq[StructField],
    Seq[PartitionerField], CarbonSpark2SqlParser, SparkSession,
      Option[LogicalPlan])): LogicalPlan = {
    val (tableHeader, skewSpecContext, bucketSpecContext, partitionColumns, columns,
    tablePropertyList, locationSpecContext, tableComment, ctas, query, provider) = createTableTuple
    val (cols, external, tableIdentifier, ifNotExists, colNames, tablePath,
    tableProperties, properties, partitionByStructFields, partitionFields,
    parser, sparkSession, selectQuery) = extraTableTuple
    val options = new CarbonOption(properties)
    // validate streaming property
    validateStreamingProperty(options)
    var fields = parser.getFields(cols ++ partitionByStructFields)
    // validate for create table as select
    selectQuery match {
      case Some(q) =>
        // create table as select does not allow creation of partitioned table
        if (partitionFields.nonEmpty) {
          val errorMessage = "A Create Table As Select (CTAS) statement is not allowed to " +
            "create a partitioned table using Carbondata file formats."
          operationNotAllowed(errorMessage, partitionColumns)
        }
        // create table as select does not allow to explicitly specify schema
        if (fields.nonEmpty) {
          operationNotAllowed(
            "Schema may not be specified in a Create Table As Select (CTAS) statement", columns)
        }
        // external table is not allow
        if (external) {
          operationNotAllowed("Create external table as select", tableHeader)
        }
        fields = parser
          .getFields(CarbonEnv.getInstance(sparkSession).carbonMetaStore
            .getSchemaFromUnresolvedRelation(sparkSession, Some(q).get))
      case _ =>
      // ignore this case
    }
    val columnNames = fields.map(_.name.get)
    checkIfDuplicateColumnExists(columns, tableIdentifier, columnNames)
    if (partitionFields.nonEmpty && options.isStreaming) {
      operationNotAllowed("Streaming is not allowed on partitioned table", partitionColumns)
    }

    if (!external && fields.isEmpty) {
      throw new MalformedCarbonCommandException("Creating table without column(s) is not supported")
    }
    if (external && fields.isEmpty && tableProperties.nonEmpty) {
      // as fields are always zero for external table, cannot validate table properties.
      operationNotAllowed(
        "Table properties are not supported for external table", tablePropertyList)
    }

    // Global dictionary is deprecated since 2.0
    if (tableProperties.contains(CarbonCommonConstants.DICTIONARY_INCLUDE) ||
      tableProperties.contains(CarbonCommonConstants.DICTIONARY_EXCLUDE)) {
      DeprecatedFeatureException.globalDictNotSupported()
    }

    val bucketFields = parser.getBucketFields(tableProperties, fields, options)
    var isTransactionalTable: Boolean = true

    val tableInfo = if (external) {
      if (fields.nonEmpty) {
        // user provided schema for this external table, this is not allow currently
        // see CARBONDATA-2866
        operationNotAllowed(
          "Schema must not be specified for external table", columns)
      }
      if (partitionByStructFields.nonEmpty) {
        operationNotAllowed(
          "Partition is not supported for external table", partitionColumns)
      }
      // read table info from schema file in the provided table path
      // external table also must convert table name to lower case
      val identifier = AbsoluteTableIdentifier.from(
        tablePath.get,
        CarbonEnv.getDatabaseName(tableIdentifier.database)(sparkSession).toLowerCase(),
        tableIdentifier.table.toLowerCase())
      val table = try {
        val schemaPath = CarbonTablePath.getSchemaFilePath(identifier.getTablePath)
        if (!FileFactory.isFileExist(schemaPath)) {
          if (provider.equalsIgnoreCase("'carbonfile'")) {
            SchemaReader.inferSchema(identifier, true)
          } else {
            isTransactionalTable = false
            SchemaReader.inferSchema(identifier, false)
          }
        } else {
          SchemaReader.getTableInfo(identifier)
        }
      } catch {
        case e: Throwable =>
          operationNotAllowed(s"Invalid table path provided: ${ tablePath.get } ", tableHeader)
      }

      // set "_external" property, so that DROP TABLE will not delete the data
      if (provider.equalsIgnoreCase("'carbonfile'")) {
        table.getFactTable.getTableProperties.put("_filelevelformat", "true")
        table.getFactTable.getTableProperties.put("_external", "false")
      } else {
        table.getFactTable.getTableProperties.put("_external", "true")
        table.getFactTable.getTableProperties.put("_filelevelformat", "false")
      }
      var isLocalDic_enabled = table.getFactTable.getTableProperties
        .get(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE)
      if (null == isLocalDic_enabled) {
        table.getFactTable.getTableProperties
          .put(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE,
            CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.LOCAL_DICTIONARY_SYSTEM_ENABLE,
                CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE_DEFAULT))
      }
      isLocalDic_enabled = table.getFactTable.getTableProperties
        .get(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE)
      if (CarbonScalaUtil.validateLocalDictionaryEnable(isLocalDic_enabled) &&
        isLocalDic_enabled.toBoolean) {
        val allColumns = table.getFactTable.getListOfColumns
        for (i <- 0 until allColumns.size()) {
          val cols = allColumns.get(i)
          if (cols.getDataType == DataTypes.STRING || cols.getDataType == DataTypes.VARCHAR) {
            cols.setLocalDictColumn(true)
          }
        }
        table.getFactTable.setListOfColumns(allColumns)
      }
      table
    } else {
      // prepare table model of the collected tokens
      val tableModel: TableModel = CarbonParserUtil.prepareTableModel(
        ifNotExists,
        convertDbNameToLowerCase(tableIdentifier.database),
        tableIdentifier.table.toLowerCase,
        fields,
        partitionFields,
        tableProperties,
        bucketFields,
        isAlterFlow = false,
        tableComment)
      TableNewProcessor(tableModel)
    }
    tableInfo.setTransactionalTable(isTransactionalTable)
    selectQuery match {
      case query@Some(q) =>
        CarbonCreateTableAsSelectCommand(
          tableInfo = tableInfo,
          query = query.get,
          ifNotExistsSet = ifNotExists,
          tableLocation = tablePath)
      case _ =>
        CarbonCreateTableCommand(
          tableInfo = tableInfo,
          ifNotExistsSet = ifNotExists,
          tableLocation = tablePath,
          external)
    }
  }

  def getField(parser: CarbonSpark2SqlParser,
               schema: Seq[QualifiedColType],
               isExternal: Boolean = false): Seq[Field] = {
    schema.map { col =>
      parser.getFields(col.comment, col.name.head, col.dataType, isExternal)
    }
  }

  def supportsBatchOrColumnar(scan: CarbonDataSourceScan): Boolean = {
    scan.supportsColumnar
  }

  def createDataset(sparkSession: SparkSession, qe: QueryExecution) : Dataset[Row] = {
    new Dataset[Row](qe, RowEncoder(qe.analyzed.schema))
  }

  def createSharedState(sparkContext: SparkContext) : SharedState = {
    new SharedState(sparkContext, Map.empty[String, String])
  }

  def translateFilter(dataFilters: Seq[Expression]) : Seq[Filter] = {
    dataFilters.flatMap(DataSourceStrategy.translateFilter(_,
      supportNestedPredicatePushdown = true))
  }

  def getCarbonOptimizer(session: SparkSession,
      sessionState: SessionState): CarbonOptimizer = {
    new CarbonOptimizer(session, sessionState.optimizer)
  }

  def isCharType(dataType: DataType): Boolean = {
    dataType.isInstanceOf[CharType]
  }

  def isVarCharType(dataType: DataType): Boolean = {
    dataType.isInstanceOf[VarcharType]
  }

  def getTypeName(s: AbstractDataType): String = {
    s.defaultConcreteType.typeName
  }

}

case class CarbonBuildSide(buildSide: BuildSide) {
  def isRight: Boolean = buildSide.isInstanceOf[BuildRight.type]
  def isLeft: Boolean = buildSide.isInstanceOf[BuildLeft.type]
}

class CarbonOptimizer(session: SparkSession, optimizer: Optimizer) extends
  Optimizer(session.sessionState.catalogManager) {

  private lazy val mvRules = Seq(Batch("Materialized View Optimizers", Once,
    Seq(new MVRewriteRule(session)): _*))

  private lazy val iudRule = Batch("IUD Optimizers", fixedPoint,
    Seq(new CarbonIUDRule(), new CarbonUDFTransformRule(), new CarbonFileIndexReplaceRule()): _*)

  private lazy val secondaryIndexRule = Batch("SI Optimizers", Once,
    Seq(new CarbonSITransformationRule(session)): _*)

  override def defaultBatches: Seq[Batch] = {
    mvRules ++ convertedBatch() :+ iudRule :+ secondaryIndexRule
  }

  def convertedBatch(): Seq[Batch] = {
    optimizer.batches.map { batch =>
      Batch(
        batch.name,
        batch.strategy match {
          case optimizer.Once =>
            Once
          case _: optimizer.FixedPoint =>
            fixedPoint
        },
        batch.rules: _*
      )
    }
  }
}
