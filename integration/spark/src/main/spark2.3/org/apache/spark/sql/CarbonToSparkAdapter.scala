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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.antlr.v4.runtime.tree.TerminalNode
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.carbondata.execution.datasources.CarbonFileIndexReplaceRule
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, SessionCatalog}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, AttributeSeq, AttributeSet, ExprId, Expression, ExpressionSet, NamedExpression, ScalaUDF, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Join, LogicalPlan, OneRowRelation, Statistics, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.{CarbonParserUtil, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, UnresolvedRelation}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.{BucketSpecContext, ColTypeListContext, CreateTableHeaderContext, LocationSpecContext, QueryContext, SkewSpecContext, TablePropertyListContext}
import org.apache.spark.sql.catalyst.parser.ParserUtils.operationNotAllowed
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.catalyst.plans.{JoinType, QueryPlan, logical}
import org.apache.spark.sql.execution.command.{ExplainCommand, Field, PartitionerField, ResetCommand, TableModel, TableNewProcessor}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, FilePartition, PartitionedFile, RefreshTable}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution, ShuffledRowRDD, SparkPlan}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.optimizer.{CarbonIUDRule, CarbonUDFTransformRule, MVRewriteRule}
import org.apache.spark.sql.secondaryindex.optimizer.CarbonSITransformationRule
import org.apache.spark.sql.types.{DataType, Metadata, StructField}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.internal.{SessionState, SharedState}
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoTable
import org.apache.spark.sql.execution.command.table.{CarbonCreateTableAsSelectCommand, CarbonCreateTableCommand}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.parser.CarbonSparkSqlParserUtil.{checkIfDuplicateColumnExists, convertDbNameToLowerCase, validateStreamingProperty}

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
    SparkSqlAdapter.addSparkSessionListener(sparkSession)
  }

  def addSparkListener(sparkContext: SparkContext): Unit = {
    sparkContext.addSparkListener(new SparkListener {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        SparkSession.setDefaultSession(null)
      }
    })
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

  def createAttributeReference(name: String, dataType: DataType, nullable: Boolean,
      metadata: Metadata, exprId: ExprId, qualifier: Option[String],
      attrRef : NamedExpression = null): AttributeReference = {
    AttributeReference(
      name,
      dataType,
      nullable,
      metadata)(exprId, qualifier)
  }

  def createAttributeReference(attr: AttributeReference,
      attrName: String,
      newSubsume: String): AttributeReference = {
    AttributeReference(attrName, attr.dataType)(
      exprId = attr.exprId,
      qualifier = Some(newSubsume))
  }

  def getTheLastQualifier(attribute: Attribute): String = {
    attribute.qualifier.head
  }

  def getOutput(subQueryAlias: SubqueryAlias): Seq[Attribute] = {
    subQueryAlias.output
  }

  def createScalaUDF(s: ScalaUDF, reference: AttributeReference): ScalaUDF = {
    ScalaUDF(s.function, s.dataType, Seq(reference), s.inputTypes)
  }

  def createExprCode(code: String, isNull: String, value: String, dataType: DataType = null
  ): ExprCode = {
    ExprCode(code, isNull, value)
  }

  def createAliasRef(child: Expression,
      name: String,
      exprId: ExprId = NamedExpression.newExprId,
      qualifier: Option[String] = None,
      explicitMetadata: Option[Metadata] = None,
      namedExpr : Option[NamedExpression] = None ) : Alias = {

    Alias(child, name)(exprId, qualifier, explicitMetadata)
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

  def getHiveExternalCatalog(sparkSession: SparkSession): HiveExternalCatalog = {
    sparkSession.sessionState.catalog.externalCatalog.asInstanceOf[HiveExternalCatalog]
  }

  def createFilePartition(index: Int, files: ArrayBuffer[PartitionedFile]) = {
    FilePartition(index, files.toArray.toSeq)
  }

  def stringToTimestamp(timestamp: String): Option[Long] = {
    DateTimeUtils.stringToTimestamp(UTF8String.fromString(timestamp))
  }

  def getTableIdentifier(u: UnresolvedRelation): Some[TableIdentifier] = {
    Some(u.tableIdentifier)
  }

  def dateToString(date: Int): String = {
    DateTimeUtils.dateToString(date.toString.toInt)
  }

  def timeStampToString(timeStamp: Long): String = {
    DateTimeUtils.timestampToString(timeStamp)
  }

  def stringToTime(value: String): java.util.Date = {
    DateTimeUtils.stringToTime(value)
  }

  def getProcessingTime: String => Trigger = {
    Trigger.ProcessingTime
  }

  def addTaskCompletionListener[U](f: => U) {
    TaskContext.get().addTaskCompletionListener { context =>
      f
    }
  }

  def createShuffledRowRDD(sparkContext: SparkContext, localTopK: RDD[InternalRow],
      child: SparkPlan, serializer: Serializer): ShuffledRowRDD = {
    new ShuffledRowRDD(
      ShuffleExchangeExec.prepareShuffleDependency(
        localTopK, child.output, SinglePartition, serializer))
  }

  def getInsertIntoCommand(table: LogicalPlan,
      partition: Map[String, Option[String]],
      query: LogicalPlan,
      overwrite: Boolean,
      ifPartitionNotExists: Boolean): InsertIntoTable = {
    InsertIntoTable(
      table,
      partition,
      query,
      overwrite,
      ifPartitionNotExists)
  }

  def getExplainCommandObj(logicalPlan: LogicalPlan = OneRowRelation(),
      mode: Option[String]) : ExplainCommand = {
    ExplainCommand(logicalPlan, mode.isDefined)
  }

  def getExplainCommandObj(mode: Option[String]) : ExplainCommand = {
    ExplainCommand(OneRowRelation(), mode.isDefined)
  }

  def invokeAnalyzerExecute(analyzer: Analyzer,
      plan: LogicalPlan): LogicalPlan = {
    analyzer.executeAndCheck(plan)
  }

  def normalizeExpressions(r: NamedExpression, attrs: AttributeSeq): NamedExpression = {
    QueryPlan.normalizeExprId(r, attrs)
  }

  def getBuildRight: BuildSide = {
    BuildRight
  }

  def getBuildLeft: BuildSide = {
    BuildLeft
  }

  type CarbonBuildSideType = BuildSide
  type InsertIntoStatementWrapper = InsertIntoTable

  def withNewExecutionId[T](sparkSession: SparkSession, queryExecution: QueryExecution): T => T = {
    SQLExecution.withNewExecutionId(sparkSession, queryExecution)(_)
  }

  def getTableIdentifier(parts: TableIdentifier): TableIdentifier = {
    parts
  }

  def createJoinNode(child: LogicalPlan,
      targetTable: LogicalPlan,
      joinType: JoinType,
      condition: Option[Expression]): Join = {
    Join(child, targetTable, joinType, condition)
  }

  def getPartitionsFromInsert(x: InsertIntoStatementWrapper): Map[String, Option[String]] = {
    x.partition
  }

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
    Statistics(stats.sizeInBytes, stats.rowCount, attributeStats, stats.hints)
  }

  def createRefreshTableCommand(tableIdentifier: TableIdentifier): RefreshTable = {
    RefreshTable(tableIdentifier)
  }

  type RefreshTables = RefreshTable

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
    partitionColumns: ColTypeListContext,
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
          , partitionColumns: ColTypeListContext)
      }
    }
    partitionerFields
  }

  /**
   * The method validates the create table command and returns the create table or
   * ctas table LogicalPlan.
   *
   * @param createTableTuple a tuple of (CreateTableHeaderContext, SkewSpecContext,
   *                         BucketSpecContext, ColTypeListContext, ColTypeListContext,
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
    BucketSpecContext, ColTypeListContext, ColTypeListContext, TablePropertyListContext,
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
               schema: Seq[StructField],
               isExternal: Boolean = false): Seq[Field] = {
    schema.map { col =>
      parser.getFields(col.getComment, col.name, col.dataType, isExternal)
    }
  }

  def supportsBatchOrColumnar(scan: CarbonDataSourceScan): Boolean = {
    scan.supportsBatch
  }

  def createDataset(sparkSession: SparkSession, qe: QueryExecution) : Dataset[Row] = {
    new Dataset[Row](sparkSession, qe, RowEncoder(qe.analyzed.schema))
  }

  def createSharedState(sparkContext: SparkContext) : SharedState = {
    new SharedState(sparkContext)
  }

  def translateFilter(dataFilters: Seq[Expression]) : Seq[Filter] = {
    dataFilters.flatMap(DataSourceStrategy.translateFilter)
  }

  def getCarbonOptimizer(session : SparkSession, sessionState: SessionState) : CarbonOptimizer = {
    new CarbonOptimizer(session, sessionState.catalog, sessionState.optimizer)
  }

  def isCharType(dataType: DataType) = {
    false
  }

}

case class CarbonBuildSide(buildSide: BuildSide) {
  def isRight: Boolean = buildSide.isInstanceOf[BuildRight.type]
  def isLeft: Boolean = buildSide.isInstanceOf[BuildLeft.type]
}

class CarbonOptimizer(
    session: SparkSession,
    catalog: SessionCatalog,
    optimizer: Optimizer) extends Optimizer(catalog) {

  private lazy val mvRules = Seq(Batch("Materialized View Optimizers", Once,
    Seq(new MVRewriteRule(session)): _*))

  private lazy val iudRule = Batch("IUD Optimizers", fixedPoint,
    Seq(new CarbonIUDRule(), new CarbonUDFTransformRule(), new CarbonFileIndexReplaceRule()): _*)

  private lazy val secondaryIndexRule = Batch("SI Optimizers", Once,
    Seq(new CarbonSITransformationRule(session)): _*)

  override def batches: Seq[Batch] = {
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
