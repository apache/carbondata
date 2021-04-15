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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.carbondata.execution.datasources.CarbonFileIndexReplaceRule
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, AttributeSeq, AttributeSet, Expression, ExpressionSet, ExprId, NamedExpression, ScalaUDF, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide, Optimizer}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Join, LogicalPlan, OneRowRelation, Statistics, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.catalyst.plans.{logical, JoinType, QueryPlan}
import org.apache.spark.sql.execution.command.{ExplainCommand, ResetCommand}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile, RefreshTable}
import org.apache.spark.sql.execution.{ExplainMode, QueryExecution, ShuffledRowRDD, SimpleMode, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.optimizer.{CarbonIUDRule, CarbonUDFTransformRule, MVRewriteRule}
import org.apache.spark.sql.secondaryindex.optimizer.CarbonSITransformationRule
import org.apache.spark.sql.types.{DataType, Metadata}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.util.ThreadLocalSessionInfo

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

  def getExplainCommandObj() : ExplainCommand = {
    ExplainCommand(OneRowRelation())
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
    DateTimeUtils.timestampToString(value.toLong * 1000)
  }

  def stringToTime(value: String): String = {
    DateTimeUtils.toJavaDate(DateTimeUtils.stringToDate(UTF8String.fromString(value),
      ZoneId.systemDefault()).get).getTime.toString
  }

  def getProcessingTime: String => ProcessingTime = {
    ProcessingTime
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

  type CarbonBuildSide = BuildSide

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

  def createResetCommand(): ResetCommand = {
    ResetCommand()
  }

  def createRefreshTableCommand(tableIdentifier: TableIdentifier): RefreshTable = {
    RefreshTable(tableIdentifier)
  }

  type RefreshTable = spark.sql.execution.datasources.RefreshTable

}


class CarbonOptimizer(session: SparkSession) extends Optimizer(session.sessionState.catalog) {

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
    session.sessionState.optimizer.batches.map { batch =>
      Batch(
        batch.name,
        batch.strategy match {
          case session.sessionState.optimizer.Once =>
            Once
          case _: session.sessionState.optimizer.FixedPoint =>
            fixedPoint
        },
        batch.rules: _*
      )
    }
  }
}
