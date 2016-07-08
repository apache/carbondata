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

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.TableIdentifier._
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, _}
import org.apache.spark.sql.catalyst.planning.{PhysicalOperation, QueryPlanner}
import org.apache.spark.sql.catalyst.plans.logical.{Filter => LogicalFilter, LogicalPlan}
import org.apache.spark.sql.execution.{DescribeCommand => RunnableDescribeCommand, ExecutedCommand, Filter, Project, SparkPlan}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{DescribeCommand => LogicalDescribeCommand, LogicalRelation}
import org.apache.spark.sql.hive.execution.{DescribeHiveTableCommand, DropTable, HiveNativeCommand}
import org.apache.spark.sql.optimizer.{CarbonAliasDecoderRelation, CarbonDecoderRelation}
import org.apache.spark.sql.types.IntegerType

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.spark.exception.MalformedCarbonCommandException


class CarbonStrategies(sqlContext: SQLContext) extends QueryPlanner[SparkPlan] {

  override def strategies: Seq[Strategy] = getStrategies

  val LOGGER = LogServiceFactory.getLogService("CarbonStrategies")

  def getStrategies: Seq[Strategy] = {
    val total = sqlContext.planner.strategies :+ CarbonTableScan
    total
  }

  /**
   * Carbon strategies for performing late materizlization (decoding dictionary key
   * as late as possbile)
   */
  private[sql] object CarbonTableScan extends Strategy {

    def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      plan match {
        case PhysicalOperation(projectList, predicates,
        l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)) =>
          if (isStarQuery(plan)) {
            carbonRawScanForStarQuery(projectList, predicates, carbonRelation, l)(sqlContext) :: Nil
          } else {
            carbonRawScan(projectList,
              predicates,
              carbonRelation,
              l)(sqlContext) :: Nil
          }
        case CarbonDictionaryCatalystDecoder(relations, profile, aliasMap, _, child) =>
          CarbonDictionaryDecoder(relations,
            profile,
            aliasMap,
            planLater(child))(sqlContext) :: Nil
        case _ =>
          Nil
      }
    }

    /**
     * Create carbon scan
     */
    private def carbonRawScan(projectList: Seq[NamedExpression],
      predicates: Seq[Expression],
      relation: CarbonDatasourceRelation,
      logicalRelation: LogicalRelation)(sc: SQLContext): SparkPlan = {

      val tableName: String =
        relation.carbonRelation.metaData.carbonTable.getFactTableName.toLowerCase
      // Check out any expressions are there in project list. if they are present then we need to
      // decode them as well.
      val projectSet = AttributeSet(projectList.flatMap(_.references))
      val scan = CarbonScan(projectSet.toSeq,
        relation.carbonRelation,
        predicates)(sqlContext)
      projectList.map {
        case attr: AttributeReference =>
        case Alias(attr: AttributeReference, _) =>
        case others =>
          others.references.map{f =>
            val dictionary = relation.carbonRelation.metaData.dictionaryMap.get(f.name)
            if (dictionary.isDefined && dictionary.get) {
              scan.attributesNeedToDecode.add(f.asInstanceOf[AttributeReference])
            }
          }
      }
      if (scan.attributesNeedToDecode.size() > 0) {
        val decoder = getCarbonDecoder(logicalRelation,
          sc,
          tableName,
          scan.attributesNeedToDecode.asScala.toSeq,
          scan)
        if (scan.unprocessedExprs.nonEmpty) {
          val filterCondToAdd = scan.unprocessedExprs.reduceLeftOption(expressions.And)
          Project(projectList, filterCondToAdd.map(Filter(_, decoder)).getOrElse(decoder))
        } else {
          Project(projectList, decoder)
        }
      } else {
        Project(projectList, scan)
      }
    }

    /**
     * Create carbon scan for star query
     */
    private def carbonRawScanForStarQuery(projectList: Seq[NamedExpression],
      predicates: Seq[Expression],
      relation: CarbonDatasourceRelation,
      logicalRelation: LogicalRelation)(sc: SQLContext): SparkPlan = {

      val tableName: String =
        relation.carbonRelation.metaData.carbonTable.getFactTableName.toLowerCase
      // Check out any expressions are there in project list. if they are present then we need to
      // decode them as well.
      val projectExprsNeedToDecode = new java.util.HashSet[Attribute]()
      val scan = CarbonScan(projectList.map(_.toAttribute),
        relation.carbonRelation,
        predicates,
        useUnsafeCoversion = false)(sqlContext)
      projectExprsNeedToDecode.addAll(scan.attributesNeedToDecode)
      val updatedAttrs = scan.attributesRaw.map(attr =>
        updateDataType(attr.asInstanceOf[AttributeReference], relation, projectExprsNeedToDecode))
      scan.attributesRaw = updatedAttrs
      if (projectExprsNeedToDecode.size() > 0
          && isDictionaryEncoded(projectExprsNeedToDecode.asScala.toSeq, relation)) {
        val decoder = getCarbonDecoder(logicalRelation,
          sc,
          tableName,
          projectExprsNeedToDecode.asScala.toSeq,
          scan)
        if (scan.unprocessedExprs.nonEmpty) {
          val filterCondToAdd = scan.unprocessedExprs.reduceLeftOption(expressions.And)
          filterCondToAdd.map(Filter(_, decoder)).getOrElse(decoder)
        } else {
          decoder
        }
      } else {
        if (scan.unprocessedExprs.nonEmpty) {
          val filterCondToAdd = scan.unprocessedExprs.reduceLeftOption(expressions.And)
          filterCondToAdd.map(Filter(_, scan)).getOrElse(scan)
        } else {
          scan
        }
      }
    }

    def getCarbonDecoder(logicalRelation: LogicalRelation,
      sc: SQLContext,
      tableName: String,
      projectExprsNeedToDecode: Seq[Attribute],
      scan: CarbonScan): CarbonDictionaryDecoder = {
      val relation = CarbonDecoderRelation(logicalRelation.attributeMap,
        logicalRelation.relation.asInstanceOf[CarbonDatasourceRelation])
      val attrs = projectExprsNeedToDecode.map { attr =>
        val newAttr = AttributeReference(attr.name,
          attr.dataType,
          attr.nullable,
          attr.metadata)(attr.exprId, Seq(tableName))
        relation.addAttribute(newAttr)
        newAttr
      }
      CarbonDictionaryDecoder(Seq(relation), IncludeProfile(attrs),
        CarbonAliasDecoderRelation(), scan)(sc)
    }

    def isDictionaryEncoded(projectExprsNeedToDecode: Seq[Attribute],
        relation: CarbonDatasourceRelation): Boolean = {
      var isEncoded = false
      projectExprsNeedToDecode.foreach { attr =>
        if (relation.carbonRelation.metaData.dictionaryMap.get(attr.name).getOrElse(false)) {
          isEncoded = true
        }
      }
      isEncoded
    }

    def updateDataType(attr: AttributeReference,
        relation: CarbonDatasourceRelation,
        allAttrsNotDecode: util.Set[Attribute]): AttributeReference = {
      if (relation.carbonRelation.metaData.dictionaryMap.get(attr.name).getOrElse(false) &&
        !allAttrsNotDecode.asScala.exists(p => p.name.equals(attr.name))) {
        AttributeReference(attr.name,
          IntegerType,
          attr.nullable,
          attr.metadata)(attr.exprId, attr.qualifiers)
      } else {
        attr
      }
    }

    private def isStarQuery(plan: LogicalPlan) = {
      plan match {
        case LogicalFilter(condition,
        LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)) => true
        case LogicalRelation(carbonRelation: CarbonDatasourceRelation, _) => true
        case _ => false
      }
    }
  }

  object DDLStrategies extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case DropTable(tableName, ifNotExists)
        if CarbonEnv.getInstance(sqlContext).carbonCatalog
            .tableExists(toTableIdentifier(tableName.toLowerCase))(sqlContext) =>
        val identifier = toTableIdentifier(tableName.toLowerCase)
        ExecutedCommand(DropTableCommand(ifNotExists, identifier.database, identifier.table)) :: Nil
      case ShowLoadsCommand(schemaName, cube, limit) =>
        ExecutedCommand(ShowLoads(schemaName, cube, limit, plan.output)) :: Nil
      case LoadTable(schemaNameOp, cubeName, factPathFromUser, dimFilesPath,
      partionValues, isOverwriteExist, inputSqlString) =>
        val isCarbonTable = CarbonEnv.getInstance(sqlContext).carbonCatalog
            .tableExists(TableIdentifier(cubeName, schemaNameOp))(sqlContext)
        if (isCarbonTable || partionValues.nonEmpty) {
          ExecutedCommand(LoadTable(schemaNameOp, cubeName, factPathFromUser,
            dimFilesPath, partionValues, isOverwriteExist, inputSqlString)) :: Nil
        } else {
          ExecutedCommand(HiveNativeCommand(inputSqlString)) :: Nil
        }
      case d: HiveNativeCommand =>
        try {
          val resolvedTable = sqlContext.executePlan(CarbonHiveSyntax.parse(d.sql)).optimizedPlan
          planLater(resolvedTable) :: Nil
        } catch {
          case ce: MalformedCarbonCommandException =>
            throw ce
          case ae: AnalysisException =>
            throw ae
          case e: Exception => ExecutedCommand(d) :: Nil
        }
      case DescribeFormattedCommand(sql, tblIdentifier) =>
        val isCube = CarbonEnv.getInstance(sqlContext).carbonCatalog
            .tableExists(tblIdentifier)(sqlContext)
        if (isCube) {
          val describe =
            LogicalDescribeCommand(UnresolvedRelation(tblIdentifier, None), isExtended = false)
          val resolvedTable = sqlContext.executePlan(describe.table).analyzed
          val resultPlan = sqlContext.executePlan(resolvedTable).executedPlan
          ExecutedCommand(DescribeCommandFormatted(resultPlan, plan.output, tblIdentifier)) :: Nil
        } else {
          ExecutedCommand(DescribeNativeCommand(sql, plan.output)) :: Nil
        }
      case describe@LogicalDescribeCommand(table, isExtended) =>
        val resolvedTable = sqlContext.executePlan(describe.table).analyzed
        resolvedTable match {
          case t: MetastoreRelation =>
            ExecutedCommand(
              DescribeHiveTableCommand(t, describe.output, describe.isExtended)) :: Nil
          case o: LogicalPlan =>
            val resultPlan = sqlContext.executePlan(o).executedPlan
            ExecutedCommand(
              RunnableDescribeCommand(resultPlan, describe.output, describe.isExtended)) :: Nil
        }
      case _ =>
        Nil
    }

    def toTableIdentifier(name: String): TableIdentifier = {
      val identifier = name.split("\\.")
      identifier match {
        case Array(tableName) => TableIdentifier(tableName, None)
        case Array(dbName, tableName) => TableIdentifier(tableName, Some(dbName))
      }
    }
  }

}

object CarbonHiveSyntax {

  @transient
  protected val sqlParser = new CarbonSqlParser

  def parse(sqlText: String): LogicalPlan = {
    sqlParser.parse(sqlText)
  }
}
