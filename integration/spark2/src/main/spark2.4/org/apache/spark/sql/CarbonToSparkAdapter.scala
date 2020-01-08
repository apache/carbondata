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

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, AttributeSet, ExprId, Expression, ExpressionSet, NamedExpression, ScalaUDF, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.types.{DataType, Metadata}

object CarbonToSparkAdapter {

  def addSparkListener(sparkContext: SparkContext) = {
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

  def createScalaUDF(s: ScalaUDF, reference: AttributeReference) = {
    ScalaUDF(s.function, s.dataType, Seq(reference), s.inputsNullSafe, s.inputTypes)
  }

  def createExprCode(code: String, isNull: String, value: String, dataType: DataType) = {
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

  def getExplainCommandObj() : ExplainCommand = {
    ExplainCommand(OneRowRelation())
  }

  /**
   * As a part of SPARK-24085 Hive tables supports scala subquery for
   * parition tables, so Carbon also needs to supports
   * @param partitionSet
   * @param filterPredicates
   * @return
   */
  def getPartitionKeyFilter(
      partitionSet: AttributeSet,
      filterPredicates: Seq[Expression]): ExpressionSet = {
    ExpressionSet(
      ExpressionSet(filterPredicates)
        .filterNot(SubqueryExpression.hasSubquery)
        .filter(_.references.subsetOf(partitionSet)))
  }

  // As per SPARK-22520 OptimizeCodegen is removed in 2.3.1
  def getOptimizeCodegenRule(conf :SQLConf): Seq[Rule[LogicalPlan]] = {
    Seq.empty
  }

  def getUpdatedStorageFormat(storageFormat: CatalogStorageFormat,
      map: Map[String, String],
      tablePath: String): CatalogStorageFormat = {
    storageFormat.copy(properties = map, locationUri = Some(new URI(tablePath)))
  }
}
