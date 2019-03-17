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
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, AttributeSet, ExprId, Expression, ExpressionSet, NamedExpression, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.optimizer.OptimizeCodegen
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.types.{DataType, Metadata}

object CarbonToSparkAdapter {

  def addSparkListener(sparkContext: SparkContext) = {
    sparkContext.addSparkListener(new SparkListener {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        SparkSession.setDefaultSession(null)
        SparkSession.sqlListener.set(null)
      }
    })
  }

  def createAttributeReference(name: String, dataType: DataType, nullable: Boolean,
                               metadata: Metadata,exprId: ExprId, qualifier: Option[String],
                               attrRef : NamedExpression): AttributeReference = {
    AttributeReference(
      name,
      dataType,
      nullable,
      metadata)(exprId, qualifier,attrRef.isGenerated)
  }

  def createAliasRef(child: Expression,
                     name: String,
                     exprId: ExprId = NamedExpression.newExprId,
                     qualifier: Option[String] = None,
                     explicitMetadata: Option[Metadata] = None,
                     namedExpr: Option[NamedExpression] = None): Alias = {
    val isGenerated:Boolean = if (namedExpr.isDefined) {
      namedExpr.get.isGenerated
    } else {
      false
    }
    Alias(child, name)(exprId, qualifier, explicitMetadata,isGenerated)
  }

  def getExplainCommandObj() : ExplainCommand = {
    ExplainCommand(OneRowRelation)
  }

  def getPartitionKeyFilter(
      partitionSet: AttributeSet,
      filterPredicates: Seq[Expression]): ExpressionSet = {
    ExpressionSet(
      ExpressionSet(filterPredicates)
        .filter(_.references.subsetOf(partitionSet)))
  }

  def getUpdatedStorageFormat(storageFormat: CatalogStorageFormat,
      map: Map[String, String],
      tablePath: String): CatalogStorageFormat = {
    storageFormat.copy(properties = map, locationUri = Some(tablePath))
  }

  def getOptimizeCodegenRule(conf :SQLConf): Seq[Rule[LogicalPlan]] = {
    Seq(OptimizeCodegen(conf))
  }
}
