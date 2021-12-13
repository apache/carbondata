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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.carbondata.execution.datasources.CarbonFileIndexReplaceRule
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, Expression, ExprId, NamedExpression, ScalaUDF, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.optimizer.{CarbonIUDRule, CarbonUDFTransformRule, MVRewriteRule}
import org.apache.spark.sql.secondaryindex.optimizer.CarbonSITransformationRule
import org.apache.spark.sql.types.{DataType, Metadata, StringType}

import org.apache.carbondata.core.util.ThreadLocalSessionInfo
import org.apache.carbondata.geo.{InPolygonJoinUDF, ToRangeListAsStringUDF}

object CarbonToSparkAdapter extends SparkVersionAdapter {

  def createFilePartition(index: Int, files: ArrayBuffer[PartitionedFile]): FilePartition = {
    FilePartition(index, files.toArray.toSeq)
  }

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

  def createRangeListScalaUDF(toRangeListUDF: ToRangeListAsStringUDF,
      dataType: StringType.type,
      children: Seq[Expression],
      inputTypes: Seq[DataType]): ScalaUDF = {
    ScalaUDF(toRangeListUDF,
      dataType,
      children,
      inputTypes,
      Some("ToRangeListAsString"))
  }

  def getTransformedPolygonJoinUdf(scalaUdf: ScalaUDF,
      udfChildren: Seq[Expression],
      polygonJoinUdf: InPolygonJoinUDF): ScalaUDF = {
    ScalaUDF(polygonJoinUdf,
      scalaUdf.dataType,
      udfChildren,
      scalaUdf.inputTypes :+ scalaUdf.inputTypes.head,
      scalaUdf.udfName,
      scalaUdf.nullable,
      scalaUdf.udfDeterministic)
  }

  def createExprCode(code: String, isNull: String, value: String, dataType: DataType = null
  ): ExprCode = {
    ExprCode(code, isNull, value)
  }

  def createAliasRef(child: Expression,
      name: String,
      exprId: ExprId = NamedExpression.newExprId,
      qualifier: Option[String] = None,
      explicitMetadata: Option[Metadata] = None) : Alias = {

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

  def getDataFilter(partitionSet: AttributeSet,
      filter: Seq[Expression],
      partitionFilter: Seq[Expression]): Seq[Expression] = {
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
