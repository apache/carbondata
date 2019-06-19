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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.plans.logical.{UnaryNode, _}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.optimizer.CarbonDecoderRelation

import org.apache.carbondata.spark.CarbonAliasDecoderRelation

case class CarbonDictionaryCatalystDecoder(
    relations: Seq[CarbonDecoderRelation],
    profile: CarbonProfile,
    aliasMap: CarbonAliasDecoderRelation,
    isOuter: Boolean,
    child: LogicalPlan) extends UnaryNode {
  // the output should be updated with converted datatype, it is need for limit+sort plan.
  override def output: Seq[Attribute] = {
    child match {
      case l: LogicalRelation =>
        // If the child is logical plan then first update all dictionary attr with IntegerType
        val logicalOut =
          CarbonDictionaryDecoder.updateAttributes(child.output, relations, aliasMap)
        CarbonDictionaryDecoder.convertOutput(logicalOut, relations, profile, aliasMap)
      case Filter(cond, l: LogicalRelation) =>
        // If the child is logical plan then first update all dictionary attr with IntegerType
        val logicalOut =
          CarbonDictionaryDecoder.updateAttributes(child.output, relations, aliasMap)
        CarbonDictionaryDecoder.convertOutput(logicalOut, relations, profile, aliasMap)
      case Join(l: LogicalRelation, r: LogicalRelation, _, _) =>
        val logicalOut =
          CarbonDictionaryDecoder.updateAttributes(child.output, relations, aliasMap)
        CarbonDictionaryDecoder.convertOutput(logicalOut, relations, profile, aliasMap)
      case _ => CarbonDictionaryDecoder.convertOutput(child.output, relations, profile, aliasMap)
    }
  }

}

abstract class CarbonProfile(attributes: Seq[Attribute]) extends Serializable {
  def isEmpty: Boolean = attributes.isEmpty
}

case class IncludeProfile(attributes: Seq[Attribute]) extends CarbonProfile(attributes)

case class ExcludeProfile(attributes: Seq[Attribute]) extends CarbonProfile(attributes)

case class ProjectForUpdate(
    table: UnresolvedRelation,
    columns: List[String],
    children: Seq[LogicalPlan] ) extends LogicalPlan {
  override def output: Seq[AttributeReference] = Seq.empty
}

case class UpdateTable(
    table: UnresolvedRelation,
    columns: List[String],
    selectStmt: String,
    alias: Option[String] = None,
    filer: String) extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Seq.empty
  override def output: Seq[AttributeReference] = Seq.empty
}

case class DeleteRecords(
    statement: String,
    alias: Option[String] = None,
    table: UnresolvedRelation) extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Seq.empty
  override def output: Seq[AttributeReference] = Seq.empty
}

/**
 * A logical plan representing insertion into Hive table
 * This plan ignores nullability of ArrayType, MapType, StructType unlike InsertIntoTable
 * because Hive Table doesn't have nullability for ARRAY, MAP,STRUCT types.
 */
case class InsertIntoCarbonTable (table: CarbonDatasourceHadoopRelation,
    partition: Map[String, Option[String]],
    child: LogicalPlan,
    overwrite: Boolean,
    ifNotExists: Boolean)
  extends Command {

    override def output: Seq[Attribute] = Seq.empty

    // This is the expected schema of the table prepared to be inserted into
    // including dynamic partition columns.
    val tableOutput = table.carbonRelation.output
}

/**
 * It checks if query is count(*) then push down to carbon
 *
 * The returned values for this match are as follows:
 * - whether its count star
 * - count star attribute
 * - child plan
 */
object CountStarPlan {
  type ReturnType = (mutable.MutableList[Attribute], LogicalPlan)

 /**
  * It fill count star query attribute.
  * 2.2.1 plan
  * Aggregate [count(1) AS count(1)#30L]
  * +- Project
  *
  *2.3.0 plan
  * Aggregate [cast(count(1) as string) AS count(1)#29]
  * +- Project
  */
 private def fillCountStarAttribute(
     expr: Expression,
     outputColumns: mutable.MutableList[Attribute]) {
   expr match {
     case par@Alias(cast: Cast, _) =>
       if (cast.child.isInstanceOf[AggregateExpression]) {
         val head = cast.child.children.head
         head match {
           case count: Count if count.children.head.isInstanceOf[Literal] =>
             outputColumns += par.toAttribute
           case _ =>
         }
       }
     case par@Alias(child, _) =>
       if (child.isInstanceOf[AggregateExpression]) {
         val head = child.children.head
         head match {
           case count: Count if count.children.head.isInstanceOf[Literal] =>
             outputColumns += par.toAttribute
           case _ =>
         }
       }
   }
 }

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    plan match {
      case Aggregate(groupingExpressions, aggregateExpressions,
      child) if strictCountStar(groupingExpressions, aggregateExpressions, child) =>
        val outputColumns = scala.collection.mutable.MutableList[Attribute]()
        fillCountStarAttribute(aggregateExpressions.head, outputColumns)
        if (outputColumns.nonEmpty) {
          Some(outputColumns, child)
        } else {
          None
        }
      case _ => None
    }
  }

  /**
   * check if child
   */
  def strictCountStar(groupingExpressions: Seq[Expression],
      partialComputation: Seq[NamedExpression],
      child: LogicalPlan): Boolean = {
    if (groupingExpressions.nonEmpty) {
      return false
    }
    if (partialComputation.isEmpty) {
      return false
    }
    if (partialComputation.size > 1 && partialComputation.nonEmpty) {
      return false
    }
    child collect {
      case cd: Filter => return false
    }
    true
  }
}
