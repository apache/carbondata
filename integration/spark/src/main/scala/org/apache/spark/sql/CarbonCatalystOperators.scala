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

import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.logical.{UnaryNode, _}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.optimizer.{CarbonAliasDecoderRelation, CarbonDecoderRelation}
import org.apache.spark.sql.types._

/**
 * Top command
 */
case class Top(count: Int, topOrBottom: Int, dim: NamedExpression, msr: NamedExpression,
    child: LogicalPlan) extends UnaryNode {
  def output: Seq[Attribute] = child.output

  override def references: AttributeSet = {
    val list = List(dim, msr)
    AttributeSet(list.flatMap(_.references))
  }
}

object getDB {

  def getDatabaseName(dbName: Option[String], sqlContext: SQLContext): String = {
    dbName.getOrElse(sqlContext.asInstanceOf[HiveContext].catalog.client.currentDatabase)
  }

}

/**
 * Shows Loads in a cube
 */
case class ShowLoadsCommand(schemaNameOp: Option[String], cube: String, limit: Option[String])
  extends LogicalPlan with Command {

  override def children: Seq[LogicalPlan] = Seq.empty

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("SegmentSequenceId", StringType, nullable = false)(),
      AttributeReference("Status", StringType, nullable = false)(),
      AttributeReference("Load Start Time", TimestampType, nullable = false)(),
      AttributeReference("Load End Time", TimestampType, nullable = false)())
  }
}

/**
 * Describe formatted for hive table
 */
case class DescribeFormattedCommand(sql: String, tblIdentifier: TableIdentifier)
  extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  override def output: Seq[AttributeReference] = {
    Seq(AttributeReference("col_name", StringType, nullable = false)(),
      AttributeReference("data_type", StringType, nullable = false)(),
      AttributeReference("comment", StringType, nullable = false)())
  }
}

case class CarbonDictionaryCatalystDecoder(
    relations: Seq[CarbonDecoderRelation],
    profile: CarbonProfile,
    aliasMap: CarbonAliasDecoderRelation,
    isOuter: Boolean,
    child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

abstract class CarbonProfile(attributes: Seq[Attribute]) extends Serializable {
  def isEmpty: Boolean = attributes.isEmpty
}

case class IncludeProfile(attributes: Seq[Attribute]) extends CarbonProfile(attributes)

case class ExcludeProfile(attributes: Seq[Attribute]) extends CarbonProfile(attributes)

case class FakeCarbonCast(child: Literal, dataType: DataType)
  extends LeafExpression with CodegenFallback {

  override def toString: String = s"FakeCarbonCast($child as ${ dataType.simpleString })"

  override def checkInputDataTypes(): TypeCheckResult = {
    TypeCheckResult.TypeCheckSuccess
  }

  override def nullable: Boolean = child.nullable

  override def eval(input: InternalRow): Any = child.value
}
