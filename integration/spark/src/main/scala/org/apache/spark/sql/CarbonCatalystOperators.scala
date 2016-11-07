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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{UnaryNode, _}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.optimizer.CarbonDecoderRelation
import org.apache.spark.sql.types._

import org.apache.carbondata.spark.CarbonAliasDecoderRelation

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
 * Shows Loads in a table
 */
case class ShowLoadsCommand(databaseNameOp: Option[String], table: String, limit: Option[String])
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

  override def output: Seq[AttributeReference] =
    Seq(AttributeReference("result", StringType, nullable = false)())
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

case class CreateDatabase(dbName: String, sql: String) extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty
  override def output: Seq[AttributeReference] = {
    Seq()
  }
}

case class DropDatabase(dbName: String, isCascade: Boolean, sql: String)
    extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty
  override def output: Seq[AttributeReference] = {
    Seq()
  }
}

/**
<<<<<<< 28eb96f84af708e273d5dcab0859952c08f5aac9
 * A logical plan representing insertion into Hive table.
 * This plan ignores nullability of ArrayType, MapType, StructType unlike InsertIntoTable
 * because Hive table doesn't have nullability for ARRAY, MAP, STRUCT types.
 */
case class InsertIntoCarbonTable(
    table: CarbonDatasourceRelation,
    partition: Map[String, Option[String]],
    child: LogicalPlan,
    overwrite: Boolean,
    ifNotExists: Boolean)
  extends LogicalPlan with Command {

  override def children: Seq[LogicalPlan] = child :: Nil
  override def output: Seq[Attribute] = Seq.empty

  // This is the expected schema of the table prepared to be inserted into,
  // including dynamic partition columns.
  val tableOutput = table.carbonRelation.output

}

/**
 * Here the syntax is 'CREATE TABLE target LIKE src'.
 *
 * @param likeTableName the src table's name, used to create target table, should check
 *                      whether the src table is carbon table.
 * @param createHiveLikeTableSql the sql string only used to create hive target table.
 */
case class CreateLikeTable(likeTableName: String, createHiveLikeTableSql: String)
  extends LogicalPlan with Command {

  override def children: Seq[LogicalPlan] = Seq.empty

  override def output: Seq[Attribute] = Seq.empty

}
