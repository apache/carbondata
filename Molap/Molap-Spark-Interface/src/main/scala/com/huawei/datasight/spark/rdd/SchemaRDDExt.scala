/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.huawei.datasight.spark.rdd

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.BinaryNode
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.OlapContext
import org.apache.spark.util.BoundedPriorityQueue
import com.huawei.unibi.molap.engine.executer.impl.topn.TopNModel
import org.apache.spark.sql.Top
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar

/*
 * TODO: Check the possibility of using implicit to convert any SchemaRDD into Cube RDD
 * */
class SchemaRDDExt(sqlContext: SQLContext,
                   logicalPlan: LogicalPlan) extends SchemaRDD(sqlContext, logicalPlan) {
  /**
    * Changes the output of this relation to the given expressions, similar to the `SELECT * , c1` clause
    * in SQL.
    *
    * {{{
    *   schemaRDD.addColumn('b + 'c).addColumn( 'd as 'aliasedName)
    * }}}
    *
    * @param exprs a set of logical expression that will be evaluated for each input row.
    * @group Query
    */
  def addColumnDirect(expr: NamedExpression): SchemaRDD = {
    new SchemaRDDExt(sqlContext, Project(List(UnresolvedStar(None), expr), logicalPlan))
  }

  /**
    * Adds a new column to current RDD, also allowing UDFs to be written while adding column
    * in SQL.
    *
    * {{{
    *   schemaRDD.addColumn('b + 'c).addColumn( 'd as 'aliasedName)
    * }}}
    *
    * @param exprs a set of logical expression that will be evaluated for each input row.
    * @group Query
    */
  def addColumn(detailedExprs: Expression): SchemaRDD = {
    val aliasedExprs = detailedExprs match {
      case ne: NamedExpression => ne
      case e => Alias(e, e.toString)()
    }
    new SchemaRDDExt(sqlContext, Project(List(UnresolvedStar(None), aliasedExprs), logicalPlan))
  }

  /**
    *
    */
  def topN(count: Int, groupDimExpr: NamedExpression = null, msrExpr: NamedExpression): SchemaRDD = {

    new SchemaRDDExt(sqlContext, Top(count, 0, groupDimExpr, msrExpr, logicalPlan))
  }

  def bottomN(count: Int, groupDimExpr: NamedExpression = null, msrExpr: NamedExpression): SchemaRDD = {

    new SchemaRDDExt(sqlContext, Top(count, 1, groupDimExpr, msrExpr, logicalPlan))
  }

  /** Adds column by joining the current RDD with other RDD. Column can be selected from current or other RDD.
    * {{{
    *   curr.addColumnByJoin(dataset("ds1"), "curr.Territory".attr === "ds1.Territory".attr )("ds1.Quantity".attr)";
    *   		or
    *   curr.addColumnByJoin("ds1", "curr.Territory".attr === "ds1.Territory".attr )("ds1.Quantity".attr as 'col1)";
    * }}}
    *
    * @param otherPlan     : other Schema RDD
    * @param expr          : Join condition Expression
    * @param joinType      : Default to Inner
    * @param detailedExprs : source column to be added after join. Column can be selected from current or other RDD.
    * @return Joined RDD with column added
    */
  def addColumnByJoin(
      otherPlan: SchemaRDDExt,
      expr: Expression,
      joinType: JoinType = Inner)(detailedExprs: Expression): SchemaRDD = {

    val aliasedExprs = detailedExprs match {
      case ne: NamedExpression => ne
      case e => Alias(e, e.toString)()
    }
    new SchemaRDDExt(sqlContext, Project(List(UnresolvedStar(Some("curr")), aliasedExprs), Join(logicalPlan, otherPlan.logicalPlan, joinType, Some(expr))))
  }


  /** Filters current RDD by applying filter condition using join with other RDD.
    * {{{
    *   curr.filterByJoin(dataset("ds1"), "curr.Territory".attr === "ds1.Territory".attr )";
    *   		or
    *   curr.filterByJoin("ds1", "curr.Territory".attr === "ds1.Territory".attr )";
    * }}}
    *
    * @param otherPlan : other Schema RDD
    * @param expr      : Join filter condition
    * @param joinType  : Default to Inner
    * @return RDD
    */
  def filterByJoin(
      otherPlan: SchemaRDDExt,
      expr: Expression,
      joinType: JoinType = Inner): SchemaRDD = {

    new SchemaRDDExt(sqlContext, Project(List(UnresolvedStar(Some("curr"))), Join(logicalPlan, otherPlan.logicalPlan, joinType, Some(expr))))
  }

}
 
