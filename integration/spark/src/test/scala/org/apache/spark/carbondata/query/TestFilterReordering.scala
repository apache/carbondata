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

package org.apache.spark.carbondata.query

import java.util

import org.apache.spark.sql.{CarbonEnv, CarbonThreadUtil}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.scan.expression.{ColumnExpression, Expression, LiteralExpression}
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression
import org.apache.carbondata.core.scan.expression.logical.{AndExpression, OrExpression}
import org.apache.carbondata.core.scan.expression.optimize.ExpressionOptimizer

class TestFilterReordering extends QueryTest with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    sql("drop table if exists filter_reorder")
    sql("create table filter_reorder(one string, two string, three string, four int, " +
      "five int) stored as carbondata")
  }

  test("Test filter reorder with various conditions") {
    checkOptimizer("(four = 11 and two = 11) or (one = 11)",
      "(one = 11 or (two = 11 and four = 11))")
    checkOptimizer("(four = 11 or two = 11) or (one = 11 or (five = 11 or three = 11))",
      "((((one = 11 or two = 11) or three = 11) or four = 11) or five = 11)")
    checkOptimizer(
      "(four = 11 or two = 11) or (one = 11 or (five = 11 or (three = 11 and three = 11)))",
      "((((one = 11 or two = 11) or (three = 11 and three = 11)) or four = 11) or five = 11)")
  }

  test("test disabling filter reordering") {
    sqlContext.sparkSession.sql(s"set ${ CarbonCommonConstants.CARBON_REORDER_FILTER }=false")
    CarbonThreadUtil.updateSessionInfoToCurrentThread(sqlContext.sparkSession)
    checkOptimizer("(four = 11 and two = 11) or (one = 11)",
      "((four = 11 and two = 11) or one = 11)")
    sqlContext.sparkSession.sql(s"set ${ CarbonCommonConstants.CARBON_REORDER_FILTER }=true")
  }

  override protected def afterAll(): Unit = {
    sqlContext.sparkSession.sql(s"set ${ CarbonCommonConstants.CARBON_REORDER_FILTER }=true")
    CarbonThreadUtil.updateSessionInfoToCurrentThread(sqlContext.sparkSession)
    sql("drop table if exists filter_reorder")
  }

  private def checkOptimizer(oldFilter: String, newFilter: String): Unit = {
    val table = CarbonEnv.getCarbonTable(None, "filter_reorder")(sqlContext.sparkSession)
    assertResult(newFilter)(
      ExpressionOptimizer.optimize(table, translate(oldFilter)).getStatement)
  }

  private def translate(expressionText: String): Expression = {
    val data = new util.Stack[Object]()
    val operation = new util.Stack[String]()
    val builder = new StringBuilder()

    def popExpression(op: String): Unit = {
      val expression = op match {
        case "=" =>
          val literal = new LiteralExpression(data.pop().toString, null)
          val column = new ColumnExpression(data.pop().toString, null)
          new EqualToExpression(column, literal)
        case "and" =>
          val right = data.pop().asInstanceOf[Expression]
          val left = data.pop().asInstanceOf[Expression]
          new AndExpression(left, right)
        case "or" =>
          val right = data.pop().asInstanceOf[Expression]
          val left = data.pop().asInstanceOf[Expression]
          new OrExpression(left, right)
      }
      data.push(expression)
    }

    def popMultiExpression(): Unit = {
      var op = operation.pop()
      while (!"(".equalsIgnoreCase(op)) {
        popExpression(op)
        op = operation.pop()
      }
    }

    expressionText.toCharArray.foreach {
      case ' ' =>
        if (builder.nonEmpty) {
          val cell = builder.toString()
          builder.clear()
          if ("and".equalsIgnoreCase(cell) || "or".equalsIgnoreCase(cell)) {
            operation.push(cell)
          } else {
            data.push(cell)
            if ("11".equalsIgnoreCase(cell)) {
              popExpression(operation.pop())
            }
          }
        }
      case '(' => operation.push("(")
      case ')' =>
        if (builder.nonEmpty) {
          data.push(builder.toString())
          builder.clear()
        }
        popMultiExpression()
      case '=' => operation.push("=")
      case c => builder.append(c)
    }
    while (!operation.isEmpty) {
      popExpression(operation.pop())
    }
    data.pop().asInstanceOf[Expression]
  }
}
