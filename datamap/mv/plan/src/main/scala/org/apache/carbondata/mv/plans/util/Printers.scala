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

package org.apache.carbondata.mv.plans.util

import java.io.{OutputStream, PrintWriter, StringWriter}

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, _}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.util.quoteIdentifier

import org.apache.carbondata.mv.plans.modular.Flags._
import org.apache.carbondata.mv.plans.util.SQLBuildDSL._

// scalastyle:off println
trait Printers {

  abstract class FragmentPrinter(out: PrintWriter) {
    protected var indentMargin = 0
    protected val indentStep = 2
    protected var indentString = "                                        " // 40

    def indent(): Unit = indentMargin += indentStep

    def undent(): Unit = indentMargin -= indentStep

    def println(): Unit = {
      out.println()
      while (indentMargin > indentString.length()) {
        indentString += indentString
      }
      if (indentMargin > 0) {
        out.write(indentString, 0, indentMargin)
      }
    }

    def printSeq[a](ls: List[a])(printelem: a => Unit)(printsep: => Unit) {
      ls match {
        case List() =>
        case List(x) => printelem(x)
        case x :: rest => printelem(x); printsep; printSeq(rest)(printelem)(printsep)
      }
    }

    def printColumn(ts: List[Fragment], start: String, sep: String, end: String) {
      print(start)
      indent
      println()
      printSeq(ts) { print(_) } { print(sep); println() }
      undent
      println()
      print(end)
    }

    def printRow(ts: List[Fragment], start: String, sep: String, end: String) {
      print(start)
      printSeq(ts) { print(_) } { print(sep) }
      print(end)
    }

    def printRow(ts: List[Fragment], sep: String) { printRow(ts, "", sep, "") }

    def printFragment(frag: Fragment): Unit

    def print(args: Any*): Unit = {
      args foreach {
        case frag: Fragment =>
          printFragment(frag)
        case arg =>
          out.print(if (arg == null) {
            "null"
          } else {
            arg.toString
          })
      }
    }
  }

  /**
   * A sql fragment printer which is stingy about vertical whitespace and unnecessary
   * punctuation.
   */
  class SQLFragmentCompactPrinter(out: PrintWriter) extends FragmentPrinter(out) {

    object Position extends Enumeration {
      type Position = Value
      val Select, From, GroupBy, Where, Sort, Limit = Value
    }

    import Position._

    def printSelect(select: Seq[NamedExpression], flags: FlagSet): Unit = {
      if (flags.hasFlag(DISTINCT)) {
        print("SELECT DISTINCT %s "
          .format(select.map(_.sql) mkString ", "))
      } else {
        print("SELECT %s ".format(select.map(_.sql) mkString ", "))
      }
    }

    def printFromElem(f: (SQLBuildDSL.Fragment, Option[JoinType], Seq[Expression])): Unit = {
      f._2 map { t => print("%s JOIN ".format(t.sql)) }
      printFragment(f._1)
      if (f._3.nonEmpty) {
        print(" ON %s".format(f._3.map(_.sql).mkString(" AND ")))
      }
    }

    def printFrom(
        from: Seq[(SQLBuildDSL.Fragment, Option[JoinType], Seq[Expression])]): Seq[Unit] = {
      def fromIndented(
          x: (SQLBuildDSL.Fragment, Option[JoinType], Seq[Expression])): Unit = {
        indent
        println()
        printFromElem(x)
        undent
      }

      print("FROM")
      from map fromIndented
    }

    def printWhere(where: Seq[Expression]): Unit = {
      if (where != Nil) {
        print("WHERE")
        //        if (where.nonEmpty) {
        //          val condition = where(0)
        //          val dt = condition.dataType
        //          val t = condition.sql
        //          print(t)
        //        }
        indent
        println()
        print("%s".format(where.map(_.sql).mkString(" AND ")))
        undent
      }
    }

    def printGroupby(groupby: (Seq[Expression], Seq[Seq[Expression]])): Unit = {

      if (groupby._1.nonEmpty) {
        print("GROUP BY %s".format(groupby._1.map(_.sql).mkString(", ")))
        if (groupby._2.nonEmpty) {
          print(" GROUPING SETS(%s)"
            .format(groupby._2.map(e => s"(${ e.map(_.sql).mkString(", ") })").mkString(", ")))
        }
      }
    }

    def printHaving(having: Seq[Expression]): Unit = {
      if (having != Nil) {
        print("HAVING %s".format(having.map(_.sql).mkString(" AND ")))
      }
    }

    def printUnion(sqls: Seq[Fragment]): Unit = {
      sqls match {
        case sql1 :: sql2 :: rest =>
          printFragment(sql1)
          println()
          print("UNION ALL")
          println()
          printUnion(sql2 :: rest)
        case sql :: Nil => printFragment(sql)
      }
    }

    def printTable(name: Seq[String]): Unit = {
      print("%s".format(name.last))
    }

    trait ExprSeq extends Seq[SortOrder]

    def printExprModifiers(modifiers: (FlagSet, Seq[Seq[Any]]), pos: Position): Unit = {
      val flagsNeedExprs = for {flag <- pickledListOrder
                                if (modifiers._1.hasFlag(flag))} yield {
        (flag)
      }

      flagsNeedExprs.zip(modifiers._2).foreach {
        case (SORT, Seq(order)) =>
          if (pos == Sort) { // TODO: how to verify type of order:  && order
            // .isInstanceOf[Seq[SortOrder]]) {
            println()
            if (modifiers._1.hasFlag(GLOBAL)) {
              print("ORDER BY %s"
                .format(order.asInstanceOf[Seq[SortOrder]].map {
                  // walk around for a sparkSQL bug
                  s => {
                         s.child match {
                           case a: Alias =>
                             val qualifierPrefix = a.qualifier
                               .map(_ + ".").getOrElse("")
                             s"$qualifierPrefix${
                               quoteIdentifier(a
                                 .name)
                             }"

                           case other => other.sql
                         }
                       } + " " + s.direction.sql + " " + s.nullOrdering.sql
                }.mkString(", ")))
            } else {
              print("SORT BY %s".format(order.asInstanceOf[Seq[SortOrder]].map {
                // walk around for a sparkSQL bug
                s => {
                       s.child match {
                         case a: Alias =>
                           val qualifierPrefix = a.qualifier.map(_ + ".")
                             .getOrElse("")
                           s"$qualifierPrefix${ quoteIdentifier(a.name) }"

                         case other => other.sql
                       }
                     } + " " + s.direction.sql + " " + s.nullOrdering.sql
              }.mkString(", ")))
            }
          }
        case (LIMIT, Seq(limitExpr)) =>
          if (pos == Limit && limitExpr.isInstanceOf[Expression]) {
            println()
            print(s"LIMIT ${ limitExpr.asInstanceOf[Expression].sql }")
          }
        case (EXPAND, Seq(_, _)) =>
        case _ =>
          throw new UnsupportedOperationException(s"unsupported modifiers: ${
            flagToString(modifiers
              ._1)
          }")
      }
    }

    override def printFragment(frag: Fragment): Unit = {
      require(
        frag.Supported,
        "Fragment is not supported.  Current frag:\n" + frag)

      frag match {
        case spjg@SPJGFragment(select, from, where, groupby, having, alias, modifiers) =>
          if (alias.nonEmpty) {
            print("(")
          }
          printSelect(select, modifiers._1)
          if (from.nonEmpty) {
            println()
          }
          printFrom(from)
          if (where.nonEmpty) {
            println()
          }
          printWhere(where)
          if (groupby._1.nonEmpty) {
            println()
          }
          printGroupby(groupby)
          if (having.nonEmpty) {
            println()
          }
          printHaving(having)
          if (modifiers._1.hasFlag(SORT)) {
            printExprModifiers(modifiers, Sort)
          }
          if (modifiers._1.hasFlag(LIMIT)) {
            printExprModifiers(modifiers, Limit)
          }
          alias.map { case a: String => print(") %s ".format(a)) }
        case union@UNIONFragment(sqls, alias, modifiers) =>
          if (alias.nonEmpty) {
            print("(")
          }
          printUnion(sqls)
          if (modifiers._1.hasFlag(LIMIT)) {
            printExprModifiers(modifiers, Limit)
          }
          alias.map { case a: String => print(") %s ".format(a)) }
        case table@TABLEFragment(name, alias, modifiers) =>
          printTable(name)
          alias.map { case a: String =>
            if (!(a == name.last)) {
              print(" %s ".format(a))
            }
          }
        case _ =>
          throw new UnsupportedOperationException(s"unsupported plan $frag")
      }
    }
  }

  /**
   * A sql fragment printer which is one single line for a fragment.
   */
  class SQLFragmentOneLinePrinter(out: PrintWriter) extends SQLFragmentCompactPrinter(out) {
    protected val sep = "  "

    override def println() { print(sep) }
  }

  /** @group Printers */
  protected def render(what: Any, mkPrinter: PrintWriter => FragmentPrinter): String = {
    val buffer = new StringWriter()
    val writer = new PrintWriter(buffer)
    var printer = mkPrinter(writer)

    printer.print(what)
    writer.flush()
    buffer.toString
  }

  def asCompactString(t: Fragment): String = render(t, newSQLFragmentCompactPrinter)

  def asOneLineString(t: Fragment): String = render(t, newSQLFragmentOneLinePrinter)

  def newSQLFragmentCompactPrinter(writer: PrintWriter): SQLFragmentCompactPrinter = {
    new SQLFragmentCompactPrinter(
      writer)
  }

  def newSQLFragmentCompactPrinter(stream: OutputStream): SQLFragmentCompactPrinter = {
    newSQLFragmentCompactPrinter(
      new PrintWriter(stream))
  }

  def newSQLFragmentOneLinePrinter(writer: PrintWriter): SQLFragmentOneLinePrinter = {
    new SQLFragmentOneLinePrinter(
      writer)
  }

  def newSQLFragmentOneLinePrinter(stream: OutputStream): SQLFragmentOneLinePrinter = {
    newSQLFragmentOneLinePrinter(
      new PrintWriter(stream))
  }
}
// scalastyle:on println
