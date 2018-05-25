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

package org.apache.spark.util

import org.antlr.v4.runtime.tree.TerminalNode

import scala.reflect.runtime._
import scala.reflect.runtime.universe._
import org.apache.spark.{SPARK_VERSION, SecurityManager, SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.parser.{AstBuilder, SqlBaseParser}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.rpc.RpcEnvConfig
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.{CreateFileFormatContext, CreateHiveTableContext}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.RowDataSourceScanExec

/**
 * Reflection APIs
 */

object CarbonReflectionUtils {
  private val rm = universe.runtimeMirror(getClass.getClassLoader)

  /**
   * Returns the field val from a object through reflection.
   * @param name - name of the field being retrieved.
   * @param obj - Object from which the field has to be retrieved.
   * @tparam T
   * @return
   */
  def getField[T: TypeTag : reflect.ClassTag](name: String, obj: T): Any = {
    val im = rm.reflect(obj)

    im.symbol.typeSignature.members.find(_.name.toString.equals(name))
      .map(l => im.reflectField(l.asTerm).get).getOrElse(null)
  }

  def getUnresolvedRelation(
      tableIdentifier: TableIdentifier,
      tableAlias: Option[String] = None): UnresolvedRelation = {
    val className = "org.apache.spark.sql.catalyst.analysis.UnresolvedRelation"
    if (SPARK_VERSION.startsWith("2.1")) {
      createObject(
        className,
        tableIdentifier,
        tableAlias)._1.asInstanceOf[UnresolvedRelation]
    } else if (SPARK_VERSION.startsWith("2.2")) {
      createObject(
        className,
        tableIdentifier)._1.asInstanceOf[UnresolvedRelation]
    } else {
      throw new UnsupportedOperationException(s"Unsupported Spark version $SPARK_VERSION")
    }
  }

  def getSubqueryAlias(sparkSession: SparkSession, alias: Option[String],
      relation: LogicalPlan,
      view: Option[TableIdentifier]): SubqueryAlias = {
    val className = "org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias"
    if (SPARK_VERSION.startsWith("2.1")) {
      createObject(
        className,
        alias.getOrElse(""),
        relation,
        Option(view))._1.asInstanceOf[SubqueryAlias]
    } else if (SPARK_VERSION.startsWith("2.2")) {
      createObject(
        className,
        alias.getOrElse(""),
        relation)._1.asInstanceOf[SubqueryAlias]
    } else {
      throw new UnsupportedOperationException("Unsupported Spark version")
    }
  }

  def getInsertIntoCommand(table: LogicalPlan,
      partition: Map[String, Option[String]],
      query: LogicalPlan,
      overwrite: Boolean,
      ifPartitionNotExists: Boolean): InsertIntoTable = {
    val className = "org.apache.spark.sql.catalyst.plans.logical.InsertIntoTable"
    if (SPARK_VERSION.startsWith("2.1")) {
      val overwriteOptions = createObject(
        "org.apache.spark.sql.catalyst.plans.logical.OverwriteOptions",
        overwrite.asInstanceOf[Object], Map.empty.asInstanceOf[Object])._1.asInstanceOf[Object]
      createObject(
        className,
        table,
        partition,
        query,
        overwriteOptions,
        ifPartitionNotExists.asInstanceOf[Object])._1.asInstanceOf[InsertIntoTable]
    } else if (SPARK_VERSION.startsWith("2.2")) {
      createObject(
        className,
        table,
        partition,
        query,
        overwrite.asInstanceOf[Object],
        ifPartitionNotExists.asInstanceOf[Object])._1.asInstanceOf[InsertIntoTable]
    } else {
      throw new UnsupportedOperationException("Unsupported Spark version")
    }
  }

  def getLogicalRelation(relation: BaseRelation,
      expectedOutputAttributes: Seq[Attribute],
      catalogTable: Option[CatalogTable]): LogicalRelation = {
    val className = "org.apache.spark.sql.execution.datasources.LogicalRelation"
    if (SPARK_VERSION.startsWith("2.1")) {
      createObject(
        className,
        relation,
        Some(expectedOutputAttributes),
        catalogTable)._1.asInstanceOf[LogicalRelation]
    } else if (SPARK_VERSION.startsWith("2.2")) {
      createObject(
        className,
        relation,
        expectedOutputAttributes,
        catalogTable)._1.asInstanceOf[LogicalRelation]
    } else {
      throw new UnsupportedOperationException("Unsupported Spark version")
    }
  }


  def getOverWriteOption[T: TypeTag : reflect.ClassTag](name: String, obj: T): Boolean = {
    var overwriteboolean: Boolean = false
    val im = rm.reflect(obj)
    for (m <- typeOf[T].members.filter(!_.isMethod)) {
      if (m.toString.contains("overwrite")) {
        val typ = m.typeSignature
        if (typ.toString.contains("Boolean")) {
          // Spark2.2
          overwriteboolean = im.reflectField(m.asTerm).get.asInstanceOf[Boolean]
        } else {
          overwriteboolean = getOverWrite("enabled", im.reflectField(m.asTerm).get)
        }
      }
    }
    overwriteboolean
  }

  private def getOverWrite[T: TypeTag : reflect.ClassTag](name: String, obj: T): Boolean = {
    var overwriteboolean: Boolean = false
    val im = rm.reflect(obj)
    for (l <- im.symbol.typeSignature.members.filter(_.name.toString.contains("enabled"))) {
      overwriteboolean = im.reflectField(l.asTerm).get.asInstanceOf[Boolean]
    }
    overwriteboolean
  }

  def getFieldOfCatalogTable[T: TypeTag : reflect.ClassTag](name: String, obj: T): Any = {
    val im = rm.reflect(obj)
    val sym = im.symbol.typeSignature.member(TermName(name))
    val tableMeta = im.reflectMethod(sym.asMethod).apply()
    tableMeta
  }

  def getAstBuilder(conf: Object,
      sqlParser: Object,
      sparkSession: SparkSession): AstBuilder = {
    val className = sparkSession.sparkContext.conf.get(
      CarbonCommonConstants.CARBON_SQLASTBUILDER_CLASSNAME,
      "org.apache.spark.sql.hive.CarbonSqlAstBuilder")
    createObject(className,
      conf,
      sqlParser, sparkSession)._1.asInstanceOf[AstBuilder]
  }

  def getSessionState(sparkContext: SparkContext,
      carbonSession: Object,
      useHiveMetaStore: Boolean): Any = {
    if (SPARK_VERSION.startsWith("2.1")) {
      val className = sparkContext.conf.get(
        CarbonCommonConstants.CARBON_SESSIONSTATE_CLASSNAME,
        "org.apache.spark.sql.hive.CarbonSessionState")
      createObject(className, carbonSession)._1
    } else if (SPARK_VERSION.startsWith("2.2") || SPARK_VERSION.startsWith("2.3")) {
      if (useHiveMetaStore) {
        val className = sparkContext.conf.get(
          CarbonCommonConstants.CARBON_SESSIONSTATE_CLASSNAME,
          "org.apache.spark.sql.hive.CarbonSessionStateBuilder")
        val tuple = createObject(className, carbonSession, None)
        val method = tuple._2.getMethod("build")
        method.invoke(tuple._1)
      } else {
        val className = sparkContext.conf.get(
          CarbonCommonConstants.CARBON_SESSIONSTATE_CLASSNAME,
          "org.apache.spark.sql.hive.CarbonInMemorySessionStateBuilder")
        val tuple = createObject(className, carbonSession, None)
        val method = tuple._2.getMethod("build")
        method.invoke(tuple._1)
      }
    } else {
      throw new UnsupportedOperationException("Spark version not supported")
    }
  }

  def hasPredicateSubquery(filterExp: Expression) : Boolean = {
    if (SPARK_VERSION.startsWith("2.1")) {
      val tuple = Class.forName("org.apache.spark.sql.catalyst.expressions.PredicateSubquery")
      val method = tuple.getMethod("hasPredicateSubquery", classOf[Expression])
      val hasSubquery : Boolean = method.invoke(tuple, filterExp).asInstanceOf[Boolean]
      hasSubquery
    } else if (SPARK_VERSION.startsWith("2.2")) {
      val tuple = Class.forName("org.apache.spark.sql.catalyst.expressions.SubqueryExpression")
      val method = tuple.getMethod("hasInOrExistsSubquery", classOf[Expression])
      val hasSubquery : Boolean = method.invoke(tuple, filterExp).asInstanceOf[Boolean]
      hasSubquery
    } else {
      throw new UnsupportedOperationException("Spark version not supported")
    }
  }

  def getDescribeTableFormattedField[T: TypeTag : reflect.ClassTag](obj: T): Boolean = {
    val im = rm.reflect(obj)
    val isFormatted = im.symbol.typeSignature.members
      .find(_.name.toString.equalsIgnoreCase("isFormatted"))
      .map(l => im.reflectField(l.asTerm).get).getOrElse("false").asInstanceOf[Boolean]
    isFormatted
  }



  def getRowDataSourceScanExecObj(relation: LogicalRelation,
                                  output: Seq[Attribute],
                                  pushedFilters: Seq[Filter],
                                  handledFilters: Seq[Filter],
                                  updateRequestedColumns: Seq[Attribute],
                                  rdd: RDD[InternalRow],
                                  partition : Partitioning,
                                  metadata: Map[String, String]): RowDataSourceScanExec = {
    val className = "org.apache.spark.sql.execution.RowDataSourceScanExec"
    if (SPARK_VERSION.startsWith("2.1") || SPARK_VERSION.startsWith("2.2")) {
      createObject(className, output, rdd, relation.relation,
        partition, metadata,
        relation.catalogTable.map(_.identifier))._1.asInstanceOf[RowDataSourceScanExec]

    } else if (SPARK_VERSION.startsWith("2.3")) {
      createObject(className,output, updateRequestedColumns.map(output.indexOf),
        pushedFilters.toSet, handledFilters.toSet,  rdd,
        relation.relation,
        relation.catalogTable.map(_.identifier))._1.asInstanceOf[RowDataSourceScanExec]

    } else {
      throw new UnsupportedOperationException("Spark version not supported")
    }

  }

  def createObject(className: String, conArgs: Object*): (Any, Class[_]) = {
    val clazz = Utils.classForName(className)
    val ctor = clazz.getConstructors.head
    ctor.setAccessible(true)
    (ctor.newInstance(conArgs: _*), clazz)
  }

}
