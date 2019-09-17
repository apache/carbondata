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

import java.lang.reflect.Method

import scala.reflect.runtime._
import scala.reflect.runtime.universe._

import org.apache.spark.{SPARK_VERSION, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.parser.AstBuilder
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.internal.HiveSerDe
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.StructField

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.hive.{CarbonHiveSerDe, MapredCarbonInputFormat, MapredCarbonOutputFormat}

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
    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      createObject(
        className,
        tableIdentifier,
        tableAlias)._1.asInstanceOf[UnresolvedRelation]
    } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
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
    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      createObject(
        className,
        alias.getOrElse(""),
        relation,
        Option(view))._1.asInstanceOf[SubqueryAlias]
    } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
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
    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
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
    } else if (SparkUtil.isSparkVersionXandAbove("2.2") ) {
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
      catalogTable: Option[CatalogTable],
      isStreaming: Boolean): LogicalRelation = {
    val className = "org.apache.spark.sql.execution.datasources.LogicalRelation"
    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      createObject(
        className,
        relation,
        Some(expectedOutputAttributes),
        catalogTable)._1.asInstanceOf[LogicalRelation]
    } else if (SparkUtil.isSparkVersionEqualTo("2.2")) {
      createObject(
        className,
        relation,
        expectedOutputAttributes,
        catalogTable)._1.asInstanceOf[LogicalRelation]
    } else if (SparkUtil.isSparkVersionEqualTo("2.3")) {
      createObject(
        className,
        relation,
        expectedOutputAttributes,
        catalogTable,
        isStreaming.asInstanceOf[Object])._1.asInstanceOf[LogicalRelation]
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
      CarbonCommonConstants.CARBON_SQLASTBUILDER_CLASSNAME_DEFAULT)
    createObject(className,
      conf,
      sqlParser, sparkSession)._1.asInstanceOf[AstBuilder]
  }

  def hasPredicateSubquery(filterExp: Expression) : Boolean = {
    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      val tuple = Class.forName("org.apache.spark.sql.catalyst.expressions.PredicateSubquery")
      val method = tuple.getMethod("hasPredicateSubquery", classOf[Expression])
      val hasSubquery : Boolean = method.invoke(tuple, filterExp).asInstanceOf[Boolean]
      hasSubquery
    } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
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
      rdd: RDD[InternalRow],
      partition: Partitioning,
      metadata: Map[String, String]): RowDataSourceScanExec = {
    val className = "org.apache.spark.sql.execution.RowDataSourceScanExec"
    if (SparkUtil.isSparkVersionEqualTo("2.1") || SparkUtil.isSparkVersionEqualTo("2.2")) {
      createObject(className, output, rdd, relation.relation,
        partition, metadata,
        relation.catalogTable.map(_.identifier))._1.asInstanceOf[RowDataSourceScanExec]
    } else if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      createObject(className, output, output.map(output.indexOf),
        pushedFilters.toSet, handledFilters.toSet, rdd,
        relation.relation,
        relation.catalogTable.map(_.identifier))._1.asInstanceOf[RowDataSourceScanExec]
    } else {
      throw new UnsupportedOperationException("Spark version not supported")
    }
  }

  def invokewriteAndReadMethod(dataSourceObj: DataSource,
      dataFrame: DataFrame,
      data: LogicalPlan,
      session: SparkSession,
      mode: SaveMode,
      query: LogicalPlan,
      physicalPlan: SparkPlan): BaseRelation = {
    if (SparkUtil.isSparkVersionEqualTo("2.2")) {
      val method: Method = dataSourceObj.getClass
        .getMethod("writeAndRead", classOf[SaveMode], classOf[DataFrame])
      method.invoke(dataSourceObj, mode, dataFrame)
        .asInstanceOf[BaseRelation]
    } else if (SparkUtil.isSparkVersionEqualTo("2.3")) {
      val method: Method = dataSourceObj.getClass
        .getMethod("writeAndRead",
          classOf[SaveMode],
          classOf[LogicalPlan],
          classOf[Seq[String]],
          classOf[SparkPlan])
      // since spark 2.3.2 version (SPARK-PR#22346),
      // change 'query.output' to 'query.output.map(_.name)'
      method.invoke(dataSourceObj, mode, query, query.output.map(_.name), physicalPlan)
        .asInstanceOf[BaseRelation]
    } else {
      throw new UnsupportedOperationException("Spark version not supported")
    }
  }

  /**
   * method to invoke alter table add columns for hive table from carbon session
   * @param table
   * @param colsToAdd
   * @return
   */
  def invokeAlterTableAddColumn(table: TableIdentifier,
      colsToAdd: Seq[StructField]): Object = {
    val caseClassName = "org.apache.spark.sql.execution.command.AlterTableAddColumnsCommand"
    CarbonReflectionUtils.createObject(caseClassName, table, colsToAdd)
      ._1.asInstanceOf[RunnableCommand]
  }

  def createSingleObject(className: String): Any = {
    val classMirror = universe.runtimeMirror(getClass.getClassLoader)
    val classTest = classMirror.staticModule(className)
    val methods = classMirror.reflectModule(classTest)
    methods.instance
  }

  def createObject(className: String, conArgs: Object*): (Any, Class[_]) = {
    val clazz = Utils.classForName(className)
    val ctor = clazz.getConstructors.head
    ctor.setAccessible(true)
    (ctor.newInstance(conArgs: _*), clazz)
  }

  def createObjectOfPrivateConstructor(className: String, conArgs: Object*): (Any, Class[_]) = {
    val clazz = Utils.classForName(className)
    val ctor = clazz.getDeclaredConstructors.head
    ctor.setAccessible(true)
    (ctor.newInstance(conArgs: _*), clazz)
  }

  /**
   * It is a hack to update the carbon input, output format information to #HiveSerDe
   */
  def updateCarbonSerdeInfo(): Unit = {
    val currentMirror = scala.reflect.runtime.currentMirror
    val instanceMirror = currentMirror.reflect(HiveSerDe)
    currentMirror.staticClass(HiveSerDe.getClass.getName).
      toType.members.find { p =>
      !p.isMethod && p.name.toString.equals("serdeMap")
    } match {
      case Some(field) =>
        val serdeMap =
          instanceMirror.reflectField(field.asTerm).get.asInstanceOf[Map[String, HiveSerDe]]
        val updatedSerdeMap =
          serdeMap ++ Map[String, HiveSerDe](
            ("org.apache.spark.sql.carbonsource", HiveSerDe(Some(
              classOf[MapredCarbonInputFormat].getName),
              Some(classOf[MapredCarbonOutputFormat[_]].getName),
              Some(classOf[CarbonHiveSerDe].getName))),
            ("carbon", HiveSerDe(Some(
              classOf[MapredCarbonInputFormat].getName),
              Some(classOf[MapredCarbonOutputFormat[_]].getName),
              Some(classOf[CarbonHiveSerDe].getName))),
            ("carbondata", HiveSerDe(Some(
              classOf[MapredCarbonInputFormat].getName),
              Some(classOf[MapredCarbonOutputFormat[_]].getName),
              Some(classOf[CarbonHiveSerDe].getName))))
        instanceMirror.reflectField(field.asTerm).set(updatedSerdeMap)
      case _ =>
    }
  }

  /**
   * This method updates the field of case class through reflection.
   */
  def setFieldToCaseClass(caseObj: Object, fieldName: String, objToSet: Object): Unit = {
    val nameField = caseObj.getClass.getDeclaredField(fieldName)
    nameField.setAccessible(true)
    nameField.set(caseObj, objToSet)
  }

  def invokeAnalyzerExecute(analyzer: Analyzer,
      plan: LogicalPlan): LogicalPlan = {
    if (SparkUtil.isSparkVersionEqualTo("2.1") || SparkUtil.isSparkVersionEqualTo("2.2")) {
      val method: Method = analyzer.getClass
        .getMethod("execute", classOf[LogicalPlan])
      method.invoke(analyzer, plan).asInstanceOf[LogicalPlan]
    } else if (SparkUtil.isSparkVersionEqualTo("2.3")) {
      val method: Method = analyzer.getClass
        .getMethod("executeAndCheck", classOf[LogicalPlan])
      method.invoke(analyzer, plan).asInstanceOf[LogicalPlan]
    } else {
      throw new UnsupportedOperationException("Spark version not supported")
    }
  }
}
