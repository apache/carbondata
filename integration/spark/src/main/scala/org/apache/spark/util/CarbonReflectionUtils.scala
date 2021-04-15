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

import scala.reflect.runtime._
import scala.reflect.runtime.universe._

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.{SparkPlan, SparkSqlAstBuilder}
import org.apache.spark.sql.execution.command.AlterTableAddColumnsCommand
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.internal.HiveSerDe
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructField

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.hive.{CarbonFileHiveSerDe, CarbonHiveSerDe, MapredCarbonInputFormat, MapredCarbonOutputFormat}

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
      .map(l => im.reflectField(l.asTerm).get).orNull
  }

  def getUnresolvedRelation(
      tableIdentifier: TableIdentifier,
      tableAlias: Option[String] = None): UnresolvedRelation = {
    UnresolvedRelation(tableIdentifier)
  }

  def getSubqueryAlias(sparkSession: SparkSession, alias: Option[String],
      relation: LogicalPlan,
      view: Option[TableIdentifier]): SubqueryAlias = {
    SubqueryAlias(alias.getOrElse(""), relation)
  }

  def getLogicalRelation(relation: BaseRelation,
      expectedOutputAttributes: Seq[Attribute],
      catalogTable: Option[CatalogTable],
      isStreaming: Boolean): LogicalRelation = {
    new LogicalRelation(
      relation,
      expectedOutputAttributes.asInstanceOf[Seq[AttributeReference]],
      catalogTable,
      isStreaming)
  }


  def getOverWriteOption[T: TypeTag : reflect.ClassTag](name: String, obj: T): Boolean = {
    var isOverwriteBoolean: Boolean = false
    val im = rm.reflect(obj)
    for (m <- typeOf[T].members.filter(!_.isMethod)) {
      if (m.toString.contains("overwrite")) {
        val typ = m.typeSignature
        if (typ.toString.contains("Boolean")) {
          // Spark2.2
          isOverwriteBoolean = im.reflectField(m.asTerm).get.asInstanceOf[Boolean]
        } else {
          isOverwriteBoolean = getOverWrite("enabled", im.reflectField(m.asTerm).get)
        }
      }
    }
    isOverwriteBoolean
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
      sparkSession: SparkSession): SparkSqlAstBuilder = {
    val className = sparkSession.sparkContext.conf.get(
      CarbonCommonConstants.CARBON_SQLASTBUILDER_CLASSNAME,
      CarbonCommonConstants.CARBON_SQLASTBUILDER_CLASSNAME_DEFAULT)
    createObject(className,
      conf,
      sqlParser, sparkSession)._1.asInstanceOf[SparkSqlAstBuilder]
  }

  def getSessionState(sparkContext: SparkContext,
                      carbonSession: Object,
                      useHiveMetaStore: Boolean): Any = {
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
  }

  def hasPredicateSubquery(filterExp: Expression) : Boolean = {
    val tuple = Class.forName("org.apache.spark.sql.catalyst.expressions.SubqueryExpression")
    val method = tuple.getMethod("hasInOrExistsSubquery", classOf[Expression])
    val hasSubquery : Boolean = method.invoke(tuple, filterExp).asInstanceOf[Boolean]
    hasSubquery
  }

  def getDescribeTableFormattedField[T: TypeTag : reflect.ClassTag](obj: T): Boolean = {
    val im = rm.reflect(obj)
    val isFormatted = im.symbol.typeSignature.members
      .find(_.name.toString.equalsIgnoreCase("isFormatted"))
      .map(l => im.reflectField(l.asTerm).get).getOrElse("false").asInstanceOf[Boolean]
    isFormatted
  }

  def invokeWriteAndReadMethod(dataSourceObj: DataSource,
      dataFrame: DataFrame,
      data: LogicalPlan,
      session: SparkSession,
      mode: SaveMode,
      query: LogicalPlan,
      physicalPlan: SparkPlan): BaseRelation = {
    dataSourceObj.writeAndRead(mode, query, query.output.map(_.name), physicalPlan)
  }

  /**
   * method to invoke alter table add columns for hive table from carbon session
   * @param table
   * @param colsToAdd
   * @return
   */
  def invokeAlterTableAddColumn(table: TableIdentifier,
      colsToAdd: Seq[StructField]): Object = {
    AlterTableAddColumnsCommand(table, colsToAdd)
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
              Some(classOf[CarbonFileHiveSerDe].getName))),
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


  /**
   * This method updates the field of case class through reflection.
   */
  def setSuperFieldToClass(caseObj: Object, fieldName: String, objToSet: Object): Unit = {
    val nameField = caseObj.getClass.getSuperclass.getDeclaredField(fieldName)
    nameField.setAccessible(true)
    nameField.set(caseObj, objToSet)
  }

}
