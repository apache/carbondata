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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser.AstBuilder
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants

/**
 * Reflection APIs
 */

object CarbonReflectionUtils {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

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
      version: String,
      tableAlias: Option[String] = None): UnresolvedRelation = {
    val className = "org.apache.spark.sql.catalyst.analysis.UnresolvedRelation"
    if (version.startsWith("2.1")) {
      createObject(
        className,
        tableIdentifier,
        tableAlias)._1.asInstanceOf[UnresolvedRelation]
    } else if (version.startsWith("2.2")) {
      createObject(
        className,
        tableIdentifier)._1.asInstanceOf[UnresolvedRelation]
    } else {
      throw new UnsupportedOperationException(s"Unsupported Spark version $version")
    }
  }

  def getSubqueryAlias(sparkSession: SparkSession, alias: Option[String],
      relation: LogicalPlan,
      view: Option[TableIdentifier]): SubqueryAlias = {
    val className = "org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias"
    if (sparkSession.version.startsWith("2.1")) {
      createObject(
        className,
        alias.getOrElse(""),
        relation,
        Option(view))._1.asInstanceOf[SubqueryAlias]
    } else if (sparkSession.version.startsWith("2.2")) {
      createObject(
        className,
        alias.getOrElse(""),
        relation)._1.asInstanceOf[SubqueryAlias]
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
    if (sparkSession.version.startsWith("2.1") || sparkSession.version.startsWith("2.2")) {
      createObject(
        "org.apache.spark.sql.hive.CarbonSqlAstBuilder",
        conf,
        sqlParser)._1.asInstanceOf[AstBuilder]
    } else {
      throw new UnsupportedOperationException("Spark version not supported")
    }
  }

  def getSessionState(sparkContext: SparkContext, carbonSession: Object): Any = {
    if (sparkContext.version.startsWith("2.1")) {
      val className = sparkContext.conf.get(
        CarbonCommonConstants.CARBON_SESSIONSTATE_CLASSNAME,
        "org.apache.spark.sql.hive.CarbonSessionState")
      createObject(className, carbonSession)._1
    } else if (sparkContext.version.startsWith("2.2")) {
      val className = sparkContext.conf.get(
        CarbonCommonConstants.CARBON_SESSIONSTATE_CLASSNAME,
        "org.apache.spark.sql.hive.CarbonSessionStateBuilder")
      val tuple = createObject(className, carbonSession, None)
      val method = tuple._2.getMethod("build")
      method.invoke(tuple._1)
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
