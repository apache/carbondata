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

package org.apache.spark.sql.execution.strategy

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GetArrayItem, GetMapValue, GetStructField, Literal, NamedExpression}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import org.apache.carbondata.hadoop.CarbonProjection

object PushDownHelper {

  def pushDownProjection(
      requiredColumns: Seq[String],
      projects: Seq[NamedExpression],
      projection: CarbonProjection
  ): Unit = {
    // In case of Struct or StructOfStruct Complex type, get the project column for given
    // parent/child field and push down the corresponding project column. In case of Array, Map,
    // ArrayOfStruct, StructOfArray, MapOfStruct or StructOfMap, push down parent column
    val output = ArrayBuffer[String]()
    var parentColumns = ArrayBuffer[String]()
    projects.foreach(PushDownHelper.collectColumns(_, output, parentColumns))
    parentColumns = parentColumns.distinct
    if (output.isEmpty) {
      requiredColumns.foreach(projection.addColumn)
    } else {
      requiredColumns.map(_.toLowerCase).foreach { requiredColumn =>
        val childOption = output.filter(_.startsWith(requiredColumn + "."))
        if (childOption.isEmpty) {
          projection.addColumn(requiredColumn)
        } else {
          // If projection contains both parent and its child, then push down parent column
          // itself instead of its child column
          if (parentColumns.contains(requiredColumn)) {
            projection.addColumn(requiredColumn)
          } else {
            childOption.foreach(projection.addColumn)
          }
        }
      }
    }
  }

  private def collectColumns(
      exp: NamedExpression,
      pushDownColumns: ArrayBuffer[String],
      parentColumns: ArrayBuffer[String]
  ): Unit = {
    exp transform {
      case struct: GetStructField =>
        val found = containDataType(struct.childSchema) || containChild(struct)
        if (found) {
          pushDownColumns += getParentName(struct)
        } else {
          pushDownColumns += getFullName(struct)
        }
        Literal.TrueLiteral
      case array: GetArrayItem =>
        pushDownColumns += getParentName(array)
        Literal.TrueLiteral
      case map: GetMapValue =>
        pushDownColumns += getParentName(map)
        Literal.TrueLiteral
      case attr: AttributeReference =>
        val attrName = attr.name.toLowerCase
        pushDownColumns += attrName
        parentColumns += attrName
        Literal.TrueLiteral
    }
  }

  private def containDataType(dataType: DataType): Boolean = {
    dataType match {
      case struct: StructType =>
        struct.exists { field =>
          containDataType(field.dataType)
        }
      case _: ArrayType => true
      case _: MapType => true
      case _ => false
    }
  }

  private def containChild(struct: GetStructField): Boolean = {
    struct.collectFirst {
      case _: GetArrayItem => true
      case _: GetMapValue => true
    }.isDefined
  }

  private def getFullName(
      expr: org.apache.spark.sql.catalyst.expressions.Expression
  ): String = {
    expr match {
      case attr: AttributeReference =>
        attr.name.toLowerCase
      case struct: GetStructField =>
        getFullName(struct.child) + "." + struct.name.get.toLowerCase
      case _ => ""
    }
  }

  private def getParentName(
      expr: org.apache.spark.sql.catalyst.expressions.Expression
  ): String = {
    expr match {
      case attr: AttributeReference =>
        attr.name.toLowerCase
      case struct: GetStructField =>
        getParentName(struct.child)
      case array: GetArrayItem =>
        getParentName(array.child)
      case map: GetMapValue =>
        getParentName(map.child)
      case _ => ""
    }
  }
}
