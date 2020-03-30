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

package org.apache.carbondata.mv.extension

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.command.Field
import org.apache.spark.sql.types.DataType

import org.apache.carbondata.common.exceptions.sql.MalformedMVCommandException
import org.apache.carbondata.mv.plans.modular.{GroupBy, ModularPlan, ModularRelation, Select}
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Utility class for keeping all the utility method for the MV
 */
class MVUtil {

  var counter = 0

  /**
   * Below method will be used to validate and get the required fields from select plan
   */
  def getFieldsAndMVFieldsFromPlan(
      plan: ModularPlan,
      relations: Seq[Relation]): mutable.LinkedHashMap[Field, MVField] = {
    plan match {
      case select: Select =>
        select.children.map {
          case groupBy: GroupBy =>
            getFieldsFromProject(groupBy.outputList, groupBy.predicateList, relations)
          case _: ModularRelation =>
            getFieldsFromProject(select.outputList, select.predicateList, relations)
        }.head
      case groupBy: GroupBy =>
        groupBy.child match {
          case select: Select =>
            getFieldsFromProject(groupBy.outputList, select.predicateList, relations)
          case _: ModularRelation =>
            getFieldsFromProject(groupBy.outputList, groupBy.predicateList, relations)
        }
    }
  }

  /**
   * Create's main table to MV table field relation map by using modular plan generated from
   * user query
   * @param outputList of the modular plan
   * @param predicateList of the modular plan
   * @param relations list of main table from query
   * @return fieldRelationMap
   */
  private def getFieldsFromProject(
      outputList: Seq[NamedExpression],
      predicateList: Seq[Expression],
      relations: Seq[Relation]): mutable.LinkedHashMap[Field, MVField] = {
    var fieldToMVFieldMap = scala.collection.mutable.LinkedHashMap.empty[Field, MVField]
    fieldToMVFieldMap ++== getFieldsFromProject(outputList, relations)
    var finalPredicateList: Seq[NamedExpression] = Seq.empty
    predicateList.map { p =>
      p.collect {
        case attr: AttributeReference =>
          finalPredicateList = finalPredicateList.:+(attr)
      }
    }
    fieldToMVFieldMap ++== getFieldsFromProject(finalPredicateList.distinct, relations)
    fieldToMVFieldMap
  }

  private def getFieldsFromProject(
      projectList: Seq[NamedExpression],
      relations: Seq[Relation]): mutable.LinkedHashMap[Field, MVField] = {
    var fieldToMVFieldMap = scala.collection.mutable.LinkedHashMap.empty[Field, MVField]
    projectList.map {
      case attr: AttributeReference =>
        val catalogTable = getCatalogTable(relations, attr)
        if (null != catalogTable) {
          val arrayBuffer: ArrayBuffer[ColumnTableRelation] = new ArrayBuffer[ColumnTableRelation]()
          val relation = getColumnRelation(
            attr.name,
            catalogTable.identifier.table,
            catalogTable.database)
          if (null != relation) {
            arrayBuffer += relation
          }
          var qualifier: Option[String] = None
          if (attr.qualifier.nonEmpty) {
            qualifier = if (attr.qualifier.headOption.get.startsWith("gen_sub")) {
              Some(catalogTable.identifier.table)
            } else {
              attr.qualifier.lastOption
            }
          }
          fieldToMVFieldMap +=
          getFieldToMVFields(
            attr.name,
            attr.dataType,
            qualifier.lastOption,
            "",
            arrayBuffer,
            catalogTable.identifier.table)
        }
      case Alias(attr: AttributeReference, name) =>
        val catalogTable = getCatalogTable(relations, attr)
        if (null != catalogTable) {
          val arrayBuffer: ArrayBuffer[ColumnTableRelation] = new ArrayBuffer[ColumnTableRelation]()
          val relation = getColumnRelation(
            attr.name,
            catalogTable.identifier.table,
            catalogTable.database)
          if (null != relation) {
            arrayBuffer += relation
          }
          fieldToMVFieldMap +=
          getFieldToMVFields(name, attr.dataType, None, "", arrayBuffer, "")
        }

      case a@Alias(agg: AggregateExpression, _) =>
        checkIfComplexDataTypeExists(a)
        val arrayBuffer: ArrayBuffer[ColumnTableRelation] = new ArrayBuffer[ColumnTableRelation]()
        a.collect {
          case attr: AttributeReference =>
            val catalogTable = getCatalogTable(relations, attr)
            if (null != catalogTable) {
              val relation = getColumnRelation(
                attr.name,
                catalogTable.identifier.table,
                catalogTable.database)
              if (null != relation) {
                arrayBuffer += relation
              }
            }
        }
        fieldToMVFieldMap +=
        getFieldToMVFields(a.name,
          a.dataType,
          None,
          agg.aggregateFunction.nodeName,
          arrayBuffer,
          "")

      case a@Alias(_, _) =>
        checkIfComplexDataTypeExists(a)
        val arrayBuffer: ArrayBuffer[ColumnTableRelation] = new ArrayBuffer[ColumnTableRelation]()
        a.collect {
          case attr: AttributeReference =>
            val catalogTable = getCatalogTable(relations, attr)
            if (null != catalogTable) {
              val relation = getColumnRelation(
                attr.name,
                catalogTable.identifier.table,
                catalogTable.database)
              if (null != relation) {
                arrayBuffer += relation
              }
            }
        }
        fieldToMVFieldMap +=
        getFieldToMVFields(a.name, a.dataType, None, "arithmetic", arrayBuffer, "")
    }
    fieldToMVFieldMap
  }

  /**
   * Below method will be used to get the column relation with the parent column
   */
  private def getColumnRelation(
      parentColumnName: String,
      parentTableName: String,
      parentDatabaseName: String) = {
    ColumnTableRelation(
      columnName = parentColumnName,
      tableName = parentTableName,
      databaseName = parentDatabaseName)
  }

  /**
   * Return the catalog table after matching the attr in logicalRelation
   */
  private def getCatalogTable(
      relations: Seq[Relation],
      attr: AttributeReference): CatalogTable = {
    val filteredRelation = relations.filter { relation =>
        relation.output.exists(attrRef => attrRef.name.equalsIgnoreCase(attr.name) &&
                                          attrRef.exprId.equals(attr.exprId))
    }
    if (filteredRelation.nonEmpty) {
      filteredRelation.head.catalogTable.get
    } else {
      null
    }
  }

  /**
   * Below method will be used to get the fields object for mv table
   */
  private def getFieldToMVFields(
      name: String,
      dataType: DataType,
      qualifier: Option[String],
      aggregateType: String,
      columnTableRelationList: ArrayBuffer[ColumnTableRelation],
      parenTableName: String) = {
    var actualColumnName = MVHelper.getUpdatedName(name, counter)
    counter += 1
    if (qualifier.isDefined) {
      actualColumnName = qualifier.map(qualifier => qualifier + "_" + name)
        .getOrElse(actualColumnName)
    }
    if (qualifier.isEmpty) {
      if (aggregateType.isEmpty && !parenTableName.isEmpty) {
        actualColumnName = parenTableName + "_" + actualColumnName
      }
    }
    val rawSchema = '`' + actualColumnName + '`' + ' ' + dataType.typeName
    val mvField = MVField(aggregateType, columnTableRelationList)
    if (dataType.typeName.startsWith("decimal")) {
      val (precision, scale) = CommonUtil.getScaleAndPrecision(dataType.catalogString)
      (Field(column = actualColumnName,
        dataType = Some(dataType.typeName),
        name = Some(actualColumnName),
        children = None,
        precision = precision,
        scale = scale,
        rawSchema = rawSchema), mvField)
    } else {
      (Field(column = actualColumnName,
        dataType = Some(dataType.typeName),
        name = Some(actualColumnName),
        children = None,
        rawSchema = rawSchema), mvField)
    }
  }

  private def checkIfComplexDataTypeExists(a: Alias): Unit = {
    if (a.child.isInstanceOf[GetMapValue] || a.child.isInstanceOf[GetStructField] ||
        a.child.isInstanceOf[GetArrayItem]) {
      throw new UnsupportedOperationException(
        s"MV is not supported for complex datatype child columns and complex datatype " +
        s"return types of function :" + a.child.simpleString)
    }
  }

  def validateDMProperty(tableProperty: mutable.Map[String, String]): Unit = {
    val tableProperties = Array("sort_columns",
      "local_dictionary_include", "local_dictionary_exclude", "long_string_columns",
      "no_inverted_index", "inverted_index", "column_meta_cache", "range_column")
    val unsupportedProps = tableProperty
      .filter(f => tableProperties.exists(prop => prop.equalsIgnoreCase(f._1)))
    if (unsupportedProps.nonEmpty) {
      throw new MalformedMVCommandException(
        "DMProperties " + unsupportedProps.keySet.mkString(",") +
        " are not allowed for this MV")
    }
  }

}
