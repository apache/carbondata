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

import org.apache.spark.sql.CarbonDatasourceHadoopRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.command.Field
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.DataType

import org.apache.carbondata.common.exceptions.sql.MalformedMaterializedViewException
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.mv.plans.modular.{GroupBy, ModularPlan, ModularRelation, Select}
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Utility class for keeping all the utility method for mv datamap
 */
class MVUtil {

  var counter = 0

  /**
   * Below method will be used to validate and get the required fields from select plan
   */
  def getFieldsAndDataMapFieldsFromPlan(
      plan: ModularPlan,
      logicalRelation: Seq[LogicalRelation]): scala.collection.mutable.LinkedHashMap[Field,
    MVField] = {
    plan match {
      case select: Select =>
        select.children.map {
          case groupBy: GroupBy =>
            getFieldsFromProject(groupBy.outputList, groupBy.predicateList,
              logicalRelation, groupBy.flagSpec)
          case _: ModularRelation =>
            getFieldsFromProject(select.outputList, select.predicateList,
              logicalRelation, select.flagSpec)
        }.head
      case groupBy: GroupBy =>
        groupBy.child match {
          case select: Select =>
            getFieldsFromProject(groupBy.outputList, select.predicateList,
              logicalRelation, select.flagSpec)
          case _: ModularRelation =>
            getFieldsFromProject(groupBy.outputList, groupBy.predicateList,
              logicalRelation, groupBy.flagSpec)
        }
    }
  }

  /**
   * Create's main table to datamap table field relation map by using modular plan generated from
   * user query
   * @param outputList of the modular plan
   * @param predicateList of the modular plan
   * @param logicalRelation list of main table from query
   * @param flagSpec to get SortOrder attribute if exists
   * @return fieldRelationMap
   */
  private def getFieldsFromProject(
      outputList: Seq[NamedExpression],
      predicateList: Seq[Expression],
      logicalRelation: Seq[LogicalRelation],
      flagSpec: Seq[Seq[Any]]): mutable.LinkedHashMap[Field, MVField] = {
    var fieldToDataMapFieldMap = scala.collection.mutable.LinkedHashMap.empty[Field, MVField]
    fieldToDataMapFieldMap ++== getFieldsFromProject(outputList, logicalRelation)
    var finalPredicateList: Seq[NamedExpression] = Seq.empty
    predicateList.map { p =>
      p.collect {
        case attr: AttributeReference =>
          finalPredicateList = finalPredicateList.:+(attr)
      }
    }
    // collect sort by columns
    if (flagSpec.nonEmpty) {
      flagSpec.map { f =>
        f.map {
          case list: ArrayBuffer[_] =>
            list.map {
              case s: SortOrder =>
                s.collect {
                  case attr: AttributeReference =>
                    finalPredicateList = finalPredicateList.:+(attr)
                }
            }
        }
      }
    }
    fieldToDataMapFieldMap ++== getFieldsFromProject(finalPredicateList.distinct, logicalRelation)
    fieldToDataMapFieldMap
  }

  private def getFieldsFromProject(
      projectList: Seq[NamedExpression],
      logicalRelation: Seq[LogicalRelation]): mutable.LinkedHashMap[Field, MVField] = {
    var fieldToDataMapFieldMap = scala.collection.mutable.LinkedHashMap.empty[Field, MVField]
    projectList.map {
      case attr: AttributeReference =>
        val carbonTable = getCarbonTable(logicalRelation, attr)
        if (null != carbonTable) {
          val arrayBuffer: ArrayBuffer[ColumnTableRelation] = new ArrayBuffer[ColumnTableRelation]()
          val relation = getColumnRelation(attr.name,
            carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableId,
            carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableName,
            carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName,
            carbonTable)
          if (null != relation) {
            arrayBuffer += relation
          }
          var qualifier: Option[String] = None
          if (attr.qualifier.nonEmpty) {
            qualifier = if (attr.qualifier.headOption.get.startsWith("gen_sub")) {
              Some(carbonTable.getTableName)
            } else {
              attr.qualifier.headOption
            }
          }
          fieldToDataMapFieldMap +=
          getFieldToDataMapFields(
            attr.name,
            attr.dataType,
            qualifier.headOption,
            "",
            arrayBuffer,
            carbonTable.getTableName)
        }
      case Alias(attr: AttributeReference, name) =>
        val carbonTable = getCarbonTable(logicalRelation, attr)
        if (null != carbonTable) {
          val arrayBuffer: ArrayBuffer[ColumnTableRelation] = new ArrayBuffer[ColumnTableRelation]()
          val relation = getColumnRelation(attr.name,
            carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableId,
            carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableName,
            carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName,
            carbonTable)
          if (null != relation) {
            arrayBuffer += relation
          }
          fieldToDataMapFieldMap +=
          getFieldToDataMapFields(name, attr.dataType, None, "", arrayBuffer, "")
        }

      case a@Alias(agg: AggregateExpression, _) =>
        checkIfComplexDataTypeExists(a)
        val arrayBuffer: ArrayBuffer[ColumnTableRelation] = new ArrayBuffer[ColumnTableRelation]()
        a.collect {
          case attr: AttributeReference =>
            val carbonTable = getCarbonTable(logicalRelation, attr)
            if (null != carbonTable) {
              val relation = getColumnRelation(attr.name,
                carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableId,
                carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableName,
                carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName,
                carbonTable)
              if (null != relation) {
                arrayBuffer += relation
              }
            }
        }
        fieldToDataMapFieldMap +=
        getFieldToDataMapFields(a.name,
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
            val carbonTable = getCarbonTable(logicalRelation, attr)
            if (null != carbonTable) {
              val relation = getColumnRelation(attr.name,
                carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableId,
                carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableName,
                carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName,
                carbonTable)
              if (null != relation) {
                arrayBuffer += relation
              }
            }
        }
        fieldToDataMapFieldMap +=
        getFieldToDataMapFields(a.name, a.dataType, None, "arithmetic", arrayBuffer, "")
    }
    fieldToDataMapFieldMap
  }

  /**
   * Below method will be used to get the column relation with the parent column
   */
  private def getColumnRelation(
      parentColumnName: String,
      parentTableId: String,
      parentTableName: String,
      parentDatabaseName: String,
      carbonTable: CarbonTable): ColumnTableRelation = {
    val parentColumn = carbonTable.getColumnByName(parentColumnName)
    var columnTableRelation: ColumnTableRelation = null
    if (null != parentColumn) {
      val parentColumnId = parentColumn.getColumnId
      columnTableRelation = ColumnTableRelation(parentColumnName = parentColumnName,
        parentColumnId = parentColumnId,
        parentTableName = parentTableName,
        parentDatabaseName = parentDatabaseName, parentTableId = parentTableId)
      columnTableRelation
    } else {
      columnTableRelation
    }
  }

  /**
   * This method is used to get carbon table for corresponding attribute reference
   * from logical relation
   */
  private def getCarbonTable(logicalRelation: Seq[LogicalRelation],
      attr: AttributeReference) = {
    val relations = logicalRelation
      .filter(lr => lr.output
        .exists(attrRef => attrRef.name.equalsIgnoreCase(attr.name) &&
                           attrRef.exprId.equals(attr.exprId)))
    if (relations.nonEmpty) {
      relations
        .head.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonRelation.carbonTable
    } else {
      null
    }
  }

  /**
   * Below method will be used to get the fields object for mv table
   */
  private def getFieldToDataMapFields(
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
    val dataMapField = MVField(aggregateType, columnTableRelationList)
    if (dataType.typeName.startsWith("decimal")) {
      val (precision, scale) = CommonUtil.getScaleAndPrecision(dataType.catalogString)
      (Field(column = actualColumnName,
        dataType = Some(dataType.typeName),
        name = Some(actualColumnName),
        children = None,
        precision = precision,
        scale = scale,
        rawSchema = rawSchema), dataMapField)
    } else {
      (Field(column = actualColumnName,
        dataType = Some(dataType.typeName),
        name = Some(actualColumnName),
        children = None,
        rawSchema = rawSchema), dataMapField)
    }
  }

  private def checkIfComplexDataTypeExists(a: Alias): Unit = {
    if (a.child.isInstanceOf[GetMapValue] || a.child.isInstanceOf[GetStructField] ||
        a.child.isInstanceOf[GetArrayItem]) {
      throw new UnsupportedOperationException(
        s"MV datamap is not supported for complex datatype child columns and complex datatype " +
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
      throw new MalformedMaterializedViewException(
        "DMProperties " + unsupportedProps.keySet.mkString(",") +
        " are not allowed for this datamap")
    }
  }

}
