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

package org.apache.carbondata.mv.datamap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.{ColumnTableRelation, DataMapField, Field}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.DataType

import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Utility class for keeping all the utility method for mv datamap
 */
class MVUtil {

  var counter = 0

  /**
   * Below method will be used to validate and get the required fields from select plan
   */
  def getFieldsAndDataMapFieldsFromPlan(plan: LogicalPlan,
      selectStmt: String,
      sparkSession: SparkSession): scala.collection.mutable.LinkedHashMap[Field, DataMapField] = {
    plan match {
      case Project(projectList, child: Sort) =>
        getFieldsFromProject(projectList, plan, child)
      case Project(projectList, _) =>
        getFieldsFromProject(projectList, plan)
      case Aggregate(groupByExp, aggExp, _) =>
        getFieldsFromAggregate(groupByExp, aggExp, plan)
    }
  }

  def getFieldsFromProject(projectList: Seq[NamedExpression],
      plan: LogicalPlan, sort: LogicalPlan): mutable.LinkedHashMap[Field, DataMapField] = {
    var fieldToDataMapFieldMap = scala.collection.mutable.LinkedHashMap.empty[Field, DataMapField]
    sort.transformDown {
      case agg@Aggregate(groupByExp, aggExp, _) =>
        fieldToDataMapFieldMap ++== getFieldsFromAggregate(groupByExp, aggExp, plan)
        agg
    }
    fieldToDataMapFieldMap ++== getFieldsFromProject(projectList, plan)
    fieldToDataMapFieldMap
  }

  def getFieldsFromProject(projectList: Seq[NamedExpression],
      plan: LogicalPlan): mutable.LinkedHashMap[Field, DataMapField] = {
    var fieldToDataMapFieldMap = scala.collection.mutable.LinkedHashMap.empty[Field, DataMapField]
    val logicalRelation =
      plan.collect {
        case lr: LogicalRelation =>
          lr
      }
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
          fieldToDataMapFieldMap +=
          getFieldToDataMapFields(attr.name,
            attr.dataType,
            attr.qualifier,
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
      case a@Alias(_, name) =>
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

  def getFieldsFromAggregate(groupByExp: Seq[Expression],
      aggExp: Seq[NamedExpression],
      plan: LogicalPlan): mutable.LinkedHashMap[Field, DataMapField] = {
    var fieldToDataMapFieldMap = scala.collection.mutable.LinkedHashMap.empty[Field, DataMapField]
    val logicalRelation =
      plan.collect {
        case lr: LogicalRelation =>
          lr
      }
    aggExp.map { agg =>
      var aggregateType: String = ""
      val arrayBuffer: ArrayBuffer[ColumnTableRelation] = new ArrayBuffer[ColumnTableRelation]()
      var isLiteralPresent = false
      agg.collect {
        case Alias(attr: AggregateExpression, name) =>
          attr.aggregateFunction.collect {
            case l@Literal(_, _) =>
              isLiteralPresent = true
          }
          if (isLiteralPresent) {
            fieldToDataMapFieldMap +=
            getFieldToDataMapFields(name,
              attr.aggregateFunction.dataType,
              None,
              attr.aggregateFunction.nodeName,
              arrayBuffer,
              "")
            aggregateType = attr.aggregateFunction.nodeName
          } else {
            aggregateType = attr.aggregateFunction.nodeName
          }
        case a@Alias(_, name) =>
          checkIfComplexDataTypeExists(a)
          // In case of arithmetic expressions like sum(a)+sum(b)
          aggregateType = "arithmetic"
      }
      agg.collect {
        case attr: AttributeReference =>
          val carbonTable: CarbonTable = getCarbonTable(logicalRelation, attr)
          if (null != carbonTable) {
            val relation = getColumnRelation(attr.name,
              carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableId,
              carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableName,
              carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName,
              carbonTable)
            if (null != relation) {
              arrayBuffer += relation
            }
            if (aggregateType.isEmpty && arrayBuffer.nonEmpty) {
              val tableName = carbonTable.getTableName
              fieldToDataMapFieldMap +=
              getFieldToDataMapFields(agg.name,
                agg.dataType,
                attr.qualifier,
                aggregateType,
                arrayBuffer,
                tableName)
            }
          }
      }
      if (!aggregateType.isEmpty && arrayBuffer.nonEmpty && !isLiteralPresent) {
        fieldToDataMapFieldMap +=
        getFieldToDataMapFields(agg.name,
          agg.dataType,
          agg.qualifier,
          aggregateType,
          arrayBuffer,
          "")
      }
    }
    groupByExp map { grp =>
      grp.collect {
        case attr: AttributeReference =>
          val carbonTable: CarbonTable = getCarbonTable(logicalRelation, attr)
          if (null != carbonTable) {
            val arrayBuffer: ArrayBuffer[ColumnTableRelation] = new
                ArrayBuffer[ColumnTableRelation]()
            arrayBuffer += getColumnRelation(attr.name,
              carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableId,
              carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableName,
              carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName,
              carbonTable)
            fieldToDataMapFieldMap +=
            getFieldToDataMapFields(attr.name,
              attr.dataType,
              attr.qualifier,
              "",
              arrayBuffer,
              carbonTable.getTableName)
          }
      }
    }
    fieldToDataMapFieldMap
  }

  /**
   * Below method will be used to get the column relation with the parent column
   */
  def getColumnRelation(parentColumnName: String,
      parentTableId: String,
      parentTableName: String,
      parentDatabaseName: String,
      carbonTable: CarbonTable): ColumnTableRelation = {
    val parentColumn = carbonTable.getColumnByName(parentTableName, parentColumnName)
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
        .head.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonRelation
        .metaData.carbonTable
    } else {
      null
    }
  }

  /**
   * Below method will be used to get the fields object for mv table
   */
  private def getFieldToDataMapFields(name: String,
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
    val dataMapField = DataMapField(aggregateType, Some(columnTableRelationList))
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
        s"MV datamap is unsupported for ComplexData type child column: " + a.child.simpleString)
    }
  }

  def validateDMProperty(tableProperty: mutable.Map[String, String]): Unit = {
    val tableProperties = Array("dictionary_include", "dictionary_exclude", "sort_columns",
      "local_dictionary_include", "local_dictionary_exclude", "long_string_columns",
      "no_inverted_index", "inverted_index", "column_meta_cache", "range_column")
    val unsupportedProps = tableProperty
      .filter(f => tableProperties.exists(prop => prop.equalsIgnoreCase(f._1)))
    if (unsupportedProps.nonEmpty) {
      throw new MalformedDataMapCommandException(
        "DMProperties " + unsupportedProps.keySet.mkString(",") +
        " are not allowed for this datamap")
    }
  }

  def updateDuplicateColumns(outputList: Seq[NamedExpression]): Seq[NamedExpression] = {
    val duplicateNameCols = outputList.groupBy(_.name).filter(_._2.length > 1).flatMap(_._2)
      .toList
    val updatedOutList = outputList.map { col =>
      val duplicateColumn = duplicateNameCols
        .find(a => a.semanticEquals(col))
      val qualifiedName = col.qualifier.getOrElse(s"${ col.exprId.id }") + "_" + col.name
      if (duplicateColumn.isDefined) {
        val attributesOfDuplicateCol = duplicateColumn.get.collect {
          case a: AttributeReference => a
        }
        val attributeOfCol = col.collect { case a: AttributeReference => a }
        // here need to check the whether the duplicate columns is of same tables,
        // since query with duplicate columns is valid, we need to make sure, not to change their
        // names with above defined qualifier name, for example in case of some expression like
        // cast((FLOOR((cast(col_name) as double))).., upper layer even exprid will be same,
        // we need to find the attribute ref(col_name) at lower level and check where expid is same
        // or of same tables, so doin the semantic equals
        val isStrictDuplicate = attributesOfDuplicateCol.forall(expr =>
          attributeOfCol.exists(a => a.semanticEquals(expr)))
        if (!isStrictDuplicate) {
          Alias(col, qualifiedName)(exprId = col.exprId)
        } else if (col.qualifier.isDefined) {
          Alias(col, qualifiedName)(exprId = col.exprId)
          // this check is added in scenario where the column is direct Attribute reference and
          // since duplicate columns select is allowed, we should just put alias for those columns
          // and update, for this also above isStrictDuplicate will be true so, it will not be
          // updated above
        } else if (duplicateColumn.get.isInstanceOf[AttributeReference] &&
                   col.isInstanceOf[AttributeReference]) {
          Alias(col, qualifiedName)(exprId = col.exprId)
        } else {
          col
        }
      } else {
        col
      }
    }
    updatedOutList
  }
}
