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

package org.apache.carbondata.view

import java.util
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.gson.Gson
import org.apache.spark.sql.{CarbonEnv, CarbonToSparkAdapter, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, GetArrayItem, GetMapValue, GetStructField, NamedExpression, ScalaUDF, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.execution.command.Field
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonHiveIndexMetadataUtil
import org.apache.spark.sql.index.CarbonIndexUtil
import org.apache.spark.sql.types.DataType

import org.apache.carbondata.common.exceptions.sql.MalformedMVCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.view.MVSchema
import org.apache.carbondata.mv.plans.modular.{GroupBy, ModularPlan, ModularRelation, Select}
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Utility class for keeping all the utility method for mv
 */
object MVHelper {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def dropDummyFunction(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case Project(expressions, child) =>
        Project(dropDummyFunction(expressions), child)
      case Aggregate(groupBy, aggregations, child) =>
        Aggregate(groupBy, dropDummyFunction(aggregations), child)
    }
  }

  private def dropDummyFunction(expressions: Seq[NamedExpression]) = {
    expressions.map {
      case Alias(_: ScalaUDF, name)
        if name.equalsIgnoreCase(MVFunctions.DUMMY_FUNCTION) => None
      case reference: AttributeReference
        if reference.name.equalsIgnoreCase(MVFunctions.DUMMY_FUNCTION) => None
      case other => Some(other)
    }.filter(_.isDefined).map(_.get)
  }

  /**
   * Below method will be used to get the fields object for mv table
   */
  private def newField(
      tableName: String,
      name: String,
      dataType: DataType,
      qualifier: Option[String],
      aggregateType: String,
      fieldCounter: AtomicInteger): Field = {
    var columnName = getUpdatedColumnName(name, fieldCounter.getAndIncrement())
    if (qualifier.isDefined) {
      columnName = qualifier.map(qualifier => qualifier + "_" + name).getOrElse(columnName)
    }
    if (qualifier.isEmpty) {
      if (aggregateType.isEmpty && !tableName.isEmpty) {
        columnName = tableName + "_" + columnName
      }
    }
    val rawSchema = '`' + columnName + '`' + ' ' + dataType.typeName
    if (dataType.typeName.startsWith("decimal")) {
      val (precision, scale) = CommonUtil.getScaleAndPrecision(dataType.catalogString)
      Field(column = columnName,
        dataType = Some(dataType.typeName),
        name = Some(columnName),
        children = None,
        precision = precision,
        scale = scale,
        rawSchema = rawSchema)
    } else {
      Field(column = columnName,
        dataType = Some(dataType.typeName),
        name = Some(columnName),
        children = None,
        rawSchema = rawSchema)
    }
  }

  /**
   * Below method will be used to validate and get the required fields from select plan
   */
  def getFieldsMapFromPlan(
      plan: ModularPlan,
      relationList: Seq[LogicalRelation]): scala.collection.mutable.LinkedHashMap[Field,
    MVField] = {
    val fieldCounter = new AtomicInteger(0)
    plan match {
      case select: Select =>
        select.children.map {
          case groupBy: GroupBy =>
            getFieldsMapFromProject(groupBy.outputList, groupBy.predicateList,
              relationList, groupBy.flagSpec, fieldCounter)
          case _: ModularRelation =>
            getFieldsMapFromProject(select.outputList, select.predicateList,
              relationList, select.flagSpec, fieldCounter)
        }.head
      case groupBy: GroupBy =>
        groupBy.child match {
          case select: Select =>
            getFieldsMapFromProject(groupBy.outputList, select.predicateList,
              relationList, select.flagSpec, fieldCounter)
          case _: ModularRelation =>
            getFieldsMapFromProject(groupBy.outputList, groupBy.predicateList,
              relationList, groupBy.flagSpec, fieldCounter)
        }
    }
  }

  /**
   * Create's main table to MV table field relation map by using modular plan generated from
   * user query
   * @param outputList of the modular plan
   * @param predicateList of the modular plan
   * @param relationList list of related table from query
   * @param flagSpec to get SortOrder attribute if exists
   * @return fieldRelationMap
   */
  private def getFieldsMapFromProject(
      outputList: Seq[NamedExpression],
      predicateList: Seq[Expression],
      relationList: Seq[LogicalRelation],
      flagSpec: Seq[Seq[Any]],
      fieldCounter: AtomicInteger): mutable.LinkedHashMap[Field, MVField] = {
    var fieldsMap = scala.collection.mutable.LinkedHashMap.empty[Field, MVField]
    fieldsMap ++== getFieldsMapFromProject(relationList, outputList, fieldCounter)
    var finalPredicateList: Seq[NamedExpression] = Seq.empty
    predicateList.map { predicate =>
      predicate.collect {
        case reference: AttributeReference =>
          finalPredicateList = finalPredicateList.:+(reference)
      }
    }
    // collect sort by columns
    if (flagSpec.nonEmpty) {
      flagSpec.map { flag =>
        flag.map {
          case list: ArrayBuffer[_] =>
            list.map {
              case order: SortOrder =>
                order.collect {
                  case reference: AttributeReference =>
                    finalPredicateList = finalPredicateList.:+(reference)
                }
            }
        }
      }
    }
    fieldsMap ++== getFieldsMapFromProject(relationList, finalPredicateList.distinct, fieldCounter)
    fieldsMap
  }

  private def checkComplexDataType(alias: Alias): Unit = {
    if (alias.child.isInstanceOf[GetMapValue] ||
        alias.child.isInstanceOf[GetStructField] ||
        alias.child.isInstanceOf[GetArrayItem]) {
      throw new UnsupportedOperationException(
        s"MV is not supported for complex datatype child columns and complex datatype " +
        s"return types of function :" + alias.child.toString())
    }
  }

  private def getFieldsMapFromProject(
      relationList: Seq[LogicalRelation],
      projectList: Seq[NamedExpression],
      fieldCounter: AtomicInteger): mutable.LinkedHashMap[Field, MVField] = {
    val fieldsMap = scala.collection.mutable.LinkedHashMap.empty[Field, MVField]
    // map of qualified name with list of column names
    val fieldColumnsMap = new util.HashMap[String, java.util.ArrayList[String]]()
    projectList.map {
      case reference: AttributeReference =>
        val columns = new util.ArrayList[String]()
        columns.add(reference.qualifiedName)
        findDuplicateColumns(fieldColumnsMap, reference.sql, columns, false)
        val relation = getRelation(relationList, reference)
        if (null != relation) {
          val relatedFields = Seq(RelatedFieldWrapper(
            relation.database,
            relation.identifier.table,
            reference.name))
          var qualifier: Option[String] = None
          if (reference.qualifier.nonEmpty) {
            qualifier = if (reference.qualifier.headOption.get.startsWith("gen_sub")) {
              Some(relation.identifier.table)
            } else {
              reference.qualifier.lastOption
            }
          }
          fieldsMap.put(
            newField(
              relation.identifier.table,
              reference.name,
              reference.dataType,
              qualifier,
              "",
              fieldCounter),
            MVField("", relatedFields)
          )
        }

      case a@Alias(reference: AttributeReference, name) =>
        val columns = new util.ArrayList[String]()
        columns.add(reference.qualifiedName)
        findDuplicateColumns(fieldColumnsMap, a.sql, columns, true)
        val relation = getRelation(relationList, reference)
        if (null != relation) {
          val relatedFields = Seq(RelatedFieldWrapper(
            relation.database,
            relation.identifier.table,
            reference.name))
          fieldsMap.put(
            newField(
              "",
              name,
              reference.dataType,
              None,
              "",
              fieldCounter),
            MVField("", relatedFields)
          )
        }

      case alias@Alias(agg: AggregateExpression, _) =>
        checkComplexDataType(alias)
        val relatedFields: ArrayBuffer[RelatedFieldWrapper] =
          new ArrayBuffer[RelatedFieldWrapper]()
        val columns = new util.ArrayList[String]()
        alias.collect {
          case reference: AttributeReference =>
            columns.add(reference.qualifiedName)
            val relation = getRelation(relationList, reference)
            if (null != relation) {
              relatedFields += RelatedFieldWrapper(
                relation.database,
                relation.identifier.table,
                reference.name)
            }
        }
       findDuplicateColumns(fieldColumnsMap, alias.sql, columns, true)
        fieldsMap.put(
          newField(
            "",
            alias.name,
            alias.dataType,
            None,
            agg.aggregateFunction.nodeName,
            fieldCounter),
          MVField(agg.aggregateFunction.nodeName, relatedFields)
        )

      case alias@Alias(_, _) =>
        checkComplexDataType(alias)
        val relatedFields: ArrayBuffer[RelatedFieldWrapper] =
          new ArrayBuffer[RelatedFieldWrapper]()
        val columns = new util.ArrayList[String]()
        alias.collect {
          case reference: AttributeReference =>
            columns.add(reference.qualifiedName)
            val relation = getRelation(relationList, reference)
            if (null != relation) {
              relatedFields += RelatedFieldWrapper(
                relation.database,
                relation.identifier.table,
                reference.name)
            }
        }
        findDuplicateColumns(fieldColumnsMap, alias.sql, columns, true)
        fieldsMap.put(
          newField(
            "",
            alias.name,
            alias.dataType,
            None,
            "arithmetic",
            fieldCounter),
          MVField("arithmetic", relatedFields)
        )
    }
    fieldsMap
  }

  private def findDuplicateColumns(
      fieldColumnsMap: util.HashMap[String, util.ArrayList[String]],
      columnName: String,
      columns: util.ArrayList[String],
      isAlias: Boolean): Unit = {
    // get qualified name without alias name
    val qualifiedName = if (isAlias) {
      columnName.substring(0, columnName.indexOf(" AS"))
    } else {
      columnName
    }
    if (null == fieldColumnsMap.get(qualifiedName)) {
      // case to check create mv with same column and different alias names
      if (fieldColumnsMap.containsKey(qualifiedName)) {
        throw new MalformedMVCommandException(
          "Cannot create mv having duplicate column with different alias name: " + columnName)
      }
      fieldColumnsMap.put(qualifiedName, columns)
    } else {
      if (fieldColumnsMap.get(qualifiedName).containsAll(columns)) {
        throw new MalformedMVCommandException(
          "Cannot create mv with duplicate column: " + columnName)
      }
    }
  }

  /**
   * Return the catalog table after matching the attr in logicalRelation
   */
  private def getRelation(
      relationList: Seq[LogicalRelation],
      reference: AttributeReference): CatalogTable = {
    val relations = relationList.filter(
      relation =>
        relation.output.exists(
          attribute =>
            attribute.name.equalsIgnoreCase(reference.name) &&
            attribute.exprId.equals(reference.exprId)
        )
    )
    if (relations.nonEmpty) {
      relations.head.catalogTable.get
    } else {
      null
    }
  }

  def getUpdatedColumnName(attribute: Attribute, counter: Int): String = {
    val name = getUpdatedColumnName(attribute.name, counter)
    if (attribute.qualifier.nonEmpty) {
      CarbonToSparkAdapter.getTheLastQualifier(attribute) + "_" + name
    } else {
      name
    }
  }

  private def getUpdatedColumnName(name: String, counter: Int): String = {
    var updatedName = name.replace("(", "_")
      .replace(")", "")
      .replace(" ", "_")
      .replace("=", "")
      .replace(",", "")
      .replace(".", "_")
      .replace("`", "")
    if (updatedName.length >= CarbonCommonConstants.MAXIMUM_CHAR_LENGTH) {
      updatedName = updatedName.substring(0, 110) + CarbonCommonConstants.UNDERSCORE + counter
    }
    updatedName
  }

  /**
   * Add or modify the MV database and table name to fact table's related mv tables Map
   * 1. During Create, add the database and mv name to the fact table's map
   * 2. On drop mv, remove if fact table's map contains mv name
   * 3. On refresh, add mv name to the fact table's map if it is not present.
   * If already present, then just return
   */
  def addOrModifyMVTablesMap(session: SparkSession,
      mvSchema: MVSchema,
      isMVDrop: Boolean = false,
      isLockAcquiredOnFactTable: String = null,
      isRefreshMV: Boolean = false): Unit = {
    val mvDatabaseName = mvSchema.getIdentifier.getDatabaseName
    val mvName = mvSchema.getIdentifier.getTableName
    mvSchema.getRelatedTables.asScala.foreach { parentTable =>
      val parentDbName = parentTable.getDatabaseName
      val parentTableName = parentTable.getTableName
      var carbonLock: ICarbonLock = null
      try {
        val carbonTable = try {
          CarbonEnv.getCarbonTable(Some(parentDbName), parentTableName)(session)
        } catch {
          case _: Exception =>
            LOGGER.error(s"Error while getting carbon table for " +
                         s"${ parentDbName + "." + parentTableName }")
            null
        }
        if (null != carbonTable) {
          // get the mv tables map from fact table
          var relatedMVTablesMap = carbonTable.getMVTablesMap
          if (isRefreshMV && relatedMVTablesMap.containsKey(mvDatabaseName) &&
              relatedMVTablesMap.get(mvDatabaseName).contains(mvName)) {
            // in case of refresh mv scenario, if mvName is already present in fact table
            // properties, then do not update , just return
            return
          }
          // get lock on fact table
          if (null == isLockAcquiredOnFactTable) {
            carbonLock = CarbonLockUtil.getLockObject(carbonTable.getAbsoluteTableIdentifier,
              LockUsage.METADATA_LOCK)
          } else if (null != isLockAcquiredOnFactTable &&
                     !isLockAcquiredOnFactTable.equals(parentTableName)) {
            carbonLock = CarbonLockUtil.getLockObject(carbonTable.getAbsoluteTableIdentifier,
              LockUsage.METADATA_LOCK)
          }
          // get the mv tables map again from fact table after acquiring lock
          relatedMVTablesMap = carbonTable.getMVTablesMap
          var needFactTableUpdate = true
          if (isMVDrop) {
            //  If database don't have any MV, then remove the database from related tables
            //  property and update table property of fact table
            relatedMVTablesMap.get(mvDatabaseName).remove(mvName)
            if (relatedMVTablesMap.get(mvDatabaseName).isEmpty) {
              relatedMVTablesMap.remove(mvDatabaseName)
            }
          } else {
            if (!relatedMVTablesMap.containsKey(mvDatabaseName)) {
              val mvTables = new util.ArrayList[String]()
              mvTables.add(mvName)
              relatedMVTablesMap.put(mvDatabaseName, mvTables)
            } else if (!relatedMVTablesMap.get(mvDatabaseName).contains(mvName)) {
              relatedMVTablesMap.get(mvDatabaseName).add(mvName)
            } else {
              needFactTableUpdate = false
            }
          }
          if (needFactTableUpdate) {
            CarbonIndexUtil.addOrModifyTableProperty(carbonTable,
              Map(CarbonCommonConstants.RELATED_MV_TABLES_MAP ->
                  new Gson().toJson(relatedMVTablesMap)),
              needLock = false)(session)
            CarbonHiveIndexMetadataUtil.refreshTable(parentDbName, parentTableName, session)
          }
        }
      } finally {
        if (null != carbonLock) {
          val unlock = carbonLock.unlock()
          if (unlock) {
            LOGGER.info("Table MetaData Unlocked Successfully")
          } else {
            LOGGER.error(s"Unable to unlock metadata lock for table $parentTableName")
          }
        }
      }
    }
  }

}


