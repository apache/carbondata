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

package org.apache.spark.sql.parser

import org.antlr.v4.runtime.tree.TerminalNode
import org.apache.carbondata.common.exceptions.DeprecatedFeatureException
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.SchemaReader
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.CarbonOption
import org.apache.carbondata.spark.util.CarbonScalaUtil
import org.apache.spark.sql.{CarbonEnv, SparkSession, UpdateTables}
import org.apache.spark.sql.catalyst.{CarbonParserUtil, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Contains, EndsWith, EqualTo, Expression, In, IsNotNull, Like, Literal, Not, StartsWith}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.parser.ParserUtils.operationNotAllowed
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.{BucketSpecContext, ColTypeListContext, CreateTableHeaderContext, LocationSpecContext, QueryContext, SkewSpecContext, TablePropertyListContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.table.{CarbonCreateTableAsSelectCommand, CarbonCreateTableCommand}
import org.apache.spark.sql.execution.command.{ExplainCommand, PartitionerField, TableModel, TableNewProcessor}
import org.apache.spark.sql.hive.CarbonHiveIndexMetadataUtil
import org.apache.spark.sql.parser.CarbonSparkSqlParserUtil.{convertDbNameToLowerCase, validateStreamingProperty}
import org.apache.spark.sql.types.StructField

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

object ParserUtil {

  def checkUpdateTable(tab: (UnresolvedRelation, List[String], Option[String], TableIdentifier),
                       relation: UnresolvedRelation, columns: List[String],
                       selectStmt: String, where: String) : UpdateTables = {
    tab._3 match {
      case Some(a) => UpdateTables(relation, columns, selectStmt, Some(tab._3.get), where)
      case None => UpdateTables(relation,
        columns,
        selectStmt,
        Some(tab._1.tableIdentifier.table),
        where)
    }
  }

  def explainCommand(isExtended: Option[String], logicalPlan: LogicalPlan) : ExplainCommand = {
    ExplainCommand(logicalPlan, extended = isExtended.isDefined)
  }

  /**
   * The method validate the create table command and returns the table's columns.
   *
   * @param tableHeader       An instance of CreateTableHeaderContext having parser rules for
   *                          create table.
   * @param skewSpecContext   An instance of SkewSpecContext having parser rules for create table.
   * @param bucketSpecContext An instance of BucketSpecContext having parser rules for create table.
   * @param columns           An instance of ColTypeListContext having parser rules for columns
   *                          of the table.
   * @param cols              Table;s columns.
   * @param tableIdentifier   Instance of table identifier.
   * @param isTempTable       Flag to identify temp table.
   * @return Table's column names <Seq[String]>.
   */
  def validateCreateTableReqAndGetColumns(tableHeader: CreateTableHeaderContext,
                                          skewSpecContext: SkewSpecContext,
                                          bucketSpecContext: BucketSpecContext,
                                          columns: ColTypeListContext,
                                          cols: Seq[StructField],
                                          tableIdentifier: TableIdentifier,
                                          isTempTable: Boolean): Seq[String] = {
    // TODO: implement temporary tables
    if (isTempTable) {
      throw new ParseException(
        "CREATE TEMPORARY TABLE is not supported yet. " +
          "Please use CREATE TEMPORARY VIEW as an alternative.", tableHeader)
    }
    if (skewSpecContext != null) {
      operationNotAllowed("CREATE TABLE ... SKEWED BY", skewSpecContext)
    }
    if (bucketSpecContext != null) {
      operationNotAllowed("CREATE TABLE ... CLUSTERED BY", bucketSpecContext)
    }

    // Ensuring whether no duplicate name is used in table definition
    val colNames: Seq[String] = cols.map(_.name)
    checkIfDuplicateColumnExists(columns, tableIdentifier, colNames)
    colNames

  }

  def checkIfDuplicateColumnExists(columns: ColTypeListContext,
                                   tableIdentifier: TableIdentifier,
                                   colNames: Seq[String]): Unit = {
    if (colNames.length != colNames.distinct.length) {
      val duplicateColumns = colNames.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => "\"" + x + "\""
      }
      val errorMessage = s"Duplicated column names found in table definition of " +
        s"$tableIdentifier: ${ duplicateColumns.mkString("[", ",", "]") }"
      // In case of create table as select, ColTypeListContext will be null. Check if
      // duplicateColumns present in column names list, If yes, throw exception
      if (null != columns) {
        operationNotAllowed(errorMessage, columns)
      } else {
        throw new UnsupportedOperationException(errorMessage)
      }
    }
  }


  /**
   * The method validates the create table command and returns the create table or
   * ctas table LogicalPlan.
   *
   * @param createTableTuple a tuple of (CreateTableHeaderContext, SkewSpecContext,
   *                         BucketSpecContext, ColTypeListContext, ColTypeListContext,
   *                         TablePropertyListContext,
   *                         LocationSpecContext, Option[String], TerminalNode, QueryContext,
   *                         String)
   * @param extraTableTuple  A tuple of (Seq[StructField], Boolean, TableIdentifier, Boolean,
   *                         Seq[String],
   *                         Option[String], mutable.Map[String, String], Map[String, String],
   *                         Seq[StructField],
   *                         Seq[PartitionerField], CarbonSpark2SqlParser, SparkSession,
   *                         Option[LogicalPlan])
   * @return <LogicalPlan> of create table or ctas table
   *
   */
  def createCarbonTable(createTableTuple: (CreateTableHeaderContext, SkewSpecContext,
    BucketSpecContext, ColTypeListContext, ColTypeListContext, TablePropertyListContext,
    LocationSpecContext, Option[String], TerminalNode, QueryContext, String),
                        extraTableTuple: (Seq[StructField], Boolean, TableIdentifier, Boolean, Seq[String],
                          Option[String], mutable.Map[String, String], Map[String, String], Seq[StructField],
                          Seq[PartitionerField], CarbonSpark2SqlParser, SparkSession,
                          Option[LogicalPlan])): LogicalPlan = {
    val (tableHeader, skewSpecContext, bucketSpecContext, partitionColumns, columns,
    tablePropertyList, locationSpecContext, tableComment, ctas, query, provider) = createTableTuple
    val (cols, external, tableIdentifier, ifNotExists, colNames, tablePath,
    tableProperties, properties, partitionByStructFields, partitionFields,
    parser, sparkSession, selectQuery) = extraTableTuple
    val options = new CarbonOption(properties)
    // validate streaming property
    validateStreamingProperty(options)
    var fields = parser.getFields(cols ++ partitionByStructFields)
    // validate for create table as select
    selectQuery match {
      case Some(q) =>
        // create table as select does not allow creation of partitioned table
        if (partitionFields.nonEmpty) {
          val errorMessage = "A Create Table As Select (CTAS) statement is not allowed to " +
            "create a partitioned table using Carbondata file formats."
          operationNotAllowed(errorMessage, partitionColumns)
        }
        // create table as select does not allow to explicitly specify schema
        if (fields.nonEmpty) {
          operationNotAllowed(
            "Schema may not be specified in a Create Table As Select (CTAS) statement", columns)
        }
        // external table is not allow
        if (external) {
          operationNotAllowed("Create external table as select", tableHeader)
        }
        fields = parser
          .getFields(CarbonEnv.getInstance(sparkSession).carbonMetaStore
            .getSchemaFromUnresolvedRelation(sparkSession, Some(q).get))
      case _ =>
      // ignore this case
    }
    val columnNames = fields.map(_.name.get)
    ParserUtil.checkIfDuplicateColumnExists(columns, tableIdentifier, columnNames)
    if (partitionFields.nonEmpty && options.isStreaming) {
      operationNotAllowed("Streaming is not allowed on partitioned table", partitionColumns)
    }

    if (!external && fields.isEmpty) {
      throw new MalformedCarbonCommandException("Creating table without column(s) is not supported")
    }
    if (external && fields.isEmpty && tableProperties.nonEmpty) {
      // as fields are always zero for external table, cannot validate table properties.
      operationNotAllowed(
        "Table properties are not supported for external table", tablePropertyList)
    }

    // Global dictionary is deprecated since 2.0
    if (tableProperties.contains(CarbonCommonConstants.DICTIONARY_INCLUDE) ||
      tableProperties.contains(CarbonCommonConstants.DICTIONARY_EXCLUDE)) {
      DeprecatedFeatureException.globalDictNotSupported()
    }

    val bucketFields = parser.getBucketFields(tableProperties, fields, options)
    var isTransactionalTable: Boolean = true

    val tableInfo = if (external) {
      if (fields.nonEmpty) {
        // user provided schema for this external table, this is not allow currently
        // see CARBONDATA-2866
        operationNotAllowed(
          "Schema must not be specified for external table", columns)
      }
      if (partitionByStructFields.nonEmpty) {
        operationNotAllowed(
          "Partition is not supported for external table", partitionColumns)
      }
      // read table info from schema file in the provided table path
      // external table also must convert table name to lower case
      val identifier = AbsoluteTableIdentifier.from(
        tablePath.get,
        CarbonEnv.getDatabaseName(tableIdentifier.database)(sparkSession).toLowerCase(),
        tableIdentifier.table.toLowerCase())
      val table = try {
        val schemaPath = CarbonTablePath.getSchemaFilePath(identifier.getTablePath)
        if (!FileFactory.isFileExist(schemaPath)) {
          if (provider.equalsIgnoreCase("'carbonfile'")) {
            SchemaReader.inferSchema(identifier, true)
          } else {
            isTransactionalTable = false
            SchemaReader.inferSchema(identifier, false)
          }
        } else {
          SchemaReader.getTableInfo(identifier)
        }
      } catch {
        case e: Throwable =>
          operationNotAllowed(s"Invalid table path provided: ${ tablePath.get } ", tableHeader)
      }

      // set "_external" property, so that DROP TABLE will not delete the data
      if (provider.equalsIgnoreCase("'carbonfile'")) {
        table.getFactTable.getTableProperties.put("_filelevelformat", "true")
        table.getFactTable.getTableProperties.put("_external", "false")
      } else {
        table.getFactTable.getTableProperties.put("_external", "true")
        table.getFactTable.getTableProperties.put("_filelevelformat", "false")
      }
      var isLocalDic_enabled = table.getFactTable.getTableProperties
        .get(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE)
      if (null == isLocalDic_enabled) {
        table.getFactTable.getTableProperties
          .put(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE,
            CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.LOCAL_DICTIONARY_SYSTEM_ENABLE,
                CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE_DEFAULT))
      }
      isLocalDic_enabled = table.getFactTable.getTableProperties
        .get(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE)
      if (CarbonScalaUtil.validateLocalDictionaryEnable(isLocalDic_enabled) &&
        isLocalDic_enabled.toBoolean) {
        val allColumns = table.getFactTable.getListOfColumns
        for (i <- 0 until allColumns.size()) {
          val cols = allColumns.get(i)
          if (cols.getDataType == DataTypes.STRING || cols.getDataType == DataTypes.VARCHAR) {
            cols.setLocalDictColumn(true)
          }
        }
        table.getFactTable.setListOfColumns(allColumns)
      }
      table
    } else {
      // prepare table model of the collected tokens
      val tableModel: TableModel = CarbonParserUtil.prepareTableModel(
        ifNotExists,
        convertDbNameToLowerCase(tableIdentifier.database),
        tableIdentifier.table.toLowerCase,
        fields,
        partitionFields,
        tableProperties,
        bucketFields,
        isAlterFlow = false,
        tableComment)
      TableNewProcessor(tableModel)
    }
    tableInfo.setTransactionalTable(isTransactionalTable)
    selectQuery match {
      case query@Some(q) =>
        CarbonCreateTableAsSelectCommand(
          tableInfo = tableInfo,
          query = query.get,
          ifNotExistsSet = ifNotExists,
          tableLocation = tablePath)
      case _ =>
        CarbonCreateTableCommand(
          tableInfo = tableInfo,
          ifNotExistsSet = ifNotExists,
          tableLocation = tablePath,
          external)
    }
  }



  def removeIsNotNullAttribute(condition: Expression,
                               pushDownNotNullFilter: Boolean): Expression = {
    val isPartialStringEnabled = CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING,
        CarbonCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING_DEFAULT)
      .equalsIgnoreCase("true")
    condition transform {
      // Like is possible only if user provides _ in between the string
      // _ in like means any single character wild card check.
      case IsNotNull(child: AttributeReference) => Literal(!pushDownNotNullFilter)
      case plan if (CarbonHiveIndexMetadataUtil.checkNIUDF(plan)) => Literal(true)
      case Like(left: AttributeReference, right: Literal) if (!isPartialStringEnabled) =>
        Literal(true)
      case EndsWith(left: AttributeReference,
      right: Literal) if (!isPartialStringEnabled) => Literal(true)
      case Contains(left: AttributeReference,
      right: Literal) if (!isPartialStringEnabled) => Literal(true)
    }
  }

  def hasStartsWith(condition: Expression): Boolean = {
    condition match {
      case Like(left: AttributeReference, right: Literal) => false
      case EndsWith(left: AttributeReference, right: Literal) => false
      case Contains(left: AttributeReference, right: Literal) => false
      case _ => true
    }
  }

  /**
   * This method will check whether the condition is valid for SI push down. If yes then return the
   * tableName which contains this condition
   *
   * @param condition
   * @param indexTableColumnsToTableMapping
   * @param pushDownRequired
   * @return
   */
  def isConditionColumnInIndexTable(condition: Expression,
                                    indexTableColumnsToTableMapping: mutable.Map[String, Set[String]],
                                    pushDownRequired: Boolean, pushDownNotNullFilter: Boolean): Option[String] = {
    // In case of Like Filter in OR, both the conditions should not be transformed
    // In case of like filter in And, only like filter should be removed and
    // other filter should be transformed with index table

    // In case NI condition with and, eg., NI(col1 = 'a') && col1 = 'b',
    // only col1 = 'b' should be pushed to index table.
    // In case NI condition with or, eg., NI(col1 = 'a') || col1 = 'b',
    // both the condition should not be pushed to index table.

    var tableName: Option[String] = None
    val doNotPushToSI = condition match {
      case IsNotNull(child: AttributeReference) => !pushDownNotNullFilter
      case Not(EqualTo(left: AttributeReference, right: Literal)) => true
      case Not(EqualTo(left: Cast, right: Literal))
        if left.child.isInstanceOf[AttributeReference] => true
      case Not(Like(left: AttributeReference, right: Literal)) => true
      case Not(In(left: AttributeReference, right: Seq[Expression])) => true
      case Not(Contains(left: AttributeReference, right: Literal)) => true
      case Not(EndsWith(left: AttributeReference, right: Literal)) => true
      case Not(StartsWith(left: AttributeReference, right: Literal)) => true
      case Like(left: AttributeReference, right: Literal) if (!pushDownRequired) => true
      case EndsWith(left: AttributeReference, right: Literal) if (!pushDownRequired) => true
      case Contains(left: AttributeReference, right: Literal) if (!pushDownRequired) => true
      case plan if (CarbonHiveIndexMetadataUtil.checkNIUDF(plan)) => true
      case _ => false
    }
    if (!doNotPushToSI) {
      val attributes = condition collect {
        case attributeRef: AttributeReference => attributeRef
      }
      var isColumnExistsInSITable = false
      breakable {
        indexTableColumnsToTableMapping.foreach { tableAndIndexColumn =>
          isColumnExistsInSITable = attributes
            .forall { attributeRef => tableAndIndexColumn._2
              .contains(attributeRef.name.toLowerCase)
            }
          if (isColumnExistsInSITable) {
            tableName = Some(tableAndIndexColumn._1)
            break
          }
        }
      }
    }
    tableName
  }


}
