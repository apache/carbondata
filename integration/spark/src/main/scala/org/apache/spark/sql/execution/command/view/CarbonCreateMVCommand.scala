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

package org.apache.spark.sql.execution.command.view

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

import org.apache.log4j.Logger
import org.apache.spark.sql.{CarbonEnv, CarbonSource, Row, SparkSession}
import org.apache.spark.sql.catalyst.{CarbonParserUtil, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast, Coalesce, Expression, Literal, ScalaUDF}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average}
import org.apache.spark.sql.catalyst.plans.logical.{Join, Limit, LogicalPlan, Sort}
import org.apache.spark.sql.execution.command.{AtomicRunnableCommand, Field, PartitionerField, TableModel, TableNewProcessor}
import org.apache.spark.sql.execution.command.table.{CarbonCreateTableCommand, CarbonDropTableCommand}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.parser.MVQueryParser
import org.apache.spark.sql.types.{ArrayType, DateType, MapType, StructType}

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedMVCommandException}
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, RelationIdentifier}
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.view._
import org.apache.carbondata.events.{withEvents, OperationContext, OperationListenerBus}
import org.apache.carbondata.mv.plans.modular.{GroupBy, ModularPlan, SimpleModularizer}
import org.apache.carbondata.mv.plans.util.{BirdcageOptimizer, SQLBuilder}
import org.apache.carbondata.spark.util.CommonUtil
import org.apache.carbondata.view._

/**
 * Create Materialized View Command implementation
 * It will create the MV table, load the MV table (if deferred rebuild is false),
 * and register the MV schema in [[MVManager]]
 */
case class CarbonCreateMVCommand(
    databaseNameOption: Option[String],
    name: String,
    properties: Map[String, String],
    queryString: String,
    ifNotExistsSet: Boolean = false,
    deferredRefresh: Boolean = false)
  extends AtomicRunnableCommand {

  private val logger = CarbonCreateMVCommand.LOGGER

  private var viewSchema: MVSchema = _

  override def processMetadata(session: SparkSession): Seq[Row] = {
    setAuditInfo(Map("mvName" -> name) ++ properties)
    checkProperties(mutable.Map[String, String](properties.toSeq: _*))
    val viewManager = MVManagerInSpark.get(session)
    val databaseName = databaseNameOption.getOrElse(session.sessionState.catalog.getCurrentDatabase)
    if (viewManager.getSchema(databaseName, name) != null) {
      if (!ifNotExistsSet) {
        throw new MalformedMVCommandException(
          s"Materialized view with name ${ databaseName }.${ name } already exists")
      } else {
        return Seq.empty
      }
    }

    val identifier = TableIdentifier(name, Option(databaseName))
    val databaseLocation = viewManager.getDatabaseLocation(databaseName)
    val systemDirectoryPath = CarbonProperties.getInstance()
      .getSystemFolderLocationPerDatabase(FileFactory
        .getCarbonFile(databaseLocation)
        .getCanonicalPath)
    withEvents(CreateMVPreExecutionEvent(session, systemDirectoryPath, identifier),
      CreateMVPostExecutionEvent(session, systemDirectoryPath, identifier)) {
      // get mv catalog
      val viewCatalog = MVManagerInSpark.getOrReloadMVCatalog(session)
      val schema = doCreate(session, identifier, viewManager, viewCatalog)

      // Update the related mv tables property to mv fact tables
      MVHelper.addOrModifyMVTablesMap(session, schema)

      try {
        viewCatalog.registerSchema(schema)
        if (schema.isRefreshOnManual) {
          viewManager.setStatus(schema.getIdentifier, MVStatus.DISABLED)
        }
      } catch {
        case exception: Exception =>
          val dropTableCommand = CarbonDropTableCommand(
            ifExistsSet = true,
            Option(databaseName),
            name,
            dropChildTable = true)
          dropTableCommand.run(session)
          viewManager.deleteSchema(databaseName, name)
          throw exception
      }
      this.viewSchema = schema
    }
    Seq.empty
  }

  override def processData(session: SparkSession): Seq[Row] = {
    if (this.viewSchema != null && !deferredRefresh) {
      MVRefresher.refresh(this.viewSchema, session)
      val viewManager = MVManagerInSpark.get(session)
      viewManager.setStatus(this.viewSchema.getIdentifier, MVStatus.ENABLED)
    }
    Seq.empty
  }

  override def undoMetadata(sparkSession: SparkSession, exception: Exception): Seq[Row] = {
    if (this.schema != null) {
      CarbonDropMVCommand(databaseNameOption, name, ifExistsSet = true).run(
        sparkSession)
    }
    Seq.empty
  }

  override protected def opName: String = "CREATE MATERIALIZED VIEW"

  private def doCreate(session: SparkSession,
      tableIdentifier: TableIdentifier,
      viewManager: MVManagerInSpark,
      viewCatalog: MVCatalogInSpark): MVSchema = {
    val logicalPlan = MVHelper.dropDummyFunction(
      MVQueryParser.getQueryPlan(queryString, session))
      // check if mv with same query already exists
    val mvSchemaWrapper = viewCatalog.getMVWithSameQueryPresent(logicalPlan)
    if (mvSchemaWrapper.nonEmpty) {
      val mvWithSameQuery = mvSchemaWrapper.get.viewSchema.getIdentifier.getTableName
      throw new MalformedMVCommandException(
        s"MV with the name `$mvWithSameQuery` has been already created with the same query")
    }
    val modularPlan = checkQuery(logicalPlan)
    val viewSchema = getOutputSchema(logicalPlan)
    val relatedTables = getRelatedTables(logicalPlan)
    val relatedTableList = toCarbonTables(session, relatedTables)
    val inputCols = logicalPlan.output.map(x =>
      x.name
    ).toList
    val relatedTableNames = new util.ArrayList[String](relatedTableList.size())
    // Check if load is in progress in any of the parent table mapped to the indexSchema
    relatedTableList.asScala.foreach {
      table =>
        val tableProperties = table.getTableInfo.getFactTable.getTableProperties.asScala
        // validate for spatial index column
        val spatialProperty = tableProperties.get(CarbonCommonConstants.SPATIAL_INDEX)
        if (spatialProperty.isDefined) {
          val spatialColumn = spatialProperty.get.trim
          if (inputCols.contains(spatialColumn)) {
            val errorMessage =
              s"$spatialColumn is a spatial index column and is not allowed for " +
              s"the option(s): MATERIALIZED VIEW"
            throw new MalformedCarbonCommandException(errorMessage)
          }
        }
        if (!table.getTableInfo.isTransactionalTable) {
          throw new MalformedCarbonCommandException(
            "Cannot create mv on non-transactional table")
        }
        if (table.isMV) {
          throw new MalformedCarbonCommandException(
            "Cannot create mv on mv table " + table.getTableUniqueName)
        }
        if (table.isStreamingSink) {
          throw new MalformedCarbonCommandException(
            "Cannot create mv on stream table " + table.getTableUniqueName)
        }
        if (SegmentStatusManager.isLoadInProgressInTable(table)) {
          throw new UnsupportedOperationException(
            "Cannot create mv when insert is in progress on table " + table.getTableUniqueName)
        }
        relatedTableNames.add(table.getTableName)
    }

    val viewRefreshMode = if (checkIsQueryNeedFullRefresh(logicalPlan) ||
      checkIsHasNonCarbonTable(relatedTables)) {
      MVProperty.REFRESH_MODE_FULL
    } else {
      MVProperty.REFRESH_MODE_INCREMENTAL
    }
    val viewRefreshTriggerMode = if (deferredRefresh) {
      MVProperty.REFRESH_TRIGGER_MODE_ON_MANUAL
    } else {
      properties.getOrElse(MVProperty.REFRESH_TRIGGER_MODE,
        MVProperty.REFRESH_TRIGGER_MODE_ON_COMMIT)
    }

    val viewProperties = mutable.Map[String, String]()
    viewProperties.put(
      CarbonCommonConstants.MV_RELATED_TABLES, relatedTableNames.asScala.mkString(","))

    val (timeSeriesColumn, granularity) = checkTimeSeriesQuery(logicalPlan, viewRefreshTriggerMode)

    val fieldsMap = MVHelper.getFieldsMapFromPlan(
      new SQLBuilder(modularPlan).SQLizer.execute(modularPlan), getLogicalRelation(logicalPlan))
    // If MV is mapped to single main table, then inherit table properties from main table,
    // else, will use default table properties. If DMProperties contains table properties, then
    // table properties of indexSchema table will be updated
    if (relatedTableList.size() == 1 && CarbonSource.isCarbonDataSource(relatedTables.head)) {
      inheritTablePropertiesFromRelatedTable(
        relatedTableList.get(0),
        fieldsMap,
        viewSchema,
        viewProperties)
      if (granularity != null) {
        val timeSeriesDataType = relatedTableList.get(0).getTableInfo.getFactTable
          .getListOfColumns.asScala
          .filter(column => column.getColumnName.equalsIgnoreCase(timeSeriesColumn))
          .head.getDataType
        if (timeSeriesDataType.equals(DataTypes.DATE) ||
          timeSeriesDataType.equals(DataTypes.TIMESTAMP)) {
          // if data type is of Date type, then check if given granularity is valid for date type
          if (timeSeriesDataType.equals(DataTypes.DATE)) {
            checkTimeSeriesGranularityForDate(granularity)
          }
        } else {
          throw new MalformedCarbonCommandException(
            "TimeSeries Column must be of TimeStamp or Date type")
        }
      }
    }
    properties.foreach(t => viewProperties.put(t._1, t._2))

    // TODO mv table support partition
    // Inherit partition from related table if mv is mapped to single related table
    val viewPartitionerFields = if (relatedTableList.size() == 1) {
      val relatedTablePartitionColumns =
        if (properties.getOrElse("partitioning", "true").toBoolean &&
          relatedTableList.get(0).isHivePartitionTable) {
          relatedTableList.get(0).getPartitionInfo
            .getColumnSchemaList.asScala.map(_.getColumnName)
        } else {
          Seq.empty
        }
      getViewPartitionerFields(relatedTablePartitionColumns, fieldsMap)
    } else {
      Seq.empty
    }

    val columnOrderMap = new java.util.HashMap[Integer, String]()
    if (viewPartitionerFields.nonEmpty) {
      viewSchema.zipWithIndex.foreach {
        case (viewField, index) =>
          columnOrderMap.put(index, viewField.column)
      }
    }

    // prepare table model of the collected tokens
    val viewTableModel: TableModel = CarbonParserUtil.prepareTableModel(
      ifNotExistPresent = ifNotExistsSet,
      CarbonParserUtil.convertDbNameToLowerCase(tableIdentifier.database),
      tableIdentifier.table.toLowerCase,
      viewSchema,
      viewPartitionerFields,
      viewProperties,
      None,
      isAlterFlow = false,
      None)

    val viewTablePath = if (viewProperties.contains("path")) {
      viewProperties("path")
    } else {
      CarbonEnv.getTablePath(viewTableModel.databaseNameOp, viewTableModel.tableName)(session)
    }
    CarbonCreateTableCommand(TableNewProcessor(viewTableModel),
      viewTableModel.ifNotExistsSet, Some(viewTablePath), isVisible = false).run(session)

    // Build and create mv schema
    // Map list of main table columns mapped to MV table and add to indexSchema
    val relatedTableToColumnsMap = new java.util.HashMap[String, util.Set[String]]()
    for (viewField <- fieldsMap.values) {
      viewField.relatedFieldList.foreach {
        relatedField =>
          if (null == relatedTableToColumnsMap.get(relatedField.tableName)) {
            val columns = new util.HashSet[String]()
            columns.add(relatedField.fieldName.toLowerCase())
            relatedTableToColumnsMap.put(relatedField.tableName, columns)
          } else {
            relatedTableToColumnsMap.get(relatedField.tableName)
              .add(relatedField.fieldName.toLowerCase())
          }
      }
    }
    val relatedTableIds = relatedTables.map { table =>
      val relatedTableId = new RelationIdentifier(table.database, table.identifier.table, "")
      relatedTableId.setTablePath(FileFactory.getUpdatedFilePath(table.location.toString))
      relatedTableId.setProvider(table.provider.get)
      relatedTableId
    }
    val viewIdentifier = new RelationIdentifier(
        tableIdentifier.database.get, tableIdentifier.table,
        CarbonEnv.getCarbonTable(tableIdentifier)(session).getTableId)
    viewIdentifier.setTablePath(viewTablePath)
    val schema = new MVSchema(viewManager)
    schema.setIdentifier(viewIdentifier)
    schema.setProperties(mutable.Map[String, String](viewProperties.toSeq: _*).asJava)
    schema.setRelatedTableColumnList(relatedTableToColumnsMap)
    schema.setColumnsOrderMap(columnOrderMap)
    schema.setRelatedTables(new util.ArrayList[RelationIdentifier](relatedTableIds.asJava))
    schema.getProperties.put(MVProperty.REFRESH_MODE, viewRefreshMode)
    schema.getProperties.put(MVProperty.REFRESH_TRIGGER_MODE, viewRefreshTriggerMode)
    if (null != granularity && null != timeSeriesColumn) {
      schema.setTimeSeries(true)
    }
    schema.setQuery(queryString)
    try {
      viewManager.createSchema(schema.getIdentifier.getDatabaseName, schema)
    } catch {
      case exception: Exception =>
        val dropTableCommand = CarbonDropTableCommand(
          ifExistsSet = true,
          Option(schema.getIdentifier.getDatabaseName),
          schema.getIdentifier.getTableName,
          dropChildTable = true,
          isInternalCall = true)
        dropTableCommand.run(session)
        throw exception
    }
    schema
  }

  private def toCarbonTables(session: SparkSession,
      catalogTables: Seq[CatalogTable]): util.List[CarbonTable] = {
    val tableList = new util.ArrayList[CarbonTable](catalogTables.size)
    catalogTables.foreach { catalogTable =>
      val table = CarbonEnv.getAnyTable(
        catalogTable.identifier.database, catalogTable.identifier.table)(session)
      tableList.add(table)
    }
    tableList
  }

  // Return all relations involved in the plan
  private def getRelatedTables(logicalPlan: LogicalPlan): Seq[CatalogTable] = {
    logicalPlan.collect {
      case relation: LogicalRelation => relation.catalogTable.get
      case relation: HiveTableRelation => relation.tableMeta
    }
  }

  private def getLogicalRelation(logicalPlan: LogicalPlan): Seq[LogicalRelation] = {
    logicalPlan.collect {
      case l: LogicalRelation => l
    }
  }

  private def getOutputSchema(logicalPlan: LogicalPlan): Seq[Field] = {
    var attributeIndex = 0
    logicalPlan.output.map { attribute =>
      if (attribute.dataType.isInstanceOf[ArrayType] ||
          attribute.dataType.isInstanceOf[StructType] ||
          attribute.dataType.isInstanceOf[MapType]) {
        throw new UnsupportedOperationException(
          s"Materialized view is not supported for complex datatype columns and " +
            s"complex datatype return types of function :" + attribute.name)
      }
      val attributeName = MVHelper.getUpdatedColumnName(attribute, attributeIndex)
      attributeIndex += 1
      val rawSchema = '`' + attributeName + '`' + ' ' + attribute.dataType.typeName
      if (attribute.dataType.typeName.startsWith("decimal")) {
        val (precision, scale) = CommonUtil.getScaleAndPrecision(attribute.dataType.catalogString)
        Field(column = attributeName,
          dataType = Some(attribute.dataType.typeName),
          name = Some(attributeName),
          children = None,
          precision = precision,
          scale = scale,
          rawSchema = rawSchema)
      } else {
        Field(column = attributeName,
          dataType = Some(attribute.dataType.typeName),
          name = Some(attributeName),
          children = None,
          rawSchema = rawSchema)
      }
    }.distinct
  }

  private def getViewColumns(
      relatedTableColumns: Array[String],
      fieldsMap: scala.collection.mutable.LinkedHashMap[Field, MVField],
      viewSchema: Seq[Field]) = {
    val viewColumns = relatedTableColumns.flatMap(
      relatedTableColumn =>
        viewSchema.collect {
          case viewField
            if fieldsMap(viewField).aggregateFunction.isEmpty &&
               fieldsMap(viewField).relatedFieldList.size == 1 &&
               fieldsMap(viewField).relatedFieldList.head.fieldName
                 .equalsIgnoreCase(relatedTableColumn) =>
            viewField.column
        })
    viewColumns
  }

  /**
   * Used to extract PartitionerFields for MV.
   * This method will keep generating partitionerFields until the sequence of
   * partition column is broken.
   *
   * For example: if x,y,z are partition columns in main table then child tables will be
   * partitioned only if the child table has List("x,y,z", "x,y", "x") as the projection columns.
   *
   */
  private def getViewPartitionerFields(relatedTablePartitionColumns: Seq[String],
      fieldsMap: mutable.LinkedHashMap[Field, MVField]): Seq[PartitionerField] = {
    @scala.annotation.tailrec
    def generatePartitionerField(partitionColumn: List[String],
      partitionerFields: Seq[PartitionerField]): Seq[PartitionerField] = {
      partitionColumn match {
        case head :: tail =>
          // Collect the first relation which matched the condition
          val validField = fieldsMap.zipWithIndex.collectFirst {
            case ((field, viewField), _) if
            viewField.relatedFieldList.nonEmpty &&
              head.equals(viewField.relatedFieldList.head.fieldName) &&
              viewField.aggregateFunction.isEmpty =>
              (PartitionerField(field.name.get,
                field.dataType,
                field.columnComment), relatedTablePartitionColumns.indexOf(head))
          }
          if (validField.isDefined) {
            val (partitionerField, index) = validField.get
            // if relation is found then check if the partitionerFields already found are equal
            // to the index of this element.
            // If x with index 1 is found then there should be exactly 1 element already found.
            // If z with index 2 comes directly after x then this check will be false are 1
            // element is skipped in between and index would be 2 and number of elements found
            // would be 1. In that case return empty sequence so that the aggregate table is not
            // partitioned on any column.
            if (index == partitionerFields.length) {
              generatePartitionerField(tail, partitionerFields :+ partitionerField)
            } else {
              Seq.empty
            }
          } else {
            // if not found then continue search for the rest of the elements. Because the rest
            // of the elements can also decide if the table has to be partitioned or not.
            generatePartitionerField(tail, partitionerFields)
          }
        case Nil =>
          // if end of list then return fields.
          partitionerFields
      }
    }
    generatePartitionerField(relatedTablePartitionColumns.toList, Seq.empty)
  }

  private def checkQuery(logicalPlan: LogicalPlan): ModularPlan = {
    // if there is limit in query string, throw exception, as its not a valid use case
    logicalPlan match {
      case Limit(_, _) =>
        throw new MalformedCarbonCommandException("Materialized view does not support the query " +
                                                  "with limit")
      case _ =>
    }

    // Order by columns needs to be present in projection list for creating mv. This is because,
    // we have to perform order by on all segments during query, which requires the order by column
    // data
    logicalPlan.transform {
      case sort@Sort(order, _, _) =>
        order.map { orderByCol =>
          orderByCol.child match {
            case attr: AttributeReference =>
              if (!logicalPlan.output.contains(attr.toAttribute)) {
                throw new UnsupportedOperationException(
                  "Order by column `" + attr.name + "` must be present in project columns")
              }
          }
          order
        }
        sort
    }

    val modularPlan =
      SimpleModularizer.modularize(BirdcageOptimizer.execute(logicalPlan)).next().semiHarmonized
    // Only queries which can be select , predicate , join, group by and having queries.
    if (!modularPlan.isSPJGH) {
      throw new UnsupportedOperationException("MV is not supported for this query")
    }
    val isValid = modularPlan match {
      case groupBy: GroupBy =>
        // Make sure all predicates are present in projections.
        groupBy.predicateList.forall {
          predicate =>
            groupBy.outputList.exists {
              case alias: Alias =>
                alias.semanticEquals(predicate) || alias.child.semanticEquals(predicate)
              case other => other.semanticEquals(predicate)
            }
        }
      case _ => true
    }
    if (!isValid) {
      throw new UnsupportedOperationException(
        "Group by columns must be present in project columns")
    }
    var expressionValid = true
    modularPlan.transformExpressions {
      case coalesce@Coalesce(_) if coalesce.children.exists(
        expression => expression.isInstanceOf[AggregateExpression]) =>
        expressionValid = false
        coalesce
    }
    if (!expressionValid) {
      throw new UnsupportedOperationException("MV doesn't support Coalesce")
    }
    modularPlan
  }

  private def checkProperties(properties: mutable.Map[String, String]): Unit = {
    // TODO check with white list will be better.
    if (properties.contains("streaming") && properties("streaming").equalsIgnoreCase("true")) {
      throw new MalformedCarbonCommandException(
        s"Materialized view does not support streaming")
    }
    val unsupportedPropertyKeys = Array(
      "sort_columns",
      "local_dictionary_include",
      "local_dictionary_exclude",
      "long_string_columns",
      "no_inverted_index",
      "inverted_index",
      "column_meta_cache",
      "range_column")
    val unsupportedProperties = properties.filter(
      property => unsupportedPropertyKeys.exists(key => key.equalsIgnoreCase(property._1)))
    if (unsupportedProperties.nonEmpty) {
      throw new MalformedMVCommandException(
        "Properties " + unsupportedProperties.keySet.mkString(",") +
        " are not allowed for this materialized view")
    }
  }

  /**
   * Return true if we can do incremental load on the mv table based on the query plan.
   * Some cases like aggregation functions which are present inside other expressions
   * like sum(a)+sum(b) cannot be incremental loaded.
   */
  private def checkIsQueryNeedFullRefresh(logicalPlan: LogicalPlan): Boolean = {
    var needFullRefresh = false
    logicalPlan.transformAllExpressions {
      case alias: Alias => alias
      case aggregate: AggregateExpression =>
        // If average function present then go for full refresh
        val reload = aggregate.aggregateFunction match {
          case _: Average => true
          case _ => false
        }
        needFullRefresh = reload || needFullRefresh
        aggregate
      case cast: Cast =>
        needFullRefresh = cast.child.find {
          case _: AggregateExpression => false
          case _ => false
        }.isDefined || needFullRefresh
        cast
      case expression: Expression =>
        // Check any aggregation function present inside other expression.
        needFullRefresh = expression.find {
          case _: AggregateExpression => true
          case _ => false
        }.isDefined || needFullRefresh
        expression
    }
    // TODO:- Remove this case when incremental data loading is supported for multiple tables
    logicalPlan.transformDown {
      case join: Join =>
        needFullRefresh = true
        join
    }
    needFullRefresh
  }

  /**
   * Return true if we can do incremental load on the mv table based on data source
   *
   * @return
   */
  private def checkIsHasNonCarbonTable(mainTables: Seq[CatalogTable]): Boolean = {
    mainTables.exists(table => !CarbonSource.isCarbonDataSource(table))
  }

  /**
   * Validate mv timeseries query for timeseries column and granularity.
   * TimeSeries udf function will contain data type as TimeStamp/cast as TimeStamp
   *
   * @param logicalPlan   to be validated
   * @param viewRefreshTriggerModel to check if it is lazy/non-lazy mv
   * @return
   */
  private def checkTimeSeriesQuery(logicalPlan: LogicalPlan,
      viewRefreshTriggerModel: String): (String, String) = {
    var timeSeriesColumn: String = null
    var granularity: String = null
    logicalPlan.transformExpressions {
      case alias@Alias(function: ScalaUDF, _) =>
        if (function.function.isInstanceOf[TimeSeriesFunction]) {
          if (null == timeSeriesColumn && null == granularity) {
            function.children.collect {
              case reference: AttributeReference =>
                timeSeriesColumn = reference.name
              case literal: Literal =>
                granularity = literal.value.toString
              case cast: Cast =>
                cast.child match {
                  case reference: AttributeReference =>
                    if (reference.dataType.isInstanceOf[DateType]) {
                      timeSeriesColumn = reference.name
                    }
                  case _ =>
                }
            }
          } else {
            function.children.collect {
              case reference: AttributeReference =>
                if (!reference.name.equalsIgnoreCase(timeSeriesColumn)) {
                  throw new MalformedCarbonCommandException(
                    "Multiple timeseries udf functions are defined in Select statement with " +
                      "different timestamp columns")
                }
              case literal: Literal =>
                if (!granularity.equalsIgnoreCase(literal.value.toString)) {
                  throw new MalformedCarbonCommandException(
                    "Multiple timeseries udf functions are defined in Select statement with " +
                      "different granularities")
                }
            }
          }
        }
        alias
    }
    // timeseries column and granularity is not null, then validate
    if (null != timeSeriesColumn && null != granularity) {
      if (viewRefreshTriggerModel.equalsIgnoreCase(
        MVProperty.REFRESH_TRIGGER_MODE_ON_MANUAL)) {
        throw new MalformedCarbonCommandException(
          "MV TimeSeries queries does not support refresh on manual")
      }
      checkTimeSeriesGranularity(granularity)
    } else if (null == timeSeriesColumn && null != granularity) {
      throw new MalformedCarbonCommandException(
        "MV TimeSeries is only supported on Timestamp/Date column")
    }
    (timeSeriesColumn, granularity)
  }

  /**
   * validate TimeSeries Granularity
   *
   * @param timeSeriesFunction user defined granularity
   */
  private def checkTimeSeriesGranularity(timeSeriesFunction: String): Unit = {
    var found = false
    breakable {
      for (granularity <- MVTimeGranularity.getAll()) {
        if (timeSeriesFunction.equalsIgnoreCase(granularity.name)) {
          found = true
          break
        }
      }
    }
    if (!found) {
      throw new MalformedCarbonCommandException("Granularity " + timeSeriesFunction + " is invalid")
    }
  }

  private def checkTimeSeriesGranularityForDate(timeSeriesFunction: String): Unit = {
    for (granularity <- MVTimeGranularity.getAll()) {
      if (timeSeriesFunction.equalsIgnoreCase(granularity.name)) {
        if (granularity.seconds < 24 * 60 * 60) {
          throw new MalformedCarbonCommandException(
            "Granularity should be of DAY/WEEK/MONTH/YEAR, for timeseries column of Date type")
        }
      }
    }
  }

  private def inheritTablePropertiesFromRelatedTable(
      relatedTable: CarbonTable,
      fieldsMap: scala.collection.mutable.LinkedHashMap[Field, MVField],
      viewSchema: Seq[Field],
      viewProperties: mutable.Map[String, String]): Unit = {
    val sortColumns = relatedTable.getSortColumns.toArray(new Array[String](0))
    val viewTableOrder = getViewColumns(sortColumns, fieldsMap, viewSchema)
    if (viewTableOrder.nonEmpty) {
      viewProperties.put(CarbonCommonConstants.SORT_COLUMNS, viewTableOrder.mkString(","))
    }
    val relatedTableProperties = relatedTable.getTableInfo.getFactTable.getTableProperties.asScala
    relatedTableProperties.get("sort_scope").foreach(x => viewProperties.put("sort_scope", x))
    viewProperties
      .put(CarbonCommonConstants.TABLE_BLOCKSIZE, relatedTable.getBlockSizeInMB.toString)
    viewProperties.put(CarbonCommonConstants.FLAT_FOLDER,
      relatedTableProperties.getOrElse(
        CarbonCommonConstants.FLAT_FOLDER, CarbonCommonConstants.DEFAULT_FLAT_FOLDER))

    // MV table name and columns are automatically added prefix with parent table name
    // in carbon. For convenient, users can type column names same as the ones in select statement
    // when config properties, and here we update column names with prefix.
    // If longStringColumn is not present in dm properties then we take long_string_columns from
    // the parent table.
    var longStringColumn = viewProperties.get(CarbonCommonConstants.LONG_STRING_COLUMNS)
    if (longStringColumn.isEmpty) {
      val longStringColumnInRelatedTables = relatedTableProperties
        .getOrElse(CarbonCommonConstants.LONG_STRING_COLUMNS, "").split(",").map(_.trim)
      val varcharColumnInRelatedTables = scala.collection.mutable.ArrayBuffer.empty[String]
      fieldsMap foreach (fields => {
        val aggregateFunction = fields._2.aggregateFunction
        val relatedFieldList = fields._2.relatedFieldList
        // check if columns present in mv are long_string_col in parent table. If they are
        // long_string_columns in parent, make them long_string_columns in mv
        if (aggregateFunction.isEmpty &&
            relatedFieldList.size == 1 &&
            longStringColumnInRelatedTables.contains(relatedFieldList.head.fieldName)) {
          varcharColumnInRelatedTables += relatedFieldList.head.fieldName
        }
      })
      if (varcharColumnInRelatedTables.nonEmpty) {
        longStringColumn = Option(varcharColumnInRelatedTables.mkString(","))
      }
    }

    if (longStringColumn.isDefined) {
      val fieldNames = viewSchema.map(_.column)
      val newLongStringColumn = longStringColumn.get.split(",").map(_.trim).map {
        columnName =>
          val newColumnName = relatedTable.getTableName.toLowerCase() + "_" + columnName
          if (!fieldNames.contains(newColumnName)) {
            throw new MalformedCarbonCommandException(
              CarbonCommonConstants.LONG_STRING_COLUMNS.toUpperCase() + ":" + columnName
              + " does not in mv")
          }
          newColumnName
      }
      viewProperties.put(CarbonCommonConstants.LONG_STRING_COLUMNS,
        newLongStringColumn.mkString(","))
    }
    // inherit compressor property
    viewProperties
      .put(CarbonCommonConstants.COMPRESSOR,
        relatedTable.getTableInfo.getFactTable.getTableProperties.asScala
          .getOrElse(CarbonCommonConstants.COMPRESSOR,
            CompressorFactory.getInstance().getCompressor.getName))

    // inherit the local dictionary properties of main parent table
    viewProperties
      .put(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE,
        relatedTable.getTableInfo.getFactTable.getTableProperties.asScala
          .getOrElse(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE, "false"))
    viewProperties
      .put(CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD,
        relatedTable.getTableInfo.getFactTable.getTableProperties.asScala
          .getOrElse(CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD,
            CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD_DEFAULT))
    val parentDictInclude = relatedTable.getTableInfo.getFactTable.getTableProperties.asScala
      .getOrElse(CarbonCommonConstants.LOCAL_DICTIONARY_INCLUDE, "").split(",")
    val parentDictExclude = relatedTable.getTableInfo.getFactTable.getTableProperties.asScala
      .getOrElse(CarbonCommonConstants.LOCAL_DICTIONARY_EXCLUDE, "").split(",")
    val newLocalDictInclude = getViewColumns(parentDictInclude, fieldsMap, viewSchema)
    val newLocalDictExclude = getViewColumns(parentDictExclude, fieldsMap, viewSchema)
    if (newLocalDictInclude.nonEmpty) {
      viewProperties
        .put(CarbonCommonConstants.LOCAL_DICTIONARY_INCLUDE, newLocalDictInclude.mkString(","))
    }
    if (newLocalDictExclude.nonEmpty) {
      viewProperties
        .put(CarbonCommonConstants.LOCAL_DICTIONARY_EXCLUDE, newLocalDictExclude.mkString(","))
    }
    val parentInvertedIndex = relatedTable.getTableInfo.getFactTable.getTableProperties.asScala
      .getOrElse(CarbonCommonConstants.INVERTED_INDEX, "").split(",")
    val newInvertedIndex = getViewColumns(parentInvertedIndex, fieldsMap, viewSchema)
    val parentNoInvertedIndex = relatedTable.getTableInfo.getFactTable.getTableProperties.asScala
      .getOrElse(CarbonCommonConstants.NO_INVERTED_INDEX, "").split(",")
    val newNoInvertedIndex =
      getViewColumns(parentNoInvertedIndex, fieldsMap, viewSchema)
    if (newInvertedIndex.nonEmpty) {
      viewProperties.put(CarbonCommonConstants.INVERTED_INDEX, newInvertedIndex.mkString(","))
    }
    if (newNoInvertedIndex.nonEmpty) {
      viewProperties.put(CarbonCommonConstants.NO_INVERTED_INDEX, newNoInvertedIndex.mkString(","))
    }
  }
}

object CarbonCreateMVCommand {

  private val LOGGER: Logger = LogServiceFactory.getLogService(
    classOf[CarbonCreateMVCommand].getCanonicalName)

}
