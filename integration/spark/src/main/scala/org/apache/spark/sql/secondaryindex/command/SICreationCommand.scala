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

package org.apache.spark.sql.secondaryindex.command

import java.io.IOException
import java.util
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.DataCommand
import org.apache.spark.sql.hive.{CarbonHiveIndexMetadataUtil, CarbonRelation}
import org.apache.spark.sql.index.{CarbonIndexUtil, IndexTableUtil}
import org.apache.spark.sql.secondaryindex.exception.IndexTableExistException
import org.apache.spark.sql.secondaryindex.load.CarbonInternalLoaderUtil

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.status.IndexStatus
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.datatype.{DataType, DataTypes}
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.index.IndexType
import org.apache.carbondata.core.metadata.schema.{SchemaEvolution, SchemaEvolutionEntry, SchemaReader}
import org.apache.carbondata.core.metadata.schema.indextable.{IndexMetadata, IndexTableInfo}
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo, TableSchema}
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.service.impl.ColumnUniqueIdGenerator
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{CreateTablePostExecutionEvent, CreateTablePreExecutionEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.spark.util.CarbonSparkUtil

class ErrorMessage(message: String) extends Exception(message) {
}

/**
 * Command for index table creation
 *
 * @param indexModel        SecondaryIndex model holding the index information
 * @param tableProperties   SI table properties
 * @param ifNotExists       true if IF NOT EXISTS is set
 * @param isDeferredRefresh true if WITH DEFERRED REFRESH is set
 * @param isCreateSIndex    if false then will not create index table schema in the carbon store
 *                          and will avoid data load for SI creation.
 */
private[sql] case class CarbonCreateSecondaryIndexCommand(
    indexModel: IndexModel,
    tableProperties: mutable.Map[String, String],
    ifNotExists: Boolean,
    isDeferredRefresh: Boolean,
    var isCreateSIndex: Boolean = true)
  extends DataCommand {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    if (isDeferredRefresh) {
      throw new UnsupportedOperationException("DEFERRED REFRESH is not supported")
    }
    val databaseName = CarbonEnv.getDatabaseName(indexModel.dbName)(sparkSession)
    indexModel.dbName = Some(databaseName)
    val tableName = indexModel.tableName
    val storePath = CarbonProperties.getStorePath
    val dbLocation = CarbonEnv.getDatabaseLocation(databaseName, sparkSession)
    val indexTableName = indexModel.indexName

    val tablePath: String = if (isCreateSIndex) {
      dbLocation + CarbonCommonConstants.FILE_SEPARATOR + indexTableName
    } else {
      tableProperties("tablePath")
    }
    setAuditTable(databaseName, indexTableName)
    setAuditInfo(Map(
      "Column names" -> indexModel.columnNames.toString(),
      "Parent TableName" -> indexModel.tableName,
      "SI Table Properties" -> tableProperties.toString()))
    LOGGER.info(
      s"Creating Index with Database name [$databaseName] and Index name [$indexTableName]")
    val identifier = TableIdentifier(tableName, indexModel.dbName)
    var carbonTable: CarbonTable = null
    var locks: List[ICarbonLock] = List()
    var oldIndexInfo = ""

    try {
      carbonTable = CarbonEnv.getCarbonTable(indexModel.dbName, tableName)(sparkSession)
      if (carbonTable == null) {
        throw new ErrorMessage(s"Parent Table $databaseName.$tableName is not found")
      }

      if (carbonTable != null &&
          (carbonTable.isFileLevelFormat || !carbonTable.getTableInfo.isTransactionalTable)) {
        throw new MalformedCarbonCommandException(
          "Unsupported operation on non transactional table")
      }

      if (carbonTable.isStreamingSink) {
        throw new ErrorMessage(
          s"Parent Table  ${ carbonTable.getDatabaseName }." +
          s"${ carbonTable.getTableName }" +
          s" is Streaming Table and Secondary index on Streaming table is not supported ")
      }

      if (carbonTable.isHivePartitionTable) {
        val isPartitionColumn = indexModel.columnNames.exists {
          siColumns => carbonTable.getTableInfo
            .getFactTable
            .getPartitionInfo
            .getColumnSchemaList
            .asScala
            .exists(_.getColumnName.equalsIgnoreCase(siColumns))
        }
        if (isPartitionColumn) {
          throw new UnsupportedOperationException(
            "Secondary Index cannot be created on a partition column.")
        }
      }

      locks = acquireLockForSecondaryIndexCreation(carbonTable.getAbsoluteTableIdentifier)
      if (locks.isEmpty) {
        throw new ErrorMessage(
          s"Not able to acquire lock. Another Data Modification operation " +
          s"is already in progress for either ${carbonTable.getDatabaseName}." +
          s"${carbonTable.getTableName} or ${carbonTable.getDatabaseName} or " +
          s"$indexTableName. Please try after some time")
      }
      // get carbon table again to reflect any changes during lock acquire.
      carbonTable =
        CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .lookupRelation(indexModel.dbName, tableName)(sparkSession)
          .asInstanceOf[CarbonRelation].carbonTable
      if (carbonTable == null) {
        throw new ErrorMessage(s"Parent Table $databaseName.$tableName is not found")
      }
      //      storePath = carbonTable.getTablePath

      // check if index table being created is a stale index table for the same or other table
      // in current database. Following cases are possible for checking stale scenarios
      // Case1: table exists in hive but deleted in carbon
      // Case2: table exists in carbon but deleted in hive
      // Case3: table neither exists in hive nor in carbon but stale folders are present for the
      // index table being created
      val indexTables = CarbonIndexUtil.getSecondaryIndexes(carbonTable)
      val indexTableExistsInCarbon = indexTables.asScala.contains(indexTableName)
      val indexTableExistsInHive = sparkSession.sessionState.catalog
        .tableExists(TableIdentifier(indexTableName, indexModel.dbName))
      val isRegisterIndex = !isCreateSIndex
      if (indexTableExistsInHive && isCreateSIndex) {
        if (!ifNotExists) {
          LOGGER.error(
            s"Index creation with Database name [$databaseName] and index name " +
            s"[$indexTableName] failed. " +
            s"Index [$indexTableName] already exists under database [$databaseName]")
          throw new ErrorMessage(
            s"Index [$indexTableName] already exists under database [$databaseName]")
        } else {
          return Seq.empty
        }
      } else if (indexTableExistsInCarbon && !indexTableExistsInHive && isCreateSIndex) {
        LOGGER.error(
          s"Index with [$indexTableName] under database [$databaseName] is present in " +
          s"stale state.")
        throw new ErrorMessage(
          s"Index with [$indexTableName] under database [$databaseName] is present in " +
          s"stale state. Please use drop index if exists command to delete the index table")
      } else if (!indexTableExistsInCarbon && !indexTableExistsInHive && isCreateSIndex) {
        val indexTableStorePath = storePath + CarbonCommonConstants.FILE_SEPARATOR + databaseName +
                                  CarbonCommonConstants.FILE_SEPARATOR + indexTableName
        if (CarbonUtil.isFileExists(indexTableStorePath)) {
          LOGGER.error(
            s"Index with [$indexTableName] under database [$databaseName] is present in " +
            s"stale state.")
          throw new ErrorMessage(
            s"Index with [$indexTableName] under database [$databaseName] is present in " +
            s"stale state. Please use drop index if exists command to delete the index " +
            s"table")
        }
      }
      val dims = carbonTable.getVisibleDimensions.asScala
      val msrs = carbonTable.getVisibleMeasures.asScala
        .map(x => if (!x.isComplex) {
          x.getColName
        })
      val dimNames = dims.map(x => if (DataTypes.isArrayType(x.getDataType) || !x.isComplex) {
        x.getColName.toLowerCase()
      })
      val isMeasureColPresent = indexModel.columnNames.find(x => msrs.contains(x))
      if (isMeasureColPresent.isDefined) {
        throw new ErrorMessage(s"Secondary Index is not supported for measure column : ${
          isMeasureColPresent
            .get
        }")
      }
      val properties = carbonTable.getTableInfo.getFactTable.getTableProperties.asScala
      val spatialProperty = properties.get(CarbonCommonConstants.SPATIAL_INDEX)
      if (spatialProperty.isDefined) {
        if (indexModel.columnNames.exists(x => x.equalsIgnoreCase(spatialProperty.get.trim))) {
          throw new ErrorMessage(s"Secondary Index is not supported for Spatial index column:" +
                                 s" ${ spatialProperty.get.trim }")
        }
      }
      // No. of index table cols are more than parent table key cols
      if (indexModel.columnNames.size > dims.size) {
        throw new ErrorMessage(s"Number of columns in Index table cannot be more than " +
          "number of key columns in Source table")
      }
      if (indexModel.columnNames.exists(x => !dimNames.contains(x))) {
        if (isRegisterIndex) {
          throw new ErrorMessage(s"Cannot Register Secondary index table $indexTableName, " +
                                 s"as it has column(s) which does not exists in $tableName. " +
                                 s"Try Drop and recreate SI.")
        }
        throw new ErrorMessage(
          s"one or more specified index cols either does not exist or not a key column or complex" +
          s" column in table $databaseName.$tableName")
      }
      // Check for duplicate column names while creating index table
      indexModel.columnNames.groupBy(col => col).foreach(f => if (f._2.size > 1) {
        throw new ErrorMessage(s"Duplicate column name found : ${ f._1 }")
      })

      // Should not allow to create index on an index table
      val isIndexTable = carbonTable.isIndexTable
      if (isIndexTable) {
        throw new ErrorMessage(
          s"Table [$tableName] under database [$databaseName] is already an index table")
      }
      val absoluteTableIdentifier = AbsoluteTableIdentifier.
        from(tablePath, databaseName, indexTableName)
      val indexTablePath = CarbonTablePath
        .getMetadataPath(absoluteTableIdentifier.getTablePath)
      val mainTblLoadMetadataDetails: Array[LoadMetadataDetails] =
        SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
      var siTblLoadMetadataDetails: Array[LoadMetadataDetails] =
        SegmentStatusManager.readLoadMetadata(indexTablePath)
      if (isRegisterIndex) {
        // check if SI segments are more than main table segments
        CarbonInternalLoaderUtil
          .checkMainTableSegEqualToSISeg(mainTblLoadMetadataDetails,
            siTblLoadMetadataDetails, isRegisterIndex)
        // check if SI table has undergone any Update or delete operation, which can happen in
        // case of compatibility scenario. IUD after Refresh SI and before register index
        val updatedSegmentsCount = siTblLoadMetadataDetails.filter(loadMetaDetail =>
          !loadMetaDetail.getUpdateStatusFileName.equals(""))
        if (!updatedSegmentsCount.isEmpty) {
          throw new ErrorMessage(s"Cannot Register Secondary index table $indexTableName" +
                                 ", as it has undergone update or delete operation. " +
                                 "Try Drop and recreate SI.")
        }
      }

      // creation of index on long string or binary columns are not supported
      val errorMsg = "one or more index columns specified contains long string or binary column" +
        s" in table $databaseName.$tableName. SI cannot be created on " +
        s"long string or binary columns."
      dims.filter(dimension => indexModel.columnNames
        .contains(dimension.getColName))
        .map(_.getDataType).foreach(dataType =>
        if (dataType.equals(DataTypes.VARCHAR) || dataType.equals(DataTypes.BINARY)) {
          throw new ErrorMessage(errorMsg)
        })

      // Check whether index table column order is same as another index table column order
      oldIndexInfo = carbonTable.getIndexInfo
      if (null == oldIndexInfo) {
        oldIndexInfo = ""
      }
      val indexProperties = new util.HashMap[String, String]
      val indexTableCols = indexModel.columnNames.asJava
      indexProperties.put(CarbonCommonConstants.INDEX_COLUMNS, indexTableCols.asScala.mkString(","))
      indexProperties.put(CarbonCommonConstants.INDEX_PROVIDER,
        IndexType.SI.getIndexProviderName)
      indexProperties.put(CarbonCommonConstants.INDEX_STATUS, IndexStatus.ENABLED.name())
      val indexInfo = IndexTableUtil.checkAndAddIndexTable(
        oldIndexInfo,
        new IndexTableInfo(
          databaseName, indexTableName,
          indexProperties),
        true)
      var tableInfo: TableInfo = null
      // if Register Index call then read schema file from the metastore
      if (!isCreateSIndex && indexTableExistsInHive) {
        tableInfo = SchemaReader.getTableInfo(absoluteTableIdentifier)
      } else {
        tableInfo = prepareTableInfo(
          carbonTable, databaseName,
          tableName, indexTableName, absoluteTableIdentifier)
      }
      if (isRegisterIndex && null != tableInfo.getFactTable.getSchemaEvolution &&
          null != carbonTable.getTableInfo.getFactTable.getSchemaEvolution) {
        // check if SI table has undergone any alter schema operation before registering it
        val indexTableSchemaEvolutionEntryList = tableInfo
          .getFactTable
          .getSchemaEvolution
          .getSchemaEvolutionEntryList
        val mainTableSchemaEvolutionEntryList = carbonTable
          .getTableInfo
          .getFactTable
          .getSchemaEvolution
          .getSchemaEvolutionEntryList
        if (indexTableSchemaEvolutionEntryList.size() > mainTableSchemaEvolutionEntryList.size()) {
          val index = mainTableSchemaEvolutionEntryList.size()
          for (i <- index until indexTableSchemaEvolutionEntryList.size()) {
            val schemaEntry = indexTableSchemaEvolutionEntryList.get(i)
            val isSITableRenamed =
              (schemaEntry.getAdded == null && schemaEntry.getRemoved == null) ||
              (schemaEntry.getAdded.isEmpty && schemaEntry.getRemoved.isEmpty)
            if (!isSITableRenamed) {
              throw new ErrorMessage(s"Cannot Register Secondary index table $indexTableName" +
                                     ", as it has undergone column schema addition or deletion. " +
                                     "Try Drop and recreate SI.")
            }
          }
        }
      }
      if (!isCreateSIndex && !indexTableExistsInHive) {
        LOGGER.error(
          s"Index registration with Database name [$databaseName] and index name " +
          s"[$indexTableName] failed. " +
          s"Index [$indexTableName] does not exists under database [$databaseName]")
        throw new ErrorMessage(
          s"Index [$indexTableName] does not exists under database [$databaseName]")
      }
      // Need to fill partitioner class when we support partition
      val tableIdentifier = AbsoluteTableIdentifier
        .from(tablePath, databaseName, indexTableName)
      // Add Database to catalog and persist
      val catalog = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      //        val tablePath = tableIdentifier.getTablePath
      val carbonSchemaString = catalog.generateTableSchemaString(tableInfo, tableIdentifier)
      // set index information in index table
      val indexTableMeta = new IndexMetadata(tableName, true, carbonTable.getTablePath)
      tableInfo.getFactTable.getTableProperties
        .put(tableInfo.getFactTable.getTableId, indexTableMeta.serialize)
      // set index information in parent table
      IndexTableUtil.addIndexInfoToParentTable(carbonTable,
        IndexType.SI.getIndexProviderName,
        indexTableName,
        indexProperties)
      val carbonRelation = CarbonSparkUtil.createCarbonRelation(tableInfo, tablePath)
      val rawSchema = CarbonSparkUtil.getRawSchema(carbonRelation)
      val operationContext = new OperationContext
      val createTablePreExecutionEvent: CreateTablePreExecutionEvent =
        CreateTablePreExecutionEvent(sparkSession, tableIdentifier, Option(tableInfo))
      OperationListenerBus.getInstance.fireEvent(createTablePreExecutionEvent, operationContext)
      // do not create index table for register table call
      // only the alter the existing table to set index related info
      if (isCreateSIndex) {
        try {
          sparkSession.sql(
            s"""CREATE TABLE $databaseName.$indexTableName
               |($rawSchema)
               |USING carbondata OPTIONS (tableName "$indexTableName",
               |dbName "$databaseName", tablePath "$tablePath", path "$tablePath",
               |parentTablePath "${ carbonTable.getTablePath }", isIndexTable "true",
               |parentTableId "${ carbonTable.getCarbonTableIdentifier.getTableId }",
               |parentTableName "$tableName"$carbonSchemaString) """.stripMargin)
            .collect()
        } catch {
          case e: IOException =>
            if (FileFactory.isFileExist(tablePath)) {
              val si_dir = FileFactory.getCarbonFile(tablePath)
              CarbonUtil.deleteFoldersAndFilesSilent(si_dir)
            }
            throw e
        }
      } else {
        sparkSession.sql(
          s"""ALTER TABLE $databaseName.$indexTableName SET SERDEPROPERTIES (
                'parentTableName'='$tableName', 'isIndexTable' = 'true', 'parentTablePath' =
                '${carbonTable.getTablePath}',
                'parentTableId' = '${carbonTable.getCarbonTableIdentifier.getTableId}')""")
          .collect()

        checkAndRemoveIndexTableExistsProperty(sparkSession, indexTableName)

        // Refresh the index table
        CarbonEnv
          .getInstance(sparkSession)
          .carbonMetaStore
          .lookupRelation(indexModel.dbName, indexTableName)(sparkSession)
          .asInstanceOf[CarbonRelation]
          .carbonTable
      }

      CarbonIndexUtil.addIndexTableInfo(IndexType.SI.getIndexProviderName,
        carbonTable,
        indexTableName,
        indexProperties)

      CarbonHiveIndexMetadataUtil.refreshTable(databaseName, indexTableName, sparkSession)

      sparkSession.sql(
        s"""ALTER TABLE $databaseName.$tableName SET SERDEPROPERTIES ('indexInfo' =
           |'$indexInfo')""".stripMargin).collect()

      val tableIdent = TableIdentifier(tableName, Some(databaseName))

      // modify the tableProperties of mainTable by adding "indexTableExists" property
      CarbonIndexUtil
        .addOrModifyTableProperty(
          carbonTable,
          Map("indexTableExists" -> "true"), needLock = false)(sparkSession)

      CarbonHiveIndexMetadataUtil.refreshTable(databaseName, tableName, sparkSession)

      // refresh the parent table relation
      sparkSession.sessionState.catalog.refreshTable(identifier)
      // load data for secondary index
      if (isCreateSIndex) {
        LoadDataForSecondaryIndex(indexModel).run(sparkSession)
      }
      val createTablePostExecutionEvent: CreateTablePostExecutionEvent =
        CreateTablePostExecutionEvent(sparkSession, tableIdentifier)
      OperationListenerBus.getInstance.fireEvent(createTablePostExecutionEvent, operationContext)
      LOGGER.info(
        s"Index created with Database name [$databaseName] and Index name [$indexTableName]")
    } catch {
      case err@(_: ErrorMessage | _: IndexTableExistException) =>
        if (err.getMessage.contains("Index Table with selected columns already exist") &&
            !isCreateSIndex) {
          checkAndRemoveIndexTableExistsProperty(sparkSession, indexTableName)
          LOGGER.warn(s"Table [$indexTableName] has been already registered as Secondary " +
                      s"Index table with table [$databaseName.${ indexModel.tableName }].")
        } else {
          sys.error(err.getMessage)
        }
      case ex@(_: IOException | _: ParseException) =>
        LOGGER.error(s"Index creation with Database name [$databaseName] " +
                     s"and Index name [$indexTableName] is failed")
        throw ex
      case e: Exception =>
        LOGGER.error(s"Index creation with Database name [$databaseName] " +
                     s"and Index name [$indexTableName] is Successful, But the data load to index" +
                     s" table is failed")
        throw e
    }
    finally {
      if (locks.nonEmpty) {
        releaseLocks(locks)
      }
    }
    Seq.empty
  }

  private def checkAndRemoveIndexTableExistsProperty(sparkSession: SparkSession,
      indexTableName: String): Unit = {
    // modify the tableProperties of indexTable by removing "indexTableExists" property
    val indexTable = CarbonEnv.getCarbonTable(indexModel.dbName, indexTableName)(sparkSession)
    if (indexTable.getTableInfo.getFactTable.getTableProperties.containsKey("indextableexists")) {
      CarbonIndexUtil
        .addOrModifyTableProperty(
          indexTable,
          Map(), needLock = false, "indextableexists")(sparkSession)
    }
  }

  def prepareTableInfo(carbonTable: CarbonTable,
      databaseName: String, tableName: String, indexTableName: String,
      absoluteTableIdentifier: AbsoluteTableIdentifier): TableInfo = {
    var schemaOrdinal = -1
    var allColumns = List[ColumnSchema]()
    var complexColumnExists = false
    indexModel.columnNames.foreach { indexCol =>
      val dimension = carbonTable.getDimensionByName(indexCol)
      schemaOrdinal += 1
      if (dimension.isComplex) {
        if (complexColumnExists) {
          throw new ErrorMessage(
            "SI creation with more than one complex type is not supported yet")
        }
        if (dimension.getNumberOfChild > 0) {
          val complexChildDims = dimension.getListOfChildDimensions.asScala
          // For complex columns, SI creation on only array of primitive types is allowed.
          // Check if child column is of complex type and throw exception.
          if (complexChildDims.exists(col => col.isComplex)) {
            throw new ErrorMessage(
              "SI creation with nested array complex type is not supported yet")
          }
        }
        allColumns = allColumns :+ cloneColumnSchema(
          dimension.getColumnSchema,
          schemaOrdinal,
          dimension.getListOfChildDimensions.get(0).getColumnSchema.getDataType)
        complexColumnExists = true
      } else {
        val colSchema = dimension.getColumnSchema
        allColumns = allColumns :+ cloneColumnSchema(colSchema, schemaOrdinal)
      }
    }
    // Setting TRUE on all sort columns
    allColumns.foreach(f => f.setSortColumn(true))

    val encoders = new util.ArrayList[Encoding]()
    schemaOrdinal += 1
    val blockletId: ColumnSchema = getColumnSchema(
      databaseName,
      DataTypes.STRING,
      CarbonCommonConstants.POSITION_REFERENCE,
      encoders,
      isDimensionCol = true,
      0,
      0,
      schemaOrdinal)
    // sort column property should be true for implicit no dictionary column position reference
    // as there exist a same behavior for no dictionary columns by default
    blockletId.setSortColumn(true)
    // set the blockletId column as local dict column implicit no dictionary column position
    // reference
    blockletId.setLocalDictColumn(true)
    schemaOrdinal += 1
    val dummyMeasure: ColumnSchema = getColumnSchema(
      databaseName,
      DataType.getDataType(DataType.DOUBLE_MEASURE_CHAR),
      CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE,
      encoders,
      isDimensionCol = false,
      0,
      0,
      schemaOrdinal)
    dummyMeasure.setInvisible(true)

    allColumns = allColumns ++ Seq(blockletId, dummyMeasure)
    val tableInfo = new TableInfo()
    val tableSchema = new TableSchema()
    val schemaEvol = new SchemaEvolution()
    schemaEvol
      .setSchemaEvolutionEntryList(new util.ArrayList[SchemaEvolutionEntry]())
    tableSchema.setTableId(UUID.randomUUID().toString)
    tableSchema.setTableName(indexTableName)
    tableSchema.setListOfColumns(allColumns.asJava)
    tableSchema.setSchemaEvolution(schemaEvol)
    // populate table properties map
    val tablePropertiesMap = new java.util.HashMap[String, String]()
    tableProperties.foreach {
      x => tablePropertiesMap.put(x._1, x._2)
    }
    // inherit and set the local dictionary properties from parent table
    setLocalDictionaryConfigs(
      tablePropertiesMap,
      carbonTable.getTableInfo.getFactTable.getTableProperties, allColumns)

    // block SI creation when the parent table has flat folder structure
    if (carbonTable.getTableInfo.getFactTable.getTableProperties
          .containsKey(CarbonCommonConstants.FLAT_FOLDER) &&
        carbonTable.getTableInfo.getFactTable.getTableProperties
          .get(CarbonCommonConstants.FLAT_FOLDER).toBoolean) {
      LOGGER.error(
        s"Index creation with Database name [$databaseName] and index name " +
        s"[$indexTableName] failed. " +
        s"Index table creation is not permitted on table with flat folder structure")
      throw new ErrorMessage(
        "Index table creation is not permitted on table with flat folder structure")
    }
    tableSchema.setTableProperties(tablePropertiesMap)
    tableInfo.setDatabaseName(databaseName)
    tableInfo.setTableUniqueName(CarbonTable.buildUniqueName(databaseName, indexTableName))
    tableInfo.setLastUpdatedTime(System.currentTimeMillis())
    tableInfo.setFactTable(tableSchema)
    tableInfo.setTablePath(absoluteTableIdentifier.getTablePath)
    tableInfo
  }

  /**
   * This function inherits and sets the local dictionary properties from parent table to index
   * table properties
   */
  def setLocalDictionaryConfigs(indexTblPropertiesMap: java.util.HashMap[String, String],
      parentTblPropertiesMap: java.util.Map[String, String],
      allColumns: List[ColumnSchema]): Unit = {
    val isLocalDictEnabledForMainTable = parentTblPropertiesMap
      .get(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE)
    indexTblPropertiesMap
      .put(
        CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE,
        isLocalDictEnabledForMainTable)
    indexTblPropertiesMap
      .put(
        CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD,
        parentTblPropertiesMap.asScala
          .getOrElse(
            CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD,
            CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD_DEFAULT))
    var localDictColumns: scala.collection.mutable.Seq[String] = scala.collection.mutable.Seq()
    allColumns.foreach(column =>
      if (column.isLocalDictColumn) {
        localDictColumns :+= column.getColumnName
      }
    )
    if (isLocalDictEnabledForMainTable != null && isLocalDictEnabledForMainTable.toBoolean) {
      indexTblPropertiesMap
        .put(
          CarbonCommonConstants.LOCAL_DICTIONARY_INCLUDE,
          localDictColumns.mkString(","))
    }
  }


  def acquireLockForSecondaryIndexCreation(absoluteTableIdentifier: AbsoluteTableIdentifier):
  List[ICarbonLock] = {
    var configuredMdtPath = CarbonProperties.getInstance()
      .getProperty(
        CarbonCommonConstants.CARBON_UPDATE_SYNC_FOLDER,
        CarbonCommonConstants.CARBON_UPDATE_SYNC_FOLDER_DEFAULT).trim
    configuredMdtPath = CarbonUtil.checkAndAppendFileSystemURIScheme(configuredMdtPath)
    val metadataLock = CarbonLockFactory
      .getCarbonLockObj(
        absoluteTableIdentifier,
        LockUsage.METADATA_LOCK)
    val alterTableCompactionLock = CarbonLockFactory
      .getCarbonLockObj(
        absoluteTableIdentifier,
        LockUsage.COMPACTION_LOCK
      )
    val deleteSegmentLock =
      CarbonLockFactory
        .getCarbonLockObj(absoluteTableIdentifier, LockUsage.DELETE_SEGMENT_LOCK)
    if (metadataLock.lockWithRetries() && alterTableCompactionLock.lockWithRetries() &&
        deleteSegmentLock.lockWithRetries()) {
      logInfo("Successfully able to get the table metadata file, compaction and delete segment " +
              "lock")
      List(metadataLock, alterTableCompactionLock, deleteSegmentLock)
    }
    else {
      List.empty
    }
  }

  def releaseLocks(locks: List[ICarbonLock]): Unit = {
    CarbonLockUtil.fileUnlock(locks.head, LockUsage.METADATA_LOCK)
    CarbonLockUtil.fileUnlock(locks(1), LockUsage.COMPACTION_LOCK)
    CarbonLockUtil.fileUnlock(locks(2), LockUsage.DELETE_SEGMENT_LOCK)
  }

  def getColumnSchema(databaseName: String, dataType: DataType, colName: String,
      encoders: java.util.List[Encoding], isDimensionCol: Boolean,
      precision: Integer, scale: Integer, schemaOrdinal: Int): ColumnSchema = {
    val columnSchema = new ColumnSchema()
    columnSchema.setDataType(dataType)
    columnSchema.setColumnName(colName)
    val colPropMap = new java.util.HashMap[String, String]()
    columnSchema.setColumnProperties(colPropMap)
    columnSchema.setEncodingList(encoders)
    val colUniqueIdGenerator = ColumnUniqueIdGenerator.getInstance
    val columnUniqueId = colUniqueIdGenerator.generateUniqueId(columnSchema)
    columnSchema.setColumnUniqueId(columnUniqueId)
    columnSchema.setColumnReferenceId(columnUniqueId)
    columnSchema.setDimensionColumn(isDimensionCol)
    columnSchema.setPrecision(precision)
    columnSchema.setScale(scale)
    columnSchema.setSchemaOrdinal(schemaOrdinal)
    columnSchema
  }

  def cloneColumnSchema(parentColumnSchema: ColumnSchema,
      schemaOrdinal: Int,
      dataType: DataType = null): ColumnSchema = {
    val columnSchema = new ColumnSchema()
    val encodingList = parentColumnSchema.getEncodingList
    var colPropMap = parentColumnSchema.getColumnProperties
    // if data type is arrayType, then store the column as its CHILD data type in SI
    if (DataTypes.isArrayType(parentColumnSchema.getDataType)) {
      columnSchema.setDataType(dataType)
      if (colPropMap == null) {
        colPropMap = new java.util.HashMap[String, String]()
      }
      colPropMap.put("isParentColumnComplex", "true")
      columnSchema.setColumnProperties(colPropMap)
      if (dataType == DataTypes.DATE) {
        encodingList.add(Encoding.DIRECT_DICTIONARY)
        encodingList.add(Encoding.DICTIONARY)
      }
    } else {
      columnSchema.setDataType(parentColumnSchema.getDataType)
      columnSchema.setColumnProperties(colPropMap)
    }
    columnSchema.setColumnName(parentColumnSchema.getColumnName)
    columnSchema.setEncodingList(encodingList)
    columnSchema.setColumnUniqueId(parentColumnSchema.getColumnUniqueId)
    columnSchema.setColumnReferenceId(parentColumnSchema.getColumnReferenceId)
    columnSchema.setDimensionColumn(parentColumnSchema.isDimensionColumn)
    columnSchema.setPrecision(parentColumnSchema.getPrecision)
    columnSchema.setScale(parentColumnSchema.getScale)
    columnSchema.setSchemaOrdinal(schemaOrdinal)
    columnSchema.setSortColumn(parentColumnSchema.isSortColumn)
    columnSchema.setLocalDictColumn(parentColumnSchema.isLocalDictColumn)
    columnSchema
  }

  override def clone(): LogicalPlan = {
    CarbonCreateSecondaryIndexCommand(indexModel,
      tableProperties,
      ifNotExists,
      isDeferredRefresh,
      isCreateSIndex)
  }

  override protected def opName: String = "SI Creation"
}
