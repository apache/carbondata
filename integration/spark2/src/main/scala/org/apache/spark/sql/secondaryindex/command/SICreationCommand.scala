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
import scala.language.implicitConversions

import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.command.DataCommand
import org.apache.spark.sql.hive.{CarbonHiveMetadataUtil, CarbonRelation}
import org.apache.spark.sql.secondaryindex.exception.IndexTableExistException
import org.apache.spark.sql.secondaryindex.load.CarbonInternalLoaderUtil
import org.apache.spark.sql.secondaryindex.util.{CarbonInternalScalaUtil, IndexTableUtil}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.datatype.{DataType, DataTypes}
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.{SchemaEvolution, SchemaEvolutionEntry, SchemaReader}
import org.apache.carbondata.core.metadata.schema.indextable.{IndexMetadata, IndexTableInfo}
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo, TableSchema}
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.service.impl.ColumnUniqueIdGenerator
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{CreateTablePostExecutionEvent, CreateTablePreExecutionEvent, OperationContext, OperationListenerBus}

class ErrorMessage(message: String) extends Exception(message) {
}

 /**
  * Command for index table creation
  * @param indexModel      SecondaryIndex model holding the index infomation
  * @param tableProperties SI table properties
  * @param isCreateSIndex  if false then will not create index table schema in the carbonstore
   *                        and will avoid dataload for SI creation.
  */
 private[sql] case class CreateIndexTable(indexModel: SecondaryIndex,
     tableProperties: scala.collection.mutable.Map[String, String],
     var isCreateSIndex: Boolean = true)
   extends DataCommand {

   val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

   override def processData(sparkSession: SparkSession): Seq[Row] = {
    val databaseName = CarbonEnv.getDatabaseName(indexModel.databaseName)(sparkSession)
    indexModel.databaseName = Some(databaseName)
    val tableName = indexModel.tableName
    val storePath = CarbonProperties.getStorePath
    val dbLocation = CarbonEnv.getDatabaseLocation(databaseName, sparkSession)
    val indexTableName = indexModel.indexTableName

    val tablePath = dbLocation + CarbonCommonConstants.FILE_SEPARATOR + indexTableName
     setAuditTable(databaseName, indexTableName)
     setAuditInfo(Map("Column names" -> indexModel.columnNames.toString(),
       "Parent TableName" -> indexModel.tableName,
       "SI Table Properties" -> tableProperties.toString()))
    LOGGER.info(
      s"Creating Index with Database name [$databaseName] and Index name [$indexTableName]")
    val catalog = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    val identifier = TableIdentifier(tableName, indexModel.databaseName)
    var carbonTable: CarbonTable = null
    var locks: List[ICarbonLock] = List()
    var oldIndexInfo = ""

    try {
      carbonTable = CarbonEnv.getCarbonTable(indexModel.databaseName, tableName)(sparkSession)
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
        throw new ErrorMessage(
          s"Parent Table  ${ carbonTable.getDatabaseName }." +
          s"${ carbonTable.getTableName }" +
          s" is Partition Table and Secondary index on Partition table is not supported ")
      }


      locks = acquireLockForSecondaryIndexCreation(carbonTable.getAbsoluteTableIdentifier)
      if (locks.isEmpty) {
        throw new ErrorMessage(s"Not able to acquire lock. Another Data Modification operation " +
                 s"is already in progress for either ${
                   carbonTable
                     .getDatabaseName
                 }. ${ carbonTable.getTableName } or ${
                   carbonTable
                     .getDatabaseName
                 } or  ${ indexTableName }. Please try after some time")
      }
      // get carbon table again to reflect any changes during lock acquire.
      carbonTable =
        CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .lookupRelation(indexModel.databaseName, tableName)(sparkSession)
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
      val indexTables = CarbonInternalScalaUtil.getIndexesTables(carbonTable)
      val indexTableExistsInCarbon = indexTables.asScala.contains(indexTableName)
      val indexTableExistsInHive = sparkSession.sessionState.catalog
        .tableExists(TableIdentifier(indexTableName, indexModel.databaseName))
      if (indexTableExistsInHive && isCreateSIndex) {
        LOGGER.error(
          s"Index creation with Database name [$databaseName] and index name " +
          s"[$indexTableName] failed. " +
          s"Index [$indexTableName] already exists under database [$databaseName]")
        throw new ErrorMessage(
          s"Index [$indexTableName] already exists under database [$databaseName]")
      } else if (((indexTableExistsInCarbon && !indexTableExistsInHive) ||
        (!indexTableExistsInCarbon && indexTableExistsInHive)) && isCreateSIndex) {
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
      val dimNames = dims.map(x => if (!x.isComplex) {
        x.getColName.toLowerCase()
      })
      val isMeasureColPresent = indexModel.columnNames.find(x => msrs.contains(x))
      if (isMeasureColPresent.isDefined) {
        throw new ErrorMessage(s"Secondary Index is not supported for measure column : ${
          isMeasureColPresent
            .get
        }")
      }
      if (indexModel.columnNames.exists(x => !dimNames.contains(x))) {
        throw new ErrorMessage(
          s"one or more specified index cols either does not exist or not a key column or complex" +
            s" column in " +
            s"table $databaseName.$tableName")
      }
      // Only Key cols are allowed while creating index table
      val isInvalidColPresent = indexModel.columnNames.find(x => !dimNames.contains(x))
      if (isInvalidColPresent.isDefined) {
        throw new ErrorMessage(s"Invalid column name found : ${ isInvalidColPresent.get }")
      }
      if (indexModel.columnNames.exists(x => !dimNames.contains(x))) {
        throw new ErrorMessage(
          s"one or more specified index cols does not exist or not a key column or complex column" +
            s" in " +
            s"table $databaseName.$tableName")
      }
      // Check for duplicate column names while creating index table
      indexModel.columnNames.groupBy(col => col).foreach(f => if (f._2.size > 1) {
        throw new ErrorMessage(s"Duplicate column name found : ${ f._1 }")
      })

      // No. of index table cols are more than parent table key cols
      if (indexModel.columnNames.size > dims.size) {
        throw new ErrorMessage(s"Number of columns in Index table cannot be more than " +
          "number of key columns in Source table")
      }

      var isColsIndexedAsPerTable = true
      for (i <- indexModel.columnNames.indices) {
        if (!dims(i).getColName.equalsIgnoreCase(indexModel.columnNames(i))) {
          isColsIndexedAsPerTable = false
        }
      }

      if (isColsIndexedAsPerTable) {
        throw new ErrorMessage(
          s"Index table column indexing order is same as Parent table column start order")
      }
      // Should not allow to create index on an index table
      val isIndexTable = carbonTable.isIndexTable
      if (isIndexTable) {
        throw new ErrorMessage(
          s"Table [$tableName] under database [$databaseName] is already an index table")
      }
      // Check whether index table column order is same as another index table column order
      oldIndexInfo = carbonTable.getIndexInfo
      if (null == oldIndexInfo) {
        oldIndexInfo = ""
      }
      val indexTableCols = indexModel.columnNames.asJava
      val indexInfo = IndexTableUtil.checkAndAddIndexTable(oldIndexInfo,
        new IndexTableInfo(databaseName, indexTableName,
          indexTableCols))
      val absoluteTableIdentifier = AbsoluteTableIdentifier.
        from(tablePath, databaseName, indexTableName)
      var tableInfo: TableInfo = null
      // if Register Index call then read schema file from the metastore
       if (!isCreateSIndex && indexTableExistsInHive) {
         tableInfo = SchemaReader.getTableInfo(absoluteTableIdentifier)
       } else {
         tableInfo = prepareTableInfo(carbonTable, databaseName,
           tableName, indexTableName, absoluteTableIdentifier)
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
        val indexTableMeta = new IndexMetadata(indexTableName, true, carbonTable.getTablePath)
        tableInfo.getFactTable.getTableProperties
          .put(tableInfo.getFactTable.getTableId, indexTableMeta.serialize)
        // set index information in parent table
        val parentIndexMetadata = if (
          carbonTable.getTableInfo.getFactTable.getTableProperties
            .get(carbonTable.getCarbonTableIdentifier.getTableId) != null) {
          IndexMetadata.deserialize(carbonTable.getTableInfo.getFactTable.getTableProperties
            .get(carbonTable.getCarbonTableIdentifier.getTableId))
        } else {
          new IndexMetadata(false)
        }
        parentIndexMetadata.addIndexTableInfo(indexTableName, indexTableCols)
        carbonTable.getTableInfo.getFactTable.getTableProperties
          .put(carbonTable.getCarbonTableIdentifier.getTableId, parentIndexMetadata.serialize)

        val cols = tableInfo.getFactTable.getListOfColumns.asScala.filter(!_.isInvisible)
        val fields = new Array[String](cols.size)
        cols.foreach(col =>
          fields(col.getSchemaOrdinal) =
            col.getColumnName + ' ' + checkAndPrepareDecimal(col))

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
                 |(${fields.mkString(",")})
                 |USING carbondata OPTIONS (tableName "$indexTableName",
                 |dbName "$databaseName", tablePath "$tablePath", path "$tablePath",
                 |parentTablePath "${carbonTable.getTablePath}", isIndexTable "true",
                 |isSITableEnabled "false", parentTableId
                 |"${carbonTable.getCarbonTableIdentifier.getTableId}",
                 |parentTableName "$tableName"$carbonSchemaString) """.stripMargin)
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
                'parentTableName'='$tableName', 'isIndexTable' = 'true', 'isSITableEnabled' =
                'false', 'parentTablePath' = '${carbonTable.getTablePath}',
                'parentTableId' = '${carbonTable.getCarbonTableIdentifier.getTableId}')""")
        }

        CarbonInternalScalaUtil.addIndexTableInfo(carbonTable, indexTableName, indexTableCols)

      CarbonHiveMetadataUtil.refreshTable(databaseName, indexTableName, sparkSession)

        sparkSession.sql(
          s"""ALTER TABLE $databaseName.$tableName SET SERDEPROPERTIES ('indexInfo' =
              |'$indexInfo')""".stripMargin)

      val tableIdent = TableIdentifier(tableName, Some(databaseName))

      // modify the tableProperties of mainTable by adding "indexTableExists" property
      CarbonInternalScalaUtil
        .addOrModifyTableProperty(carbonTable,
          Map("indexTableExists" -> "true"), needLock = false)(sparkSession)

      CarbonHiveMetadataUtil.refreshTable(databaseName, tableName, sparkSession)

      // refersh the parent table relation
      sparkSession.sessionState.catalog.refreshTable(identifier)
      // load data for secondary index
      if (isCreateSIndex) {
        LoadDataForSecondaryIndex(indexModel).run(sparkSession)
      }
      val indexTablePath = CarbonTablePath
        .getMetadataPath(tableInfo.getOrCreateAbsoluteTableIdentifier.getTablePath)
      val isMaintableSegEqualToSISegs = CarbonInternalLoaderUtil
        .checkMainTableSegEqualToSISeg(carbonTable.getMetadataPath, indexTablePath)
      if (isMaintableSegEqualToSISegs) {
        // enable the SI table
        sparkSession.sql(
          s"""ALTER TABLE $databaseName.$indexTableName SET
             |SERDEPROPERTIES ('isSITableEnabled' = 'true')""".stripMargin)
      }
      val createTablePostExecutionEvent: CreateTablePostExecutionEvent =
        CreateTablePostExecutionEvent(sparkSession, tableIdentifier)
      OperationListenerBus.getInstance.fireEvent(createTablePostExecutionEvent, operationContext)
      LOGGER.info(
        s"Index created with Database name [$databaseName] and Index name [$indexTableName]")
    } catch {
      case err@(_: ErrorMessage | _: IndexTableExistException) =>
        sys.error(err.getMessage)
      case ex@(_: IOException | _: ParseException) =>
        LOGGER.error(s"Index creation with Database name [$databaseName] " +
                     s"and Index name [$indexTableName] is failed")
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

  def prepareTableInfo(carbonTable: CarbonTable,
    databaseName: String, tableName: String, indexTableName: String,
    absoluteTableIdentifier: AbsoluteTableIdentifier): TableInfo = {
    var schemaOrdinal = -1
    var allColumns = indexModel.columnNames.map { indexCol =>
      val colSchema = carbonTable.getDimensionByName(indexCol).getColumnSchema
      schemaOrdinal += 1
      cloneColumnSchema(colSchema, schemaOrdinal)
    }
    // Setting TRUE on all sort columns
    allColumns.foreach(f => f.setSortColumn(true))

    val encoders = new util.ArrayList[Encoding]()
    schemaOrdinal += 1
    val blockletId: ColumnSchema = getColumnSchema(databaseName,
      DataTypes.STRING,
      CarbonCommonConstants.POSITION_REFERENCE,
      encoders,
      isDimensionCol = true,
      0,
      0,
      schemaOrdinal)
    // sort column proeprty should be true for implicit no dictionary column position reference
    // as there exist a same behavior for no dictionary columns by default
    blockletId.setSortColumn(true)
    // set the blockletId column as local dict column implicit no dictionary column position
    // reference
    blockletId.setLocalDictColumn(true)
    schemaOrdinal += 1
    val dummyMeasure: ColumnSchema = getColumnSchema(databaseName,
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
    setLocalDictionaryConfigs(tablePropertiesMap,
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
     val isLocalDictEnabledFormainTable = parentTblPropertiesMap
       .get(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE)
     indexTblPropertiesMap
       .put(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE,
         isLocalDictEnabledFormainTable)
     indexTblPropertiesMap
       .put(CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD,
         parentTblPropertiesMap.asScala
           .getOrElse(CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD,
             CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD_DEFAULT))
     var localDictColumns: scala.collection.mutable.Seq[String] = scala.collection.mutable.Seq()
     allColumns.foreach(column =>
       if (column.isLocalDictColumn) {
         localDictColumns :+= column.getColumnName
       }
     )
     if (isLocalDictEnabledFormainTable != null && isLocalDictEnabledFormainTable.toBoolean) {
       indexTblPropertiesMap
         .put(CarbonCommonConstants.LOCAL_DICTIONARY_INCLUDE,
           localDictColumns.mkString(","))
     }
   }


  def acquireLockForSecondaryIndexCreation(absoluteTableIdentifier: AbsoluteTableIdentifier):
  List[ICarbonLock] = {
    var configuredMdtPath = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_UPDATE_SYNC_FOLDER,
        CarbonCommonConstants.CARBON_UPDATE_SYNC_FOLDER_DEFAULT).trim
    configuredMdtPath = CarbonUtil.checkAndAppendFileSystemURIScheme(configuredMdtPath)
    val metadataLock = CarbonLockFactory
      .getCarbonLockObj(absoluteTableIdentifier,
        LockUsage.METADATA_LOCK)
    val alterTableCompactionLock = CarbonLockFactory
      .getCarbonLockObj(absoluteTableIdentifier,
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

  private def checkAndPrepareDecimal(columnSchema: ColumnSchema): String = {
    columnSchema.getDataType.getName.toLowerCase match {
      case "decimal" => "decimal(" + columnSchema.getPrecision + "," + columnSchema.getScale + ")"
      case others => others
    }
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

  def cloneColumnSchema(parentColumnSchema: ColumnSchema, schemaOrdinal: Int): ColumnSchema = {
    val columnSchema = new ColumnSchema()
    columnSchema.setDataType(parentColumnSchema.getDataType)
    columnSchema.setColumnName(parentColumnSchema.getColumnName)
    columnSchema.setColumnProperties(parentColumnSchema.getColumnProperties)
    columnSchema.setEncodingList(parentColumnSchema.getEncodingList)
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

   override protected def opName: String = "SI Creation"
}
