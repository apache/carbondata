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

package org.apache.spark.sql.execution.command

import java.util
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.language.implicitConversions
import org.apache.spark.SparkEnv
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, Literal}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.{CarbonHiveMetadataUtil, CarbonRelation, HiveContext}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.util.FileUtils
import org.codehaus.jackson.map.ObjectMapper
import org.apache.carbondata.common.factory.CarbonCommonFactory
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.carbon.{CarbonDataLoadSchema, CarbonTableIdentifier}
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata
import org.apache.carbondata.core.carbon.metadata.datatype.DataType
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding
import org.apache.carbondata.core.carbon.metadata.schema.{SchemaEvolution, SchemaEvolutionEntry}
import org.apache.carbondata.core.carbon.metadata.schema.table.{CarbonTable, TableInfo, TableSchema}
import org.apache.carbondata.core.carbon.metadata.schema.table.column.{CarbonDimension, ColumnSchema}
import org.apache.carbondata.core.carbon.path.CarbonStorePath
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastorage.store.impl.FileFactory
import org.apache.carbondata.core.load.LoadMetadataDetails
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.integration.spark.merger.CompactionType
import org.apache.carbondata.lcm.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.lcm.status.SegmentStatusManager
import org.apache.carbondata.processing.constants.TableOptionConstant
import org.apache.carbondata.processing.etl.DataLoadingException
import org.apache.carbondata.processing.model.CarbonLoadModel
import org.apache.carbondata.spark.CarbonSparkFactory
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.load._
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory
import org.apache.carbondata.spark.util.{CarbonScalaUtil, DataTypeConverterUtil, GlobalDictionaryUtil}

//
//object TableProcessor {
//  def apply(cm: TableModel, sqlContext: SQLContext): Table = {
//    new TableProcessor(cm, sqlContext).process()
//  }
//}
//
//class TableProcessor(cm: TableModel, sqlContext: SQLContext) {
//  val timeDims = Seq("TimeYears", "TimeMonths", "TimeDays", "TimeHours", "TimeMinutes")
//  val numericTypes = Seq(CarbonCommonConstants.INTEGER_TYPE, CarbonCommonConstants.DOUBLE_TYPE,
//    CarbonCommonConstants.LONG_TYPE, CarbonCommonConstants.FLOAT_TYPE)
//
//  def getAllChildren(fieldChildren: Option[List[Field]]): Seq[Level] = {
//    var levels: Seq[Level] = Seq[Level]()
//    fieldChildren.foreach(fields => {
//      fields.foreach(field => {
//        if (field.parent != null) {
//          levels ++= Seq(Level(field.name.getOrElse(field.column), field.column, Int.MaxValue,
//            field.dataType.getOrElse(CarbonCommonConstants.STRING), field.parent,
//            field.storeType.getOrElse("Columnar")))
//        } else {
//          levels ++= Seq(Level(field.name.getOrElse(field.column), field.column, Int.MaxValue,
//            field.dataType.getOrElse(CarbonCommonConstants.STRING),
//            field.storeType.getOrElse("Columnar")))
//        }
//        if (field.children.get != null) {
//          levels ++= getAllChildren(field.children)
//        }
//      })
//    })
//    levels
//  }
//
//  def process(): Table = {
//
//    var levels = Seq[Level]()
//    var measures = Seq[Measure]()
//    var dimSrcDimensions = Seq[Dimension]()
//    val LOGGER = LogServiceFactory.getLogService(TableProcessor.getClass.getName)
//
//    // Create Table DDL with Database defination
//    cm.dimCols.foreach(field => {
//      if (field.parent != null) {
//        levels ++= Seq(Level(field.name.getOrElse(field.column), field.column, Int.MaxValue,
//          field.dataType.getOrElse(CarbonCommonConstants.STRING), field.parent,
//          field.storeType.getOrElse(CarbonCommonConstants.COLUMNAR)))
//      } else {
//        levels ++= Seq(Level(field.name.getOrElse(field.column), field.column, Int.MaxValue,
//          field.dataType.getOrElse(CarbonCommonConstants.STRING), field.parent,
//          field.storeType.getOrElse(CarbonCommonConstants.COLUMNAR)))
//      }
//      if (field.children.get != null) {
//        levels ++= getAllChildren(field.children)
//      }
//    })
//    measures = cm.msrCols.map(field => Measure(field.name.getOrElse(field.column), field.column,
//      field.dataType.getOrElse(CarbonCommonConstants.NUMERIC)))
//
//    if (cm.withKeyword.equalsIgnoreCase(CarbonCommonConstants.WITH) &&
//        cm.simpleDimRelations.nonEmpty) {
//      cm.simpleDimRelations.foreach(relationEntry => {
//
//        // Split the levels and seperate levels with dimension levels
//        val split = levels.partition(x => relationEntry.cols.get.contains(x.name))
//
//        val dimLevels = split._1
//        levels = split._2
//
//        def getMissingRelationLevel: Level = {
//          Level(relationEntry.relation.rightColumn,
//            relationEntry.relation.rightColumn, Int.MaxValue, CarbonCommonConstants.STRING)
//        }
//
//        val dimHierarchies = dimLevels.map(field =>
//          Hierarchy(relationEntry.tableName, Some(dimLevels.find(dl =>
//            dl.name.equalsIgnoreCase(relationEntry.relation.rightColumn))
//            .getOrElse(getMissingRelationLevel).column),
//            Seq(field), Some(relationEntry.tableName)))
//        dimSrcDimensions = dimSrcDimensions ++ dimHierarchies.map(
//          field => Dimension(field.levels.head.name, Seq(field),
//            Some(relationEntry.relation.leftColumn)))
//      })
//    }
//
//    // Check if there is any duplicate measures or dimensions.
//    // Its based on the dimension name and measure name
//    levels.groupBy(_.name).foreach(f => if (f._2.size > 1) {
//      val name = f._1
//      LOGGER.error(s"Duplicate dimensions found with name: $name")
//      LOGGER.audit(
//        "Validation failed for Create/Alter Table Operation " +
//        s"for ${ cm.databaseName }.${ cm.tableName } " +
//        s"Duplicate dimensions found with name: $name")
//      sys.error(s"Duplicate dimensions found with name: $name")
//    })
//
//    levels.groupBy(_.column).foreach(f => if (f._2.size > 1) {
//      val name = f._1
//      LOGGER.error(s"Duplicate dimensions found with column name: $name")
//      LOGGER.audit(
//        "Validation failed for Create/Alter Table Operation " +
//        s"for ${ cm.databaseName }.${ cm.tableName } " +
//        s"Duplicate dimensions found with column name: $name")
//      sys.error(s"Duplicate dimensions found with column name: $name")
//    })
//
//    measures.groupBy(_.name).foreach(f => if (f._2.size > 1) {
//      val name = f._1
//      LOGGER.error(s"Duplicate measures found with name: $name")
//      LOGGER.audit(
//        s"Validation failed for Create/Alter Table Operation " +
//        s"for ${ cm.databaseName }.${ cm.tableName } " +
//        s"Duplicate measures found with name: $name")
//      sys.error(s"Duplicate measures found with name: $name")
//    })
//
//    measures.groupBy(_.column).foreach(f => if (f._2.size > 1) {
//      val name = f._1
//      LOGGER.error(s"Duplicate measures found with column name: $name")
//      LOGGER.audit(
//        s"Validation failed for Create/Alter Table Operation " +
//        s"for ${ cm.databaseName }.${ cm.tableName } " +
//        s"Duplicate measures found with column name: $name")
//      sys.error(s"Duplicate measures found with column name: $name")
//    })
//
//    val levelsArray = levels.map(_.name)
//    val levelsNdMesures = levelsArray ++ measures.map(_.name)
//
//    cm.aggregation.foreach(a => {
//      if (levelsArray.contains(a.msrName)) {
//        val fault = a.msrName
//        LOGGER.error(s"Aggregator should not be defined for dimension fields [$fault]")
//        LOGGER.audit(
//          s"Validation failed for Create/Alter Table Operation for " +
//          s"${ cm.databaseName }.${ cm.tableName } " +
//          s"Aggregator should not be defined for dimension fields [$fault]")
//        sys.error(s"Aggregator should not be defined for dimension fields [$fault]")
//      }
//    })
//
//    levelsNdMesures.groupBy(x => x).foreach(f => if (f._2.size > 1) {
//      val name = f._1
//      LOGGER.error(s"Dimension and Measure defined with same name: $name")
//      LOGGER.audit(
//        s"Validation failed for Create/Alter Table Operation " +
//        s"for ${ cm.databaseName }.${ cm.tableName } " +
//        s"Dimension and Measure defined with same name: $name")
//      sys.error(s"Dimension and Measure defined with same name: $name")
//    })
//
//    dimSrcDimensions.foreach(d => {
//      d.hierarchies.foreach(h => {
//        h.levels.foreach(l => {
//          levels = levels.dropWhile(lev => lev.name.equalsIgnoreCase(l.name))
//        })
//      })
//    })
//
//    val groupedSeq = levels.groupBy(_.name.split('.')(0))
//    val hierarchies = levels.filter(level => !level.name.contains(".")).map(
//      parentLevel => Hierarchy(parentLevel.name, None, groupedSeq.get(parentLevel.name).get, None))
//    var dimensions = hierarchies.map(field => Dimension(field.name, Seq(field), None))
//
//    dimensions = dimensions ++ dimSrcDimensions
//    val highCardinalityDims = cm.highcardinalitydims.getOrElse(Seq())
//    for (dimension <- dimensions) {
//
//      if (highCardinalityDims.contains(dimension.name)) {
//        dimension.highCardinality = true
//      }
//
//    }
//
//    if (measures.length <= 0) {
//      measures = measures ++ Seq(Measure(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE,
//        CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE, CarbonCommonConstants.NUMERIC,
//        CarbonCommonConstants.SUM, visible = false))
//    }
//
//    // Update measures with aggregators if specified.
//    val msrsUpdatedWithAggregators = cm.aggregation match {
//      case aggs: Seq[Aggregation] =>
//        measures.map { f =>
//          val matchedMapping = aggs.filter(agg => f.name.equals(agg.msrName))
//          if (matchedMapping.isEmpty) {
//            f
//          } else {
//            Measure(f.name, f.column, f.dataType, matchedMapping.head.aggType)
//          }
//        }
//      case _ => measures
//    }
//
//    val partitioner = cm.partitioner match {
//      case Some(part: Partitioner) =>
//        var definedpartCols = part.partitionColumn
//        val columnBuffer = new ArrayBuffer[String]
//        part.partitionColumn.foreach { col =>
//          dimensions.foreach { dim =>
//            dim.hierarchies.foreach { hier =>
//              hier.levels.foreach { lev =>
//                if (lev.name.equalsIgnoreCase(col)) {
//                  definedpartCols = definedpartCols.dropWhile(c => c.equals(col))
//                  columnBuffer += lev.name
//                }
//              }
//            }
//          }
//        }
//
//
//        // Special Case, where Partition count alone is sent to Carbon for dataloading
//        if (part.partitionClass.isEmpty && part.partitionColumn(0).isEmpty) {
//          Partitioner(
//            "org.apache.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl",
//            Array(""), part.partitionCount, null)
//        } else if (definedpartCols.nonEmpty) {
//          val msg = definedpartCols.mkString(", ")
//          LOGGER.error(s"partition columns specified are not part of Dimension columns: $msg")
//          LOGGER.audit(
//            s"Validation failed for Create/Alter Table Operation - " +
//            s"partition columns specified are not part of Dimension columns: $msg")
//          sys.error(s"partition columns specified are not part of Dimension columns: $msg")
//        } else {
//          try {
//            Class.forName(part.partitionClass).newInstance()
//          } catch {
//            case e: Exception =>
//              val cl = part.partitionClass
//              LOGGER.audit(
//                s"Validation failed for Create/Alter Table Operation for " +
//                s"${ cm.databaseName }.${ cm.tableName } " +
//                s"partition class specified can not be found or loaded: $cl")
//              sys.error(s"partition class specified can not be found or loaded: $cl")
//          }
//
//          Partitioner(part.partitionClass, columnBuffer.toArray, part.partitionCount, null)
//        }
//      case None =>
//        Partitioner("org.apache.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl",
//          Array(""), 20, null)
//    }
//
//    Table(cm.databaseName, cm.tableName, dimensions, msrsUpdatedWithAggregators, partitioner)
//  }
//
//  // For filtering INCLUDE and EXCLUDE fields if any is defined for Dimention relation
//  def filterRelIncludeCols(relationEntry: DimensionRelation, p: (String, String)): Boolean = {
//    if (relationEntry.includeKey.get.equalsIgnoreCase(CarbonCommonConstants.INCLUDE)) {
//      relationEntry.cols.get.map(x => x.toLowerCase()).contains(p._1.toLowerCase())
//    } else {
//      !relationEntry.cols.get.map(x => x.toLowerCase()).contains(p._1.toLowerCase())
//    }
//  }
//
//}


/**
 * Command for the compaction in alter table command
 *
 * @param alterTableModel
 */
private[sql] case class AlterTableCompaction(alterTableModel: AlterTableModel) extends
  RunnableCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    // TODO : Implement it.
    val tableName = alterTableModel.tableName
    val databaseName = alterTableModel.dbName.getOrElse(sparkSession.catalog.currentDatabase)
    if (null == org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
      .getCarbonTable(databaseName + "_" + tableName)) {
      logError(s"alter table failed. table not found: $databaseName.$tableName")
      sys.error(s"alter table failed. table not found: $databaseName.$tableName")
    }

    val relation =
      CarbonEnv.get.carbonMetastore
        .lookupRelation(Option(databaseName), tableName)(sparkSession)
        .asInstanceOf[CarbonRelation]
    if (relation == null) {
      sys.error(s"Table $databaseName.$tableName does not exist")
    }
    val carbonLoadModel = new CarbonLoadModel()


    val table = relation.tableMeta.carbonTable
    carbonLoadModel.setAggTables(table.getAggregateTablesName.asScala.toArray)
    carbonLoadModel.setTableName(table.getFactTableName)
    val dataLoadSchema = new CarbonDataLoadSchema(table)
    // Need to fill dimension relation
    carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
    carbonLoadModel.setTableName(relation.tableMeta.carbonTableIdentifier.getTableName)
    carbonLoadModel.setDatabaseName(relation.tableMeta.carbonTableIdentifier.getDatabaseName)
    carbonLoadModel.setStorePath(relation.tableMeta.storePath)

    val kettleHomePath = CarbonScalaUtil.getKettleHome(sparkSession.sqlContext)

    var storeLocation = CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.STORE_LOCATION_TEMP_PATH,
        System.getProperty("java.io.tmpdir")
      )
    storeLocation = storeLocation + "/carbonstore/" + System.nanoTime()
    try {
      CarbonDataRDDFactory
        .alterTableForCompaction(sparkSession.sqlContext,
          alterTableModel,
          carbonLoadModel,
          relation.tableMeta.storePath,
          kettleHomePath,
          storeLocation
        )
    } catch {
      case e: Exception =>
        if (null != e.getMessage) {
          sys.error(s"Compaction failed. Please check logs for more info. ${ e.getMessage }")
        } else {
          sys.error("Exception in compaction. Please check logs for more info.")
        }
    }
    Seq.empty
  }
}

case class CreateTable(cm: TableModel) extends RunnableCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    cm.databaseName = cm.databaseNameOp.getOrElse(sparkSession.catalog.currentDatabase)
    val tbName = cm.tableName
    val dbName = cm.databaseName
    LOGGER.audit(s"Creating Table with Database name [$dbName] and Table name [$tbName]")

    val tableInfo: TableInfo = TableNewProcessor(cm, sparkSession.sqlContext)

    if (tableInfo.getFactTable.getListOfColumns.size <= 0) {
      sys.error("No Dimensions found. Table should have at least one dimesnion !")
    }

//    if (sparkSession.sqlContext.tableNames(dbName).exists(_.equalsIgnoreCase(tbName))) {
//      if (!cm.ifNotExistsSet) {
//        LOGGER.audit(
//          s"Table creation with Database name [$dbName] and Table name [$tbName] failed. " +
//          s"Table [$tbName] already exists under database [$dbName]")
//        sys.error(s"Table [$tbName] already exists under database [$dbName]")
//      }
//    } else {
      // Add Database to catalog and persist
      val catalog = CarbonEnv.get.carbonMetastore
      val tablePath = catalog.createTableFromThrift(tableInfo, dbName, tbName)(sparkSession)
//      try {
//        sparkSession.sql(
//          s"""CREATE TABLE $dbName.$tbName USING carbondata""" +
//          s""" OPTIONS (tableName "$dbName.$tbName", tablePath "$tablePath") """)
//          .collect
//      } catch {
//        case e: Exception =>
//          val identifier: TableIdentifier = TableIdentifier(tbName, Some(dbName))
//          // call the drop table to delete the created table.
//
//          CarbonEnv.get.carbonMetastore
//            .dropTable(catalog.storePath, identifier)(sparkSession)
//
//          LOGGER.audit(s"Table creation with Database name [$dbName] " +
//                       s"and Table name [$tbName] failed")
//          throw e
//      }

      LOGGER.audit(s"Table created with Database name [$dbName] and Table name [$tbName]")
//    }

    Seq.empty
  }

  def setV(ref: Any, name: String, value: Any): Unit = {
    ref.getClass.getFields.find(_.getName == name).get
      .set(ref, value.asInstanceOf[AnyRef])
  }
}

private[sql] case class DeleteLoadsById(
    loadids: Seq[String],
    databaseNameOp: Option[String],
    tableName: String) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sparkSession: SparkSession): Seq[Row] = {

    val databaseName = databaseNameOp.getOrElse(sparkSession.catalog.currentDatabase)
    LOGGER.audit(s"Delete segment by Id request has been received for $databaseName.$tableName")

    // validate load ids first
    validateLoadIds
    val dbName = databaseNameOp.getOrElse(sparkSession.catalog.currentDatabase)
    val identifier = TableIdentifier(tableName, Option(dbName))
    val relation = CarbonEnv.get.carbonMetastore.lookupRelation(
      identifier, None)(sparkSession).asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER.audit(s"Delete segment by Id is failed. Table $dbName.$tableName does not exist")
      sys.error(s"Table $dbName.$tableName does not exist")
    }

    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(dbName + '_' + tableName)

    if (null == carbonTable) {
      CarbonEnv.get.carbonMetastore
        .lookupRelation(identifier, None)(sparkSession).asInstanceOf[CarbonRelation]
    }
    val path = carbonTable.getMetaDataFilepath

    try {
      val invalidLoadIds = SegmentStatusManager.updateDeletionStatus(
        carbonTable.getAbsoluteTableIdentifier, loadids.asJava, path).asScala

      if (invalidLoadIds.isEmpty) {

        LOGGER.audit(s"Delete segment by Id is successfull for $databaseName.$tableName.")
      }
      else {
        sys.error("Delete segment by Id is failed. Invalid ID is:" +
                  s" ${ invalidLoadIds.mkString(",") }")
      }
    } catch {
      case ex: Exception =>
        sys.error(ex.getMessage)
    }

    Seq.empty

  }

  // validates load ids
  private def validateLoadIds: Unit = {
    if (loadids.isEmpty) {
      val errorMessage = "Error: Segment id(s) should not be empty."
      throw new MalformedCarbonCommandException(errorMessage)

    }
  }
}

private[sql] case class DeleteLoadsByLoadDate(
    databaseNameOp: Option[String],
    tableName: String,
    dateField: String,
    loadDate: String) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.TableModel.tableSchema")

  def run(sparkSession: SparkSession): Seq[Row] = {

    LOGGER.audit("The delete segment by load date request has been received.")
    val dbName = databaseNameOp.getOrElse(sparkSession.catalog.currentDatabase)
    val identifier = TableIdentifier(tableName, Option(dbName))
    val relation = CarbonEnv.get.carbonMetastore
      .lookupRelation(identifier, None)(sparkSession).asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER
        .audit(s"Delete segment by load date is failed. Table $dbName.$tableName does not " +
               s"exist")
      sys.error(s"Table $dbName.$tableName does not exist")
    }

    val timeObj = Cast(Literal(loadDate), TimestampType).eval()
    if (null == timeObj) {
      val errorMessage = "Error: Invalid load start time format " + loadDate
      throw new MalformedCarbonCommandException(errorMessage)
    }

    val carbonTable = org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
      .getCarbonTable(dbName + '_' + tableName)

    if (null == carbonTable) {
      var relation = CarbonEnv.get.carbonMetastore
        .lookupRelation(identifier, None)(sparkSession).asInstanceOf[CarbonRelation]
    }
    val path = carbonTable.getMetaDataFilepath()

    try {
      val invalidLoadTimestamps = SegmentStatusManager.updateDeletionStatus(
        carbonTable.getAbsoluteTableIdentifier, loadDate, path,
        timeObj.asInstanceOf[java.lang.Long]).asScala
      if (invalidLoadTimestamps.isEmpty) {
        LOGGER.audit(s"Delete segment by date is successfull for $dbName.$tableName.")
      }
      else {
        sys.error("Delete segment by date is failed. No matching segment found.")
      }
    } catch {
      case ex: Exception =>
        sys.error(ex.getMessage)
    }
    Seq.empty

  }

}

case class LoadTable(
    databaseNameOp: Option[String],
    tableName: String,
    factPathFromUser: String,
    dimFilesPath: Seq[DataLoadTableFileMapping],
    options: scala.collection.immutable.Map[String, String],
    isOverwriteExist: Boolean = false,
    var inputSqlString: String = null,
    dataFrame: Option[DataFrame] = None) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)


  def run(sparkSession: SparkSession): Seq[Row] = {

    val dbName = databaseNameOp.getOrElse(sparkSession.catalog.currentDatabase)
    val identifier = TableIdentifier(tableName, Option(dbName))
    if (isOverwriteExist) {
      sys.error(s"Overwrite is not supported for carbon table with $dbName.$tableName")
    }
    if (null == CarbonMetadata.getInstance.getCarbonTable(dbName + "_" + tableName)) {
      logError(s"Data loading failed. table not found: $dbName.$tableName")
      LOGGER.audit(s"Data loading failed. table not found: $dbName.$tableName")
      sys.error(s"Data loading failed. table not found: $dbName.$tableName")
    }

    val relation = CarbonEnv.get.carbonMetastore
        .lookupRelation(Option(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
    if (relation == null) {
      sys.error(s"Table $dbName.$tableName does not exist")
    }
    CarbonProperties.getInstance().addProperty("zookeeper.enable.lock", "false")
    val carbonLock = CarbonLockFactory
      .getCarbonLockObj(relation.tableMeta.carbonTable.getAbsoluteTableIdentifier
        .getCarbonTableIdentifier,
        LockUsage.METADATA_LOCK
      )
    try {
      if (carbonLock.lockWithRetries()) {
        logInfo("Successfully able to get the table metadata file lock")
      } else {
        sys.error("Table is locked for updation. Please try after some time")
      }

      val factPath = if (dataFrame.isDefined) {
        ""
      } else {
        FileUtils.getPaths(
          CarbonUtil.checkAndAppendHDFSUrl(factPathFromUser))
      }
      val carbonLoadModel = new CarbonLoadModel()
      carbonLoadModel.setTableName(relation.tableMeta.carbonTableIdentifier.getTableName)
      carbonLoadModel.setDatabaseName(relation.tableMeta.carbonTableIdentifier.getDatabaseName)
      carbonLoadModel.setStorePath(relation.tableMeta.storePath)
      if (dimFilesPath.isEmpty) {
        carbonLoadModel.setDimFolderPath(null)
      } else {
        val x = dimFilesPath.map(f => f.table + ":" + CarbonUtil.checkAndAppendHDFSUrl(f.loadPath))
        carbonLoadModel.setDimFolderPath(x.mkString(","))
      }

      val table = relation.tableMeta.carbonTable
      carbonLoadModel.setAggTables(table.getAggregateTablesName.asScala.toArray)
      carbonLoadModel.setTableName(table.getFactTableName)
      val dataLoadSchema = new CarbonDataLoadSchema(table)
      // Need to fill dimension relation
      carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)

      var partitionLocation = relation.tableMeta.storePath + "/partition/" +
                              relation.tableMeta.carbonTableIdentifier.getDatabaseName + "/" +
                              relation.tableMeta.carbonTableIdentifier.getTableName + "/"


      val columnar = sparkSession.conf.get("carbon.is.columnar.storage", "true").toBoolean
      val kettleHomePath = CarbonScalaUtil.getKettleHome(sparkSession.sqlContext)

      // TODO It will be removed after kettle is removed.
      val useKettle = options.get("use_kettle") match {
        case Some(value) => value.toBoolean
        case _ =>
          val useKettleLocal = System.getProperty("use.kettle")
          if (useKettleLocal == null) {
            sparkSession.sqlContext.sparkContext.getConf.get("use_kettle_default", "true").toBoolean
          } else {
            useKettleLocal.toBoolean
          }
      }

      val delimiter = options.getOrElse("delimiter", ",")
      val quoteChar = options.getOrElse("quotechar", "\"")
      val fileHeader = options.getOrElse("fileheader", "")
      val escapeChar = options.getOrElse("escapechar", "\\")
      val commentchar = options.getOrElse("commentchar", "#")
      val columnDict = options.getOrElse("columndict", null)
      val serializationNullFormat = options.getOrElse("serialization_null_format", "\\N")
      val badRecordsLoggerEnable = options.getOrElse("bad_records_logger_enable", "false")
      val badRecordsLoggerRedirect = options.getOrElse("bad_records_action", "force")
      val allDictionaryPath = options.getOrElse("all_dictionary_path", "")
      val complex_delimiter_level_1 = options.getOrElse("complex_delimiter_level_1", "\\$")
      val complex_delimiter_level_2 = options.getOrElse("complex_delimiter_level_2", "\\:")
      val dateFormat = options.getOrElse("dateformat", null)
      validateDateFormat(dateFormat, table)
      val multiLine = options.getOrElse("multiline", "false").trim.toLowerCase match {
        case "true" => true
        case "false" => false
        case illegal =>
          val errorMessage = "Illegal syntax found: [" + illegal + "] .The value multiline in " +
                             "load DDL which you set can only be 'true' or 'false', please check " +
                             "your input DDL."
          throw new MalformedCarbonCommandException(errorMessage)
      }
      val maxColumns = options.getOrElse("maxcolumns", null)
      carbonLoadModel.setMaxColumns(maxColumns)
      carbonLoadModel.setEscapeChar(escapeChar)
      carbonLoadModel.setQuoteChar(quoteChar)
      carbonLoadModel.setCommentChar(commentchar)
      carbonLoadModel.setDateFormat(dateFormat)
      carbonLoadModel
        .setSerializationNullFormat(
          TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName + "," + serializationNullFormat)
      carbonLoadModel
        .setBadRecordsLoggerEnable(
          TableOptionConstant.BAD_RECORDS_LOGGER_ENABLE.getName + "," + badRecordsLoggerEnable)
      carbonLoadModel
        .setBadRecordsAction(
          TableOptionConstant.BAD_RECORDS_ACTION.getName + "," + badRecordsLoggerRedirect)

      if (delimiter.equalsIgnoreCase(complex_delimiter_level_1) ||
          complex_delimiter_level_1.equalsIgnoreCase(complex_delimiter_level_2) ||
          delimiter.equalsIgnoreCase(complex_delimiter_level_2)) {
        sys.error(s"Field Delimiter & Complex types delimiter are same")
      }
      else {
        carbonLoadModel.setComplexDelimiterLevel1(
          CarbonUtil.delimiterConverter(complex_delimiter_level_1))
        carbonLoadModel.setComplexDelimiterLevel2(
          CarbonUtil.delimiterConverter(complex_delimiter_level_2))
      }
      // set local dictionary path, and dictionary file extension
      carbonLoadModel.setAllDictPath(allDictionaryPath)

      var partitionStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
      try {
        // First system has to partition the data first and then call the load data
        LOGGER.info(s"Initiating Direct Load for the Table : ($dbName.$tableName)")
        carbonLoadModel.setFactFilePath(factPath)
        carbonLoadModel.setCsvDelimiter(CarbonUtil.unescapeChar(delimiter))
        carbonLoadModel.setCsvHeader(fileHeader)
        carbonLoadModel.setColDictFilePath(columnDict)
        carbonLoadModel.setDirectLoad(true)
        GlobalDictionaryUtil
          .generateGlobalDictionary(sparkSession.sqlContext, carbonLoadModel, relation.tableMeta.storePath,
            dataFrame)
        CarbonDataRDDFactory.loadCarbonData(sparkSession.sqlContext,
            carbonLoadModel,
            relation.tableMeta.storePath,
            kettleHomePath,
            columnar,
            partitionStatus,
            useKettle,
            dataFrame)
      } catch {
        case ex: Exception =>
          LOGGER.error(ex)
          LOGGER.audit(s"Dataload failure for $dbName.$tableName. Please check the logs")
          throw ex
      } finally {
        // Once the data load is successful delete the unwanted partition files
        try {
          val fileType = FileFactory.getFileType(partitionLocation)
          if (FileFactory.isFileExist(partitionLocation, fileType)) {
            val file = FileFactory
              .getCarbonFile(partitionLocation, fileType)
            CarbonUtil.deleteFoldersAndFiles(file)
          }
        } catch {
          case ex: Exception =>
            LOGGER.error(ex)
            LOGGER.audit(s"Dataload failure for $dbName.$tableName. " +
                         "Problem deleting the partition folder")
            throw ex
        }

      }
    } catch {
      case dle: DataLoadingException =>
        LOGGER.audit(s"Dataload failed for $dbName.$tableName. " + dle.getMessage)
        throw dle
      case mce: MalformedCarbonCommandException =>
        LOGGER.audit(s"Dataload failed for $dbName.$tableName. " + mce.getMessage)
        throw mce
    } finally {
      if (carbonLock != null) {
        if (carbonLock.unlock()) {
          logInfo("Table MetaData Unlocked Successfully after data load")
        } else {
          logError("Unable to unlock Table MetaData")
        }
      }
    }
    Seq.empty
  }

  private def validateDateFormat(dateFormat: String, table: CarbonTable): Unit = {
    val dimensions = table.getDimensionByTableName(tableName).asScala
    if (dateFormat != null) {
      if (dateFormat.trim == "") {
        throw new MalformedCarbonCommandException("Error: Option DateFormat is set an empty " +
                                                  "string.")
      } else {
        var dateFormats: Array[String] = dateFormat.split(CarbonCommonConstants.COMMA)
        for (singleDateFormat <- dateFormats) {
          val dateFormatSplits: Array[String] = singleDateFormat.split(":", 2)
          val columnName = dateFormatSplits(0).trim.toLowerCase
          if (!dimensions.exists(_.getColName.equals(columnName))) {
            throw new MalformedCarbonCommandException("Error: Wrong Column Name " +
                                                      dateFormatSplits(0) +
                                                      " is provided in Option DateFormat.")
          }
          if (dateFormatSplits.length < 2 || dateFormatSplits(1).trim.isEmpty) {
            throw new MalformedCarbonCommandException("Error: Option DateFormat is not provided " +
                                                      "for " + "Column " + dateFormatSplits(0) +
                                                      ".")
          }
        }
      }
    }
  }
}

private[sql] case class DescribeCommandFormatted(
    child: SparkPlan,
    override val output: Seq[Attribute],
    tblIdentifier: TableIdentifier)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val relation = CarbonEnv.get.carbonMetastore
      .lookupRelation(tblIdentifier)(sparkSession).asInstanceOf[CarbonRelation]
    val mapper = new ObjectMapper()
    val colProps = StringBuilder.newBuilder
    var results: Seq[(String, String, String)] = child.schema.fields.map { field =>
      val comment = if (relation.metaData.dims.contains(field.name)) {
        val dimension = relation.metaData.carbonTable.getDimensionByName(
          relation.tableMeta.carbonTableIdentifier.getTableName,
          field.name)
        if (null != dimension.getColumnProperties && dimension.getColumnProperties.size() > 0) {
          val colprop = mapper.writeValueAsString(dimension.getColumnProperties)
          colProps.append(field.name).append(".")
            .append(mapper.writeValueAsString(dimension.getColumnProperties))
            .append(",")
        }
        if (dimension.hasEncoding(Encoding.DICTIONARY) &&
            !dimension.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
          "DICTIONARY, KEY COLUMN"
        } else {
          "KEY COLUMN"
        }
      } else {
        ("MEASURE")
      }
      (field.name, field.dataType.simpleString, comment)
    }
    val colPropStr = if (colProps.toString().trim().length() > 0) {
      // drops additional comma at end
      colProps.toString().dropRight(1)
    } else {
      colProps.toString()
    }
    results ++= Seq(("", "", ""), ("##Detailed Table Information", "", ""))
    results ++= Seq(("Database Name: ", relation.tableMeta.carbonTableIdentifier
      .getDatabaseName, "")
    )
    results ++= Seq(("Table Name: ", relation.tableMeta.carbonTableIdentifier.getTableName, ""))
    results ++= Seq(("CARBON Store Path: ", relation.tableMeta.storePath, ""))
    val carbonTable = relation.tableMeta.carbonTable
    results ++= Seq(("Table Block Size : ", carbonTable.getBlockSizeInMB + " MB", ""))
    results ++= Seq(("", "", ""), ("##Detailed Column property", "", ""))
    if (colPropStr.length() > 0) {
      results ++= Seq((colPropStr, "", ""))
    } else {
      results ++= Seq(("NONE", "", ""))
    }
    val dimension = carbonTable
      .getDimensionByTableName(relation.tableMeta.carbonTableIdentifier.getTableName)
    results ++= getColumnGroups(dimension.asScala.toList)
    results.map { case (name, dataType, comment) =>
      Row(f"$name%-36s $dataType%-80s $comment%-72s")
    }
  }

  private def getColumnGroups(dimensions: List[CarbonDimension]): Seq[(String, String, String)] = {
    var results: Seq[(String, String, String)] =
      Seq(("", "", ""), ("##Column Group Information", "", ""))
    val groupedDimensions = dimensions.groupBy(x => x.columnGroupId()).filter {
      case (groupId, _) => groupId != -1
    }.toSeq.sortBy(_._1)
    val groups = groupedDimensions.map(colGroups => {
      colGroups._2.map(dim => dim.getColName).mkString(", ")
    })
    var index = 1
    groups.map { x =>
      results = results :+ (s"Column Group $index", x, "")
      index = index + 1
    }
    results
  }
}

private[sql] case class DeleteLoadByDate(
    databaseNameOp: Option[String],
    tableName: String,
    dateField: String,
    dateValue: String
) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sparkSession: SparkSession): Seq[Row] = {
    val dbName = databaseNameOp.getOrElse(sparkSession.catalog.currentDatabase)
    LOGGER.audit(s"The delete load by date request has been received for $dbName.$tableName")
    val identifier = TableIdentifier(tableName, Option(dbName))
    val relation = CarbonEnv.get.carbonMetastore
      .lookupRelation(identifier)(sparkSession).asInstanceOf[CarbonRelation]
    var level: String = ""
    val carbonTable = org.apache.carbondata.core.carbon.metadata.CarbonMetadata
      .getInstance().getCarbonTable(dbName + '_' + tableName)
    if (relation == null) {
      LOGGER.audit(s"The delete load by date is failed. Table $dbName.$tableName does not exist")
      sys.error(s"Table $dbName.$tableName does not exist")
    }
    val matches: Seq[AttributeReference] = relation.dimensionsAttr.filter(
      filter => filter.name.equalsIgnoreCase(dateField) &&
                filter.dataType.isInstanceOf[TimestampType]).toList
    if (matches.isEmpty) {
      LOGGER.audit("The delete load by date is failed. " +
                   s"Table $dbName.$tableName does not contain date field: $dateField")
      sys.error(s"Table $dbName.$tableName does not contain date field $dateField")
    } else {
      level = matches.asJava.get(0).name
    }
    val actualColName = relation.metaData.carbonTable.getDimensionByName(tableName, level)
      .getColName
    CarbonDataRDDFactory.deleteLoadByDate(
      sparkSession.sqlContext,
      new CarbonDataLoadSchema(carbonTable),
      dbName,
      tableName,
      CarbonEnv.get.carbonMetastore.storePath,
      level,
      actualColName,
      dateValue)
    LOGGER.audit(s"The delete load by date $dateValue is successful for $dbName.$tableName.")
    Seq.empty
  }

}

private[sql] case class CleanFiles(
    databaseNameOp: Option[String],
    tableName: String) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sparkSession: SparkSession): Seq[Row] = {
    val dbName = databaseNameOp.getOrElse(sparkSession.catalog.currentDatabase)
    LOGGER.audit(s"The clean files request has been received for $dbName.$tableName")
    val identifier = TableIdentifier(tableName, Option(dbName))
    val relation = CarbonEnv.get.carbonMetastore
      .lookupRelation(identifier)(sparkSession).asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER.audit(s"The clean files request is failed. Table $dbName.$tableName does not exist")
      sys.error(s"Table $dbName.$tableName does not exist")
    }

    val carbonLoadModel = new CarbonLoadModel()
    carbonLoadModel.setTableName(relation.tableMeta.carbonTableIdentifier.getTableName)
    carbonLoadModel.setDatabaseName(relation.tableMeta.carbonTableIdentifier.getDatabaseName)
    val table = relation.tableMeta.carbonTable
    carbonLoadModel.setAggTables(table.getAggregateTablesName.asScala.toArray)
    carbonLoadModel.setTableName(table.getFactTableName)
    carbonLoadModel.setStorePath(relation.tableMeta.storePath)
    val dataLoadSchema = new CarbonDataLoadSchema(table)
    carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
    try {
      CarbonDataRDDFactory.cleanFiles(
        sparkSession.sqlContext.sparkContext,
        carbonLoadModel,
        relation.tableMeta.storePath)
      LOGGER.audit(s"Clean files request is successfull for $dbName.$tableName.")
    } catch {
      case ex: Exception =>
        sys.error(ex.getMessage)
    }
    Seq.empty
  }
}
