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

package org.apache.carbondata.spark.load

import java.util.Comparator

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.classTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.spark.{CarbonInputMetrics, DataSkewRangePartitioner, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.command.ExecutionErrors
import org.apache.spark.sql.types.{ByteType, DateType, LongType, StringType, TimestampType}
import org.apache.spark.sql.util.{SparkSQLUtil, SparkTypeConverter}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{CollectionAccumulator, LongAccumulator}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.converter.SparkDataTypeConverterImpl
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.row.CarbonRow
import org.apache.carbondata.core.metadata.datatype.{DataType, DataTypes, StructField, StructType}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonColumn, CarbonDimension}
import org.apache.carbondata.core.segmentmeta.SegmentMetaDataInfo
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus}
import org.apache.carbondata.core.util._
import org.apache.carbondata.core.util.ByteUtil.UnsafeComparer
import org.apache.carbondata.hadoop.CarbonProjection
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat
import org.apache.carbondata.processing.datatypes.GenericDataType
import org.apache.carbondata.processing.loading.{CarbonDataLoadConfiguration, DataField, DataLoadProcessBuilder, FailureCauses}
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants
import org.apache.carbondata.processing.loading.csvinput.CSVInputFormat
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.sort.sortdata.{NewRowComparator, NewRowComparatorForNormalDims, SortParameters}
import org.apache.carbondata.processing.util.{CarbonDataProcessorUtil, TableOptionConstant}
import org.apache.carbondata.spark.rdd.{CarbonScanRDD, InsertTaskCompletionListener, StringArrayRow}
import org.apache.carbondata.spark.util.{CommonUtil, Util}
import org.apache.carbondata.store.CarbonRowReadSupport

/**
 * Use sortBy operator in spark to load the data
 */
object DataLoadProcessBuilderOnSpark {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def loadDataUsingGlobalSort(
      sparkSession: SparkSession,
      dataFrame: Option[DataFrame],
      model: CarbonLoadModel,
      hadoopConf: Configuration,
      segmentMetaDataAccumulator: CollectionAccumulator[Map[String, SegmentMetaDataInfo]],
      isCompactionFlow: Boolean = false)
  : Array[(String, (LoadMetadataDetails, ExecutionErrors))] = {
    var isLoadFromCSV = false
    val originRDD = if (dataFrame.isDefined) {
      dataFrame.get.rdd
    } else {
      // input data from files
      isLoadFromCSV = true
      val columnCount = model.getCsvHeaderColumns.length
      CsvRDDHelper.csvFileScanRDD(sparkSession, model, hadoopConf)
        .map(DataLoadProcessorStepOnSpark.toStringArrayRow(_, columnCount))
    }

    val sc = sparkSession.sparkContext
    val modelBroadcast = sc.broadcast(model)
    val partialSuccessAccum = sc.longAccumulator("Partial Success Accumulator")
    val inputStepRowCounter = sc.longAccumulator("Input Processor Accumulator")
    val convertStepRowCounter = sc.longAccumulator("Convert Processor Accumulator")
    val sortStepRowCounter = sc.longAccumulator("Sort Processor Accumulator")
    val writeStepRowCounter = sc.longAccumulator("Write Processor Accumulator")

    hadoopConf
      .set(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, sparkSession.sparkContext.appName)

    val conf = SparkSQLUtil.broadCastHadoopConf(sc, hadoopConf)
    // 1. Input
    val inputRDD = if (isLoadFromCSV) {
      // No need of wrap with NewRDDIterator, which converts object to string,
      // as it is already a string.
      // So, this will avoid new object creation in case of CSV global sort load for each row
      originRDD.mapPartitionsWithIndex { case (index, rows) =>
        DataLoadProcessorStepOnSpark.inputFuncForCsvRows(
          rows.asInstanceOf[Iterator[StringArrayRow]],
          index,
          modelBroadcast,
          inputStepRowCounter)
      }
    } else {
      originRDD
        .mapPartitions(rows => DataLoadProcessorStepOnSpark.toRDDIterator(rows, modelBroadcast))
        .mapPartitionsWithIndex { case (index, rows) =>
          DataLoadProcessorStepOnSpark.inputFunc(rows, index, modelBroadcast, inputStepRowCounter)
        }
    }

    // 2. Convert
    val convertRDD = inputRDD.mapPartitionsWithIndex { case (index, rows) =>
      ThreadLocalSessionInfo.setConfigurationToCurrentThread(conf.value.value)
      DataLoadProcessorStepOnSpark.convertFunc(rows,
        index,
        modelBroadcast,
        partialSuccessAccum,
        convertStepRowCounter,
        false,
        isCompactionFlow)
    }.filter(_ != null) // Filter the bad record

    // 3. Sort
    val configuration = DataLoadProcessBuilder.createConfiguration(model)
    val sortParameters = SortParameters.createSortParameters(configuration)
    val rowComparator: Comparator[Array[AnyRef]] =
      if (sortParameters.getNoDictionaryCount > 0) {
        new NewRowComparator(sortParameters.getNoDictionarySortColumn,
          sortParameters.getNoDictDataType)
      } else {
        new NewRowComparatorForNormalDims(sortParameters.getDimColCount)
      }
    object RowOrdering extends Ordering[Array[AnyRef]] {
      def compare(rowA: Array[AnyRef], rowB: Array[AnyRef]): Int = {
        rowComparator.compare(rowA, rowB)
      }
    }

    var numPartitions = CarbonDataProcessorUtil.getGlobalSortPartitions(
      configuration.getDataLoadProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS))
    if (numPartitions <= 0) {
      numPartitions = convertRDD.partitions.length
    } else if (System.getProperty("useIndexServer") != null) {
      convertRDD.partitions
    }

    // Because if the number of partitions greater than 1, there will be action operator(sample) in
    // sortBy operator. So here we cache the rdd to avoid do input and convert again.
    if (numPartitions > 1) {
      convertRDD.persist(StorageLevel.fromString(
        CarbonProperties.getInstance().getGlobalSortRddStorageLevel()))
    }

    import scala.reflect.classTag
    val sortRDD = convertRDD
      .sortBy(_.getData, numPartitions = numPartitions)(RowOrdering, classTag[Array[AnyRef]])
      .mapPartitionsWithIndex { case (index, rows) =>
        DataLoadProcessorStepOnSpark.convertTo3Parts(rows, index, modelBroadcast,
          sortStepRowCounter)
      }

    // 4. Write
    sc.runJob(sortRDD, (context: TaskContext, rows: Iterator[CarbonRow]) => {
      setTaskListener(model.getTableName, model.getSegmentId, segmentMetaDataAccumulator)
      val loadModel = modelBroadcast.value.getCopyWithTaskNo(context.partitionId.toString)
      loadModel.setMetrics(new DataLoadMetrics())
      DataLoadProcessorStepOnSpark.writeFunc(
        rows, context.partitionId, loadModel, writeStepRowCounter, conf.value.value)
      SparkSQLUtil.setOutputMetrics(context.taskMetrics().outputMetrics, loadModel.getMetrics)
    })

    // clean cache only if persisted and keeping unpersist non-blocking as non-blocking call will
    // not have any functional impact as spark automatically monitors the cache usage on each node
    // and drops out old data partitions in a least-recently used (LRU) fashion.
    if (numPartitions > 1) {
      convertRDD.unpersist(false)
    }

    // Log the number of rows in each step
    LOGGER.info("Total rows processed in step Input Processor: " + inputStepRowCounter.value)
    LOGGER.info("Total rows processed in step Data Converter: " + convertStepRowCounter.value)
    LOGGER.info("Total rows processed in step Sort Processor: " + sortStepRowCounter.value)
    LOGGER.info("Total rows processed in step Data Writer: " + writeStepRowCounter.value)

    updateLoadStatus(model, partialSuccessAccum)
  }

  def insertDataUsingGlobalSortWithInternalRow(
      sparkSession: SparkSession,
      scanResultRDD : RDD[InternalRow],
      model: CarbonLoadModel,
      hadoopConf: Configuration,
      segmentMetaDataAccumulator: CollectionAccumulator[Map[String, SegmentMetaDataInfo]])
  : Array[(String, (LoadMetadataDetails, ExecutionErrors))] = {
    val originRDD = scanResultRDD

    val sc = sparkSession.sparkContext
    val modelBroadcast = sc.broadcast(model)
    val partialSuccessAccum = sc.longAccumulator("Partial Success Accumulator")
    val sortStepRowCounter = sc.longAccumulator("Sort Processor Accumulator")
    val writeStepRowCounter = sc.longAccumulator("Write Processor Accumulator")

    hadoopConf
      .set(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, sparkSession.sparkContext.appName)

    val conf = SparkSQLUtil.broadCastHadoopConf(sc, hadoopConf)
    val configuration = DataLoadProcessBuilder.createConfiguration(model)

    // 1. Convert internalRow to object array
    val fields = Util
      .convertToSparkSchemaFromColumnSchema(model.getCarbonDataLoadSchema.getCarbonTable, true)
      .fields
      .toSeq

    val dataTypes = fields.map(field => field.dataType)
    val map: mutable.Map[String, GenericDataType[_]] = mutable.Map[String, GenericDataType[_]]()
    CommonUtil.convertComplexDataType(map, configuration)
    val rdd = originRDD.map { internalRow =>
      CommonUtil.getObjectArrayFromInternalRowAndConvertComplexTypeForGlobalSort(internalRow,
        fields,
        map)
    }

    // 2. sort
    var numPartitions = CarbonDataProcessorUtil.getGlobalSortPartitions(
      configuration.getDataLoadProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS))
    if (numPartitions <= 0) {
      numPartitions = originRDD.partitions.length
    }
    // Because if the number of partitions greater than 1, there will be action operator
    // (sample) in
    // sortBy operator. So here we cache the rdd to avoid do input and convert again.
    if (numPartitions > 1) {
      rdd.persist(StorageLevel.fromString(
        CarbonProperties.getInstance().getGlobalSortRddStorageLevel()))
    }
    val sortColumnsLength = model.getCarbonDataLoadSchema.getCarbonTable.getSortColumns.size()
    var sortColumnDataTypes = dataTypes.take(sortColumnsLength)
    sortColumnDataTypes = sortColumnDataTypes.map {
      case StringType => ByteType
      case TimestampType | DateType => LongType
      case datatype => datatype
    }
    val rowComparator = GlobalSortHelper.generateRowComparator(sortColumnDataTypes.zipWithIndex)
    val sortRDD = rdd.sortBy(row =>
      getKey(row, sortColumnsLength),
      true,
      numPartitions)(
      rowComparator, classTag[Array[AnyRef]])
    val newRDD = sortRDD
      .mapPartitionsWithIndex { case (index, rows) =>
        DataLoadProcessorStepOnSpark.convertTo3PartsFromObjectArray(rows, index, model,
          sortStepRowCounter)
      }

    // 3. Write
    sc.runJob(newRDD, (context: TaskContext, rows: Iterator[CarbonRow]) => {
      setTaskListener(model.getTableName, model.getSegmentId, segmentMetaDataAccumulator)
      val loadModel = modelBroadcast.value.getCopyWithTaskNo(context.partitionId.toString)
      loadModel.setMetrics(new DataLoadMetrics())
      DataLoadProcessorStepOnSpark.writeFunc(rows, context.partitionId, loadModel,
        writeStepRowCounter, conf.value.value)
      SparkSQLUtil.setOutputMetrics(context.taskMetrics().outputMetrics, loadModel.getMetrics)
    })
    // clean cache only if persisted and keeping unpersist non-blocking as non-blocking call will
    // not have any functional impact as spark automatically monitors the cache usage on each node
    // and drops out old data partitions in a least-recently used (LRU) fashion.
    if (numPartitions > 1) {
      rdd.unpersist(false)
    }
    // Log the number of rows in each step
    LOGGER.info("Total rows processed in step Sort Processor: " + sortStepRowCounter.value)
    LOGGER.info("Total rows processed in step Data Writer: " + writeStepRowCounter.value)

    updateLoadStatus(model, partialSuccessAccum)
  }

  def getKey(row: Array[AnyRef],
      sortColumnsLength: Int): Array[AnyRef] = {
    val key: Array[AnyRef] = new Array[AnyRef](sortColumnsLength)
    System.arraycopy(row, 0, key, 0, sortColumnsLength)
    key
  }

  private def updateLoadStatus(model: CarbonLoadModel, partialSuccessAccum: LongAccumulator
  ): Array[(String, (LoadMetadataDetails, ExecutionErrors))] = {
    // Update status
    val loadMetadataDetails = new LoadMetadataDetails()
    loadMetadataDetails.setLoadName(model.getSegmentId)
    if (partialSuccessAccum.value != 0) {
      val uniqueLoadStatusId = model.getTableName + CarbonCommonConstants.UNDERSCORE +
        "Partial_Success"
      loadMetadataDetails.setSegmentStatus(SegmentStatus.LOAD_PARTIAL_SUCCESS)
      val executionErrors = ExecutionErrors(FailureCauses.NONE, "")
      executionErrors.failureCauses = FailureCauses.BAD_RECORDS
      Array((uniqueLoadStatusId, (loadMetadataDetails, executionErrors)))
    } else {
      val uniqueLoadStatusId = model.getTableName + CarbonCommonConstants.UNDERSCORE + "Success"
      loadMetadataDetails.setSegmentStatus(SegmentStatus.SUCCESS)
      val executionErrors = ExecutionErrors(FailureCauses.NONE, "")
      Array((uniqueLoadStatusId, (loadMetadataDetails, executionErrors)))
    }
  }

  /**
   * 1. range partition the whole input data
   * 2. for each range, sort the data and writ it to CarbonData files
   */
  def loadDataUsingRangeSort(
      sparkSession: SparkSession,
      model: CarbonLoadModel,
      hadoopConf: Configuration,
      segmentMetaDataAccumulator: CollectionAccumulator[Map[String, SegmentMetaDataInfo]])
  : Array[(String, (LoadMetadataDetails, ExecutionErrors))] = {
    // initialize and prepare row counter
    val sc = sparkSession.sparkContext
    val modelBroadcast = sc.broadcast(model)
    val partialSuccessAccum = sc.longAccumulator("Partial Success Accumulator")
    val inputStepRowCounter = sc.longAccumulator("Input Processor Accumulator")
    val convertStepRowCounter = sc.longAccumulator("Convert Processor Accumulator")
    val sortStepRowCounter = sc.longAccumulator("Sort Processor Accumulator")
    val writeStepRowCounter = sc.longAccumulator("Write Processor Accumulator")

    // 1. Input
    hadoopConf
      .set(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, sparkSession.sparkContext.appName)
    val inputRDD = CsvRDDHelper
      .csvFileScanRDD(sparkSession, model, hadoopConf)
      .mapPartitionsWithIndex { case (index, rows) =>
        DataLoadProcessorStepOnSpark.internalInputFunc(
          rows, index, modelBroadcast, Option(inputStepRowCounter), Option.empty)
      }

    // 2. Convert
    val conf = SparkSQLUtil.broadCastHadoopConf(sc, hadoopConf)
    val convertRDD = inputRDD
      .mapPartitionsWithIndex { case (index, rows) =>
        ThreadLocalSessionInfo.setConfigurationToCurrentThread(conf.value.value)
        DataLoadProcessorStepOnSpark
          .convertFunc(rows, index, modelBroadcast, partialSuccessAccum, convertStepRowCounter)
      }
      .filter(_ != null)

    // 3. Range partition by range_column
    val configuration = DataLoadProcessBuilder.createConfiguration(model)
    val rangeColumnIndex =
      indexOfColumn(model.getRangePartitionColumn, configuration.getDataFields)
    // convert RDD[CarbonRow] to RDD[(rangeColumn, CarbonRow)]
    val keyRDD = convertRDD.keyBy(_.getObject(rangeColumnIndex))
    // range partition by key
    val numPartitions = getNumPartitions(configuration, model, convertRDD)
    val objectOrdering: Ordering[Object] = createOrderingForColumn(model.getRangePartitionColumn)
    import scala.reflect.classTag
    val sampleRDD = getSampleRDD(sparkSession, model, hadoopConf, configuration, modelBroadcast)
    val rangeRDD = keyRDD
      .partitionBy(
        new DataSkewRangePartitioner(
          numPartitions,
          sampleRDD,
          false)(objectOrdering, classTag[Object]))
      .map(_._2)

    // 4. Sort and Write data
    sc.runJob(rangeRDD, (context: TaskContext, rows: Iterator[CarbonRow]) => {
      setTaskListener(model.getTableName, model.getSegmentId, segmentMetaDataAccumulator)
      DataLoadProcessorStepOnSpark.sortAndWriteFunc(rows, context.partitionId, modelBroadcast,
        writeStepRowCounter, conf.value.value)
    })

    // Log the number of rows in each step
    LOGGER.info("Total rows processed in step Input Processor: " + inputStepRowCounter.value)
    LOGGER.info("Total rows processed in step Data Converter: " + convertStepRowCounter.value)
    LOGGER.info("Total rows processed in step Sort Processor: " + sortStepRowCounter.value)
    LOGGER.info("Total rows processed in step Data Writer: " + writeStepRowCounter.value)

    // Update status
    updateLoadStatus(model, partialSuccessAccum)
  }

  /**
   * provide RDD for sample
   * CSVRecordReader(Univocity parser) will output only one column
   */
  private def getSampleRDD(
      sparkSession: SparkSession,
      model: CarbonLoadModel,
      hadoopConf: Configuration,
      configuration: CarbonDataLoadConfiguration,
      modelBroadcast: Broadcast[CarbonLoadModel]
  ): RDD[(Object, Object)] = {
    // initialize and prepare row counter
    val configuration = DataLoadProcessBuilder.createConfiguration(model)
    val header = configuration.getHeader
    val rangeColumn = model.getRangePartitionColumn
    val rangeColumnIndex = (0 until header.length).find {
      index =>
        header(index).equalsIgnoreCase(rangeColumn.getColName)
    }.get
    val rangeField = configuration
      .getDataFields
      .find(dataField => dataField.getColumn.getColName.equals(rangeColumn.getColName))
      .get

    // 1. Input
    val newHadoopConf = new Configuration(hadoopConf)
    newHadoopConf
      .set(CSVInputFormat.SELECT_COLUMN_INDEX, "" + rangeColumnIndex)
    val inputRDD = CsvRDDHelper
      .csvFileScanRDD(sparkSession, model, newHadoopConf)
      .mapPartitionsWithIndex { case (index, rows) =>
        DataLoadProcessorStepOnSpark
          .internalInputFunc(rows, index, modelBroadcast, Option.empty, Option(rangeField))
      }

    // 2. Convert
    val conf = SparkSQLUtil.broadCastHadoopConf(sparkSession.sparkContext, hadoopConf)
    val convertRDD = inputRDD
      .mapPartitionsWithIndex { case (index, rows) =>
        ThreadLocalSessionInfo.setConfigurationToCurrentThread(conf.value.value)
        DataLoadProcessorStepOnSpark
          .sampleConvertFunc(rows, rangeField, index, modelBroadcast)
      }
      .filter(_ != null)

    convertRDD.map(row => (row.getObject(0), null))
  }

  /**
   * calculate the number of partitions.
   */
  private def getNumPartitions(
      configuration: CarbonDataLoadConfiguration,
      model: CarbonLoadModel,
      convertRDD: RDD[CarbonRow]
  ): Int = {
    var numPartitions = CarbonDataProcessorUtil.getGlobalSortPartitions(
      configuration.getDataLoadProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS))
    if (numPartitions <= 0) {
      if (model.getTotalSize <= 0) {
        numPartitions = convertRDD.partitions.length
      } else {
        // calculate the number of partitions
        // better to generate a CarbonData file for each partition
        val totalSize = model.getTotalSize.toDouble
        val table = model.getCarbonDataLoadSchema.getCarbonTable
        numPartitions = getNumPartitionsBasedOnSize(totalSize, table, model, false)
      }
    }
    numPartitions
  }

  def getNumPartitionsBasedOnSize(totalSize: Double,
      table: CarbonTable,
      model: CarbonLoadModel,
      mergerFlag: Boolean): Int = {
    val blockSize = 1024L * 1024 * table.getBlockSizeInMB
    val blockletSize = 1024L * 1024 * table.getBlockletSizeInMB
    val scaleFactor = if (mergerFlag) {
      1
    } else if (model.getScaleFactor == 0) {
      // use system properties
      CarbonProperties.getInstance().getRangeColumnScaleFactor
    } else {
      model.getScaleFactor
    }
    // For Range_Column, it will try to generate one big file for each partition.
    // And the size of the big file is about TABLE_BLOCKSIZE of this table.
    val splitSize = Math.max(blockletSize, (blockSize - blockletSize)) * scaleFactor
    Math.ceil(totalSize / splitSize).toInt
  }

  private def indexOfColumn(column: CarbonColumn, fields: Array[DataField]): Int = {
    (0 until fields.length)
      .find(index => fields(index).getColumn.getColName.equals(column.getColName))
      .get
  }

  private def createOrderingForColumn(column: CarbonColumn): Ordering[Object] = {
    if (column.isDimension) {
      val dimension = column.asInstanceOf[CarbonDimension]
      if (dimension.getDataType == DataTypes.DATE) {
        new PrimitiveOrdering(DataTypes.INT)
      } else {
        if (DataTypeUtil.isPrimitiveColumn(column.getDataType)) {
          new PrimitiveOrdering(column.getDataType)
        } else {
          new ByteArrayOrdering()
        }
      }
    } else {
      new PrimitiveOrdering(column.getDataType)
    }
  }

  def setTaskListener(tableName: String,
      segmentId: String,
      segmentMetaDataAccumulator: CollectionAccumulator[Map[String, SegmentMetaDataInfo]]): Unit = {
    TaskContext.get.addTaskCompletionListener {
      new InsertTaskCompletionListener(null, null, segmentMetaDataAccumulator, tableName, segmentId)
    }
    TaskMetricsMap.initializeThreadLocal()
    val carbonTaskInfo = new CarbonTaskInfo
    carbonTaskInfo.setTaskId(CarbonUtil.generateUUID())
    ThreadLocalTaskInfo.setCarbonTaskInfo(carbonTaskInfo)
  }

  /**
   * create CarbonLoadModel for global_sort
   */
  def createLoadModelForGlobalSort(
      sparkSession: SparkSession,
      carbonTable: CarbonTable
  ): CarbonLoadModel = {
    val conf = SparkSQLUtil.sessionState(sparkSession).newHadoopConf()
    CarbonTableOutputFormat.setDatabaseName(conf, carbonTable.getDatabaseName)
    CarbonTableOutputFormat.setTableName(conf, carbonTable.getTableName)
    CarbonTableOutputFormat.setCarbonTable(conf, carbonTable)
    val fieldList = carbonTable.getCreateOrderColumn
      .asScala
      .map { column =>
        new StructField(column.getColName, column.getDataType)
      }
    CarbonTableOutputFormat.setInputSchema(conf, new StructType(fieldList.asJava))
    val loadModel = CarbonTableOutputFormat.getLoadModel(conf)
    loadModel.setSerializationNullFormat(
      TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName + ",\\N")
    loadModel.setBadRecordsLoggerEnable(
      TableOptionConstant.BAD_RECORDS_LOGGER_ENABLE.getName + ",false")
    loadModel.setBadRecordsAction(
      TableOptionConstant.BAD_RECORDS_ACTION.getName + ",force")
    loadModel.setIsEmptyDataBadRecord(
      DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD + ",false")
    val globalSortPartitions =
      carbonTable.getTableInfo.getFactTable.getTableProperties.get("global_sort_partitions")
    if (globalSortPartitions != null) {
      loadModel.setGlobalSortPartitions(globalSortPartitions)
    }
    loadModel
  }

  /**
   * create DataFrame basing on specified splits
   */
  def createInputDataFrame(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      splits: Seq[InputSplit]
  ): DataFrame = {
    val columns = carbonTable
      .getCreateOrderColumn
      .asScala
      .map(_.getColName)
      .toArray
    val schema = SparkTypeConverter.createSparkSchema(carbonTable, columns)
    val rdd: RDD[InternalRow] = new CarbonScanRDD[CarbonRow](
      sparkSession,
      columnProjection = new CarbonProjection(columns),
      null,
      carbonTable.getAbsoluteTableIdentifier,
      carbonTable.getTableInfo.serialize,
      carbonTable.getTableInfo,
      new CarbonInputMetrics,
      null,
      classOf[SparkDataTypeConverterImpl],
      classOf[CarbonRowReadSupport],
      splits.asJava)
      .map { row =>
        new GenericInternalRow(row.getData.asInstanceOf[Array[Any]])
      }
    SparkSQLUtil.execute(rdd, schema, sparkSession)
  }
}

class PrimitiveOrdering(dataType: DataType) extends Ordering[Object] {
  val comparator = org.apache.carbondata.core.util.comparator.Comparator
    .getComparator(dataType)

  override def compare(x: Object, y: Object): Int = {
    comparator.compare(x, y)
  }
}

class ByteArrayOrdering() extends Ordering[Object] {
  override def compare(x: Object, y: Object): Int = {
    UnsafeComparer.INSTANCE.compareTo(x.asInstanceOf[Array[Byte]], y.asInstanceOf[Array[Byte]])
  }
}

class StringOrdering() extends Ordering[Object] {
  override def compare(x: Object, y: Object): Int = {
    if (x == null) {
      return -1
    } else if (y == null) {
      return 1
    }
    return (x.asInstanceOf[UTF8String]).compare(y.asInstanceOf[UTF8String])
  }
}
