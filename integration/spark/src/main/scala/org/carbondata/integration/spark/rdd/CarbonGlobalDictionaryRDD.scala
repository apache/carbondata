/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.integration.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import scala.collection.mutable.HashSet
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.integration.spark.util.GlobalDictionaryUtil
import org.apache.spark.Partitioner
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer
import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.integration.spark.util.CarbonSparkInterFaceLogEvent
import org.carbondata.core.carbon.CarbonTableIdentifier

/**
 * A partitioner partition by column.
 *
 * @constructor create a partitioner
 * @param numParts  the number of partitions
 */
class ColumnPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}

/**
 * a case class to package some attributes
 */
case class DictionaryLoadModel(table: CarbonTableIdentifier,
  columnNames: Array[String],
  columnIds: Array[String],
  hdfsLocation: String,
  dictfolderPath: String,
  isSharedDimension: Boolean,
  dictFilePaths: Array[String],
  dictFileExists: Array[Boolean]) extends Serializable

/**
 * A RDD to combine distinct values in block.
 *
 * @constructor create a RDD with RDD[Row]
 * @param prev the input RDD[Row]
 * @param dictfolderPath the dictionary folder
 * @param tableName table name
 * @param indexes location index of require columns
 * @param columns require columns
 */
class CarbonBlockDistinctValuesCombineRDD(
  prev: RDD[Row],
  model: DictionaryLoadModel)
    extends RDD[(Int, HashSet[String])](prev) with Logging {

  override def getPartitions: Array[Partition] = firstParent[Row].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(Int, HashSet[String])] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass().getName());

    val distinctValuesList = new ArrayBuffer[(Int, HashSet[String])]
    try {
      //load exists dictionary file to list of HashMap
      val (dicts, existDicts) = GlobalDictionaryUtil.readGlobalDictionaryFromFile(model)
      //local combine set
      val numColumns = model.columnNames.length
      val sets = new Array[HashSet[String]](numColumns)
      for (i <- 0 until numColumns) {
        sets(i) = new HashSet[String]
        distinctValuesList += ((i, sets(i)))
      }
      var row: Row = null
      var value: String = null
      val rddIter = firstParent[Row].iterator(split, context)
      //generate block distinct value set
      while (rddIter.hasNext) {
        row = rddIter.next()
        if (row != null) {
          for (i <- 0 until numColumns) {
            value = row.getString(i)
            if (value != null) {
              if (existDicts(i)) {
                if (!dicts(i).contains(value)) {
                  sets(i).add(value)
                }
              } else {
                sets(i).add(value)
              }
            }
          }
        }
      }
    } catch {
      case ex: Exception => LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, ex)
    }
    distinctValuesList.toIterator
  }
}

/**
 * A RDD to generate dictionary file for each column
 *
 * @constructor create a RDD with RDD[Row]
 * @param prev the input RDD[Row]
 * @param carbonLoadModel a model package load info
 * @param columns require columns
 * @param hdfsLocation store location
 */
class CarbonGlobalDictionaryGenerateRDD(
  prev: RDD[(Int, HashSet[String])],
  model: DictionaryLoadModel)
    extends RDD[(String, String)](prev) with Logging {

  override def getPartitions: Array[Partition] = firstParent[(Int, HashSet[String])].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(String, String)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass().getName());
    var status = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
    val iter = new Iterator[(String, String)] {
      //generate distinct value list
      try {
        val distinctValues = new HashSet[String]
        val rddIter = firstParent[(Int, HashSet[String])].iterator(split, context)
        while (rddIter.hasNext) {
          distinctValues ++= rddIter.next()._2
        }
        //write to file
        if (!model.dictFileExists(split.index) || distinctValues.size > 0) {
          GlobalDictionaryUtil.writeGlobalDictionaryToFile(model, split.index, distinctValues.toIterator)
          GlobalDictionaryUtil.writeGlobalDictionaryColumnSortInfo(model, split.index);
        }
      } catch {
        case ex: Exception =>
          status = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
          LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, ex)
      }
      var finished = false

      override def hasNext: Boolean = {

        if (!finished) {
          finished = true
          finished
        } else {
          !finished
        }
      }

      override def next(): (String, String) = {
        (model.columnNames(split.index), status)
      }
    }
    iter
  }
}