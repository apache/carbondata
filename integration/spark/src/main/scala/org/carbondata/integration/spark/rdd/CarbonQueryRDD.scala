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


package org.carbondata.integration.spark.rdd

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.{Logging, Partition, SerializableWritable, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.carbon.AbsoluteTableIdentifier
import org.carbondata.core.carbon.datastore.block.TableBlockInfo
import org.carbondata.core.iterator.CarbonIterator
import org.carbondata.hadoop.{CarbonInputFormat, CarbonInputSplit}
import org.carbondata.integration.spark.KeyVal
import org.carbondata.integration.spark.util.CarbonSparkInterFaceLogEvent
import org.carbondata.lcm.status.SegmentStatusManager
import org.carbondata.query.carbon.executor.QueryExecutorFactory
import org.carbondata.query.carbon.model.QueryModel
import org.carbondata.query.carbon.result.RowResult
import org.carbondata.query.expression.Expression



class CarbonSparkPartition(rddId: Int, val idx: Int,
  @transient val carbonInputSplit: CarbonInputSplit)
  extends Partition {

  override val index: Int = idx
  val serializableHadoopSplit = new SerializableWritable[CarbonInputSplit](carbonInputSplit)

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}


 /**
  * This RDD is used to perform query.
  */
  class CarbonQueryRDD[K, V](
  sc: SparkContext,
  queryModel: QueryModel,
  filterExpression: Expression,
  keyClass: KeyVal[K, V],
  @transient conf: Configuration,
  cubeCreationTime: Long,
  schemaLastUpdatedTime: Long,
  baseStoreLocation: String)
  extends RDD[(K, V)](sc, Nil) with Logging {

  private val jobtrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  override def getPartitions: Array[Partition] = {
    val carbonInputFormat = new CarbonInputFormat[RowResult]();
    val jobConf: JobConf = new JobConf(new Configuration)
    val job: Job = new Job(jobConf)
    val absoluteTableIdentifier: AbsoluteTableIdentifier = queryModel.getAbsoluteTableIdentifier()
    FileInputFormat.addInputPath(job, new Path(absoluteTableIdentifier.getStorePath))
    CarbonInputFormat.setTableToAccess(job, absoluteTableIdentifier.getCarbonTableIdentifier)

    val validSegments = new SegmentStatusManager(absoluteTableIdentifier).getValidSegments;
    val validSegmentNos =
      validSegments.listOfValidSegments.asScala.map(x => new Integer(Integer.parseInt(x)))
    CarbonInputFormat.setSegmentsToAccess(job, validSegmentNos.asJava)

    val splits =
      if (filterExpression != null) {
        // set filter resolver tree
        val filterResolver = carbonInputFormat.getResolvedFilter(job, filterExpression)
        queryModel.setFilterExpressionResolverTree(filterResolver)
        // get splits considering filter
        carbonInputFormat.getSplits(job, filterResolver)
      } else {
        // get splits
        carbonInputFormat.getSplits(job)
      }

    val carbonInputSplits = splits.asScala.map(_.asInstanceOf[CarbonInputSplit])

    val result = new Array[Partition](splits.size)
    for (i <- 0 until result.length) {
      result(i) = new CarbonSparkPartition(id, i, carbonInputSplits(i))
    }
    result
  }

  override def compute(thepartition: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass().getName());
    val iter = new Iterator[(K, V)] {
      var rowIterator: CarbonIterator[RowResult] = _
      var queryStartTime: Long = 0
      try {
        val carbonSparkPartition = thepartition.asInstanceOf[CarbonSparkPartition]
        val carbonInputSplit = carbonSparkPartition.serializableHadoopSplit.value

        // fill table block info
        val tableBlockInfoList = new util.ArrayList[TableBlockInfo]();
        tableBlockInfoList.add(new TableBlockInfo(carbonInputSplit.getPath.getName,
          carbonInputSplit.getStart,
          carbonInputSplit.getSegmentId, carbonInputSplit.getLocations, carbonInputSplit.getLength
        )
        )
        queryModel.setTableBlockInfos(tableBlockInfoList)
        queryStartTime = System.currentTimeMillis

        val carbonPropertiesFilePath = System.getProperty("carbon.properties.filepath", null)
        logInfo("*************************" + carbonPropertiesFilePath)
        if (null == carbonPropertiesFilePath) {
          System.setProperty("carbon.properties.filepath", System.getProperty("user.dir")
            + '/' + "conf" + '/' + "carbon.properties"
          );
        }
        // execute query
        rowIterator = QueryExecutorFactory.getQueryExecutor(queryModel).execute(queryModel)
        // TODO: CarbonQueryUtil.isQuickFilter quick filter from dictionary needs to support
      } catch {
        case e: Exception =>
          LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, e)
          // updateCubeAndLevelCacheStatus(levelCacheKeys)
          if (null != e.getMessage) {
            sys.error("Exception occurred in query execution :: " + e.getMessage)
          } else {
            sys.error("Exception occurred in query execution.Please check logs.")
          }
      }

      var havePair = false
      var finished = false

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = !rowIterator.hasNext()
          havePair = !finished
        }
        if (finished) {
          // updateCubeAndLevelCacheStatus(levelCacheKeys)
        }
        !finished
      }

      override def next(): (K, V) = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        val row = rowIterator.next()
        val key = row.getKey()
        val value = row.getValue()
        keyClass.getKey(key, value)
      }

      logInfo("*************************** Total Time Taken to execute the query in Carbon Side: " +
        (System.currentTimeMillis - queryStartTime)
      )
    }
    iter
  }

   /**
    * Get the preferred locations where to launch this task.
    */
  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val theSplit = partition.asInstanceOf[CarbonSparkPartition]
    theSplit.serializableHadoopSplit.value.getLocations.filter(_ != "localhost")
  }
}
