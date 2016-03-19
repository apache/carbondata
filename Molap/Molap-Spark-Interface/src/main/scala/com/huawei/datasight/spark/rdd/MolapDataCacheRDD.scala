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


package com.huawei.datasight.spark.rdd

import java.text.SimpleDateFormat
import java.util.Date

import com.huawei.datasight.molap.load.MolapLoaderUtil
import com.huawei.datasight.molap.spark.splits.TableSplit
import com.huawei.datasight.molap.spark.util.MolapQueryUtil
import com.huawei.datasight.spark.KeyVal
import com.huawei.unibi.molap.engine.scanner.impl.{MolapKey, MolapValue}
import com.huawei.unibi.molap.olap.MolapDef
import com.huawei.unibi.molap.util.MolapProperties
import org.apache.spark.{Logging, Partition, SerializableWritable, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cubemodel.Partitioner

import scala.collection.JavaConversions._

class MolapCachePartition(rddId: Int, val idx: Int, @transient val tableSplit: TableSplit)
  extends Partition {

  val serializableHadoopSplit = new SerializableWritable[TableSplit](tableSplit)

  override def hashCode(): Int = 41 * (41 + rddId) + idx

  override val index: Int = idx
}

class MolapDataCacheRDD[K, V](
    sc: SparkContext,
    keyClass: KeyVal[K, V],
    schema: MolapDef.Schema,
    baseStoreLocation: String,
    cubeName: String,
    schemaName: String,
    partitioner: Partitioner,
    columinar: Boolean,
    cubeCreationTime: Long)
  extends RDD[(K, V)](sc, Nil) with Logging {
  
  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  private val jobtrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  override def getPartitions: Array[Partition] = {

    val splits = MolapQueryUtil.getTableSplits(schemaName, cubeName, null, partitioner)
    //
    val result = new Array[Partition](splits.length)
    for (i <- 0 until result.length) {
      result(i) = new MolapCachePartition(id, i, splits(i))
    }
    result
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  override def compute(theSplit: Partition, context: TaskContext) = {
    val iter = new Iterator[(K, V)] {
      val currentRestructNumber = MolapLoaderUtil.getCurrentRestructFolder(schemaName, cubeName, schema)
      val split = theSplit.asInstanceOf[MolapCachePartition]
      logInfo("Input split: " + split.serializableHadoopSplit.value)

      if (columinar) {
        println("**************** Loading cube Columnar");
        MolapProperties.getInstance().addProperty("molap.is.columnar.storage", "true");
        MolapProperties.getInstance().addProperty("molap.dimension.split.value.in.columnar", "1");
        MolapProperties.getInstance().addProperty("molap.is.fullyfilled.bits", "true");
        MolapProperties.getInstance().addProperty("is.int.based.indexer", "true");
        MolapProperties.getInstance().addProperty("aggregate.columnar.keyblock", "true");
        MolapProperties.getInstance().addProperty("high.cardinality.value", "100000");
        MolapProperties.getInstance().addProperty("is.compressed.keyblock", "false");
        MolapProperties.getInstance().addProperty("molap.leaf.node.size", "120000");
      }
      MolapProperties.getInstance().addProperty("molap.storelocation", baseStoreLocation);
      MolapQueryUtil.createDataSource(currentRestructNumber, schema, null, split.serializableHadoopSplit.value.getPartition().getUniqueID(), null, null, null, cubeCreationTime);

      // Register an on-task-completion callback to close the input stream.
      context.addOnCompleteCallback(() => close())
      var havePair = false
      var finished = false

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = !false
          havePair = !finished
        }
        !finished
      }

      override def next(): (K, V) = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        val row = new MolapKey(null)
        val value = new MolapValue(null)
        keyClass.getKey(row, value)
      }

      private def close() {
        try {
          //          reader.close()
        } catch {
          case e: Exception => logWarning("Exception in RecordReader.close()", e)
        }
      }
    }
    iter
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[MolapCachePartition]
    val s = theSplit.serializableHadoopSplit.value.getLocations //.filter(_ != "localhost")
    logInfo("Host Name : " + s(0) + s.length)
    s
  }
}

