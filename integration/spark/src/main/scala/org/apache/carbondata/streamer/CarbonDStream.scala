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
package org.apache.carbondata.streamer

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql.execution.command.mutation.merge.MergeOperationType
import org.apache.spark.streaming.dstream.DStream

/**
 * Wrapper class to hold the spark's DStream object as Dstream can be of different types based on
 * the different input sources like text, avro, kafka etc.
 * @param inputDStream Spark's DStream object
 */
case class CarbonDStream(inputDStream: DStream[Any]) extends Serializable {

  /**
   * Performs the merge operation onto target carbondata table based on the operation type.
   * @param targetDsOri target dataset of carbondata table.
   * @param srcDS source dataset prepared from different sources like kafka, avro, json etc.
   * @param keyColumn the join column based on which merge is performed.
   * @param mergeOperationType Merge operation type to perform, can be UPSERT, UPDATE, INSERT and
   *                           DELETE.
   */
  def performMergeOperation(
      targetDsOri: Dataset[Row],
      srcDS: Dataset[Row],
      keyColumn: String,
      mergeOperationType: String): Unit = {
    MergeOperationType.withName(mergeOperationType.toUpperCase) match {
      case MergeOperationType.UPSERT =>
        targetDsOri.upsert(srcDS, keyColumn).execute()
      case MergeOperationType.UPDATE =>
        targetDsOri.update(srcDS, keyColumn).execute()
      case MergeOperationType.DELETE =>
        targetDsOri.delete(srcDS, keyColumn).execute()
      case MergeOperationType.INSERT =>
        targetDsOri.insert(srcDS, keyColumn).execute()
    }
  }

}
