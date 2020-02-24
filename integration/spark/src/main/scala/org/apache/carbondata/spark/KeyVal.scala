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


/**
 * It is just Key value class. I don't get any other alternate to make the RDD class to
 * work with my minimum knowledge in scala.
 * May be I will remove later once I gain good knowledge :)
 *
 */

package org.apache.carbondata.spark

import org.apache.spark.sql.execution.command.ExecutionErrors

import org.apache.carbondata.core.mutate.SegmentUpdateDetails
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus}


trait Value[V] extends Serializable {
  def getValue(value: Array[Object]): V
}

class ValueImpl extends Value[Array[Object]] {
  override def getValue(value: Array[Object]): Array[Object] = value
}

trait RawValue[V] extends Serializable {
  def getValue(value: Array[Any]): V
}

class RawValueImpl extends RawValue[Array[Any]] {
  override def getValue(value: Array[Any]): Array[Any] = value
}

trait DataLoadResult[K, V] extends Serializable {
  def getKey(key: String, value: (LoadMetadataDetails, ExecutionErrors)): (K, V)
}

class DataLoadResultImpl extends DataLoadResult[String, (LoadMetadataDetails, ExecutionErrors)] {
  override def getKey(key: String,
      value: (LoadMetadataDetails, ExecutionErrors)): (String, (LoadMetadataDetails,
    ExecutionErrors)) = {
    (key, value)
  }
}

trait updateResult[K, V] extends Serializable {
  def getKey(key: String,
             value: (LoadMetadataDetails, ExecutionErrors)):
  (K, V)
}

class updateResultImpl
  extends updateResult[String, (LoadMetadataDetails, ExecutionErrors)] {
  override def getKey(key: String,
                      value: (LoadMetadataDetails, ExecutionErrors)):
  (String,
    (LoadMetadataDetails, ExecutionErrors)) = {
    (key, value)
  }
}

trait DeleteDelataResult[K, V] extends Serializable {
  def getKey(key: SegmentStatus, value: (SegmentUpdateDetails, ExecutionErrors, Long)): (K, V)
}

class DeleteDelataResultImpl
  extends DeleteDelataResult[SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long)] {
  override def getKey(key: SegmentStatus,
      value: (SegmentUpdateDetails, ExecutionErrors, Long)): (SegmentStatus, (SegmentUpdateDetails,
    ExecutionErrors, Long)) = {
    (key, value)
  }
}

trait PartitionResult[K, V] extends Serializable {
  def getKey(key: Int, value: Boolean): (K, V)
}

class PartitionResultImpl extends PartitionResult[Int, Boolean] {
  override def getKey(key: Int, value: Boolean): (Int, Boolean) = (key, value)
}

trait MergeResult[K, V] extends Serializable {
  def getKey(key: String, value: Boolean): (K, V)

}

class MergeResultImpl extends MergeResult[String, Boolean] {
  override def getKey(key: String, value: Boolean): (String, Boolean) = (key, value)
}

trait HandoffResult[K, V] extends Serializable {
  def getKey(key: String, value: Boolean): (K, V)

}

class HandoffResultImpl extends HandoffResult[String, Boolean] {
  override def getKey(key: String, value: Boolean): (String, Boolean) = (key, value)
}

trait AlterPartitionResult[K, V] extends Serializable {
  def getKey(key: String, value: Boolean): (K, V)
}

class AlterPartitionResultImpl extends AlterPartitionResult[String, Boolean] {
  override def getKey(key: String, value: Boolean): (String, Boolean) = (key, value)
}

trait DeletedLoadResult[K, V] extends Serializable {
  def getKey(key: String, value: String): (K, V)
}

class DeletedLoadResultImpl extends DeletedLoadResult[String, String] {
  override def getKey(key: String, value: String): (String, String) = (key, value)
}

trait RestructureResult[K, V] extends Serializable {
  def getKey(key: Int, value: Boolean): (K, V)
}

class RestructureResultImpl extends RestructureResult[Int, Boolean] {
  override def getKey(key: Int, value: Boolean): (Int, Boolean) = (key, value)
}

trait RefreshResult[K, V] extends Serializable {
  /**
   * Previously index datamap refresh is per segment, for CARBONDATA-2685 it will refresh
   * all segments in a batch. The structure is taskNo -> (segmentNo, status)
   */
  def getKey(key: String, value: (String, Boolean)): (K, V)
}

class RefreshResultImpl extends RefreshResult[String, (String, Boolean)] {
  override def getKey(key: String,
      value: (String, Boolean)): (String, (String, Boolean)) = (key, value)
}
