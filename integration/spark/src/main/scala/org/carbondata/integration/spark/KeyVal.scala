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


/**
 * It is just Key value class. I don't get any other alternate to make the RDD class to work with my minimum knowledge in scala.
 * May be I will remove later once I gain good knowledge :)
 *
 */

package org.carbondata.integration.spark

import org.carbondata.core.load.LoadMetadataDetails
import org.carbondata.query.scanner.impl.{MolapKey, MolapValue}

trait KeyVal[K, V] extends Serializable {
  def getKey(key: MolapKey, value: MolapValue): (K, V)

}

class KeyValImpl extends KeyVal[MolapKey, MolapValue] {
  override def getKey(key: MolapKey, value: MolapValue) = (key, value)
}

trait Result[K, V] extends Serializable {
  def getKey(key: Int, value: LoadMetadataDetails): (K, V)

}

class ResultImpl extends Result[Int, LoadMetadataDetails] {
  override def getKey(key: Int, value: LoadMetadataDetails) = (key, value)
}


trait PartitionResult[K, V] extends Serializable {
  def getKey(key: Int, value: Boolean): (K, V)

}

class PartitionResultImpl extends PartitionResult[Int, Boolean] {
  override def getKey(key: Int, value: Boolean) = (key, value)
}

trait MergeResult[K, V] extends Serializable {
  def getKey(key: Int, value: Boolean): (K, V)

}

class MergeResultImpl extends MergeResult[Int, Boolean] {
  override def getKey(key: Int, value: Boolean) = (key, value)
}

trait DeletedLoadResult[K, V] extends Serializable {
  def getKey(key: String, value: String): (K, V)
}

class DeletedLoadResultImpl extends DeletedLoadResult[String, String] {
  override def getKey(key: String, value: String) = (key, value)
}

trait RestructureResult[K, V] extends Serializable {
  def getKey(key: Int, value: Boolean): (K, V)
}

class RestructureResultImpl extends RestructureResult[Int, Boolean] {
  override def getKey(key: Int, value: Boolean) = (key, value)
}
