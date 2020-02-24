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
package org.apache.spark.sql.execution.command.mutation.merge

/**
 * It manages the transaction number for update or delete operations. Since we are applying update
 * and delete delta records in a separate transactions it is required to keep track of transaction
 * numbers.
 */
case class TranxManager(factTimestamp: Long) {

  private var newFactTimestamp: Long = factTimestamp
  private var mutationMap = Map.empty[MutationAction, Long]

  def getNextTransaction(mutationAction: MutationAction): Long = {
    if (mutationMap.isEmpty) {
      mutationMap ++= Map[MutationAction, Long]((mutationAction, newFactTimestamp))
    } else {
      if (mutationMap.get(mutationAction).isDefined) {
        return mutationMap(mutationAction)
      } else {
        newFactTimestamp = newFactTimestamp + 1
        mutationMap ++= Map[MutationAction, Long]((mutationAction, newFactTimestamp))
      }
    }
    newFactTimestamp
  }

  def getLatestTrx: Long = newFactTimestamp

  def getUpdateTrx: Long = {
    val map = mutationMap.filter(_._1.isInstanceOf[HandleUpdateAction])
    if (map.isEmpty) {
      -1
    } else {
      map.head._2
    }
  }

  def getDeleteTrx: Long = {
    val map = mutationMap.filter(_._1.isInstanceOf[HandleDeleteAction])
    if (map.isEmpty) {
      -1
    } else {
      map.head._2
    }
  }
}
