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

import org.apache.spark.Accumulator

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.processing.loading.model.CarbonLoadModel

object GlobalSortHelper {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   *
   * @param loadModel       Carbon load model instance
   * @param badRecordsAccum Accumulator to maintain the load state if 0 then success id !0 then
   *                        partial successfull
   * @param hasBadRecord    if <code>true<code> then load bad records vice versa.
   */
  def badRecordsLogger(loadModel: CarbonLoadModel,
      badRecordsAccum: Accumulator[Int], hasBadRecord: Boolean): Unit = {
    if (hasBadRecord) {
      LOGGER.error("Data Load is partially success for table " + loadModel.getTableName)
      badRecordsAccum.add(1)
    } else {
      LOGGER.info("Data loading is successful for table " + loadModel.getTableName)
    }
  }
}
