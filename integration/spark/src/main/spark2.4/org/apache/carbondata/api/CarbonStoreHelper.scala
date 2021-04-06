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

package org.apache.carbondata.api

import java.time.ZoneId

import org.apache.spark.TaskContex
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.util.ThreadLocalSessionInfo

object CarbonStoreHelper {

  def unsetAll(): Unit = {
    TaskContext.get.addTaskCompletionListener(_ => ThreadLocalSessionInfo.unsetAll())
  }

  def validateTimeFormat(timestamp: String): Long = {
    try {
      DateTimeUtils.stringToTimestamp(UTF8String.fromString(timestamp)).get
    } catch {
      case e: Exception =>
        val errorMessage = "Error: Invalid load start time format: " + timestamp
        throw new MalformedCarbonCommandException(errorMessage)
    }
  }

}
