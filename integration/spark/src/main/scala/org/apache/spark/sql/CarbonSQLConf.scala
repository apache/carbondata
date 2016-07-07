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

package org.apache.spark.sql

import org.apache.spark.sql.SQLConf.SQLConfEntry
import org.apache.spark.sql.hive.CarbonSQLDialect

object CarbonSQLConf {

  val PUSH_COMPUTATION =
    SQLConfEntry.booleanConf("spark.sql.carbon.push.computation", defaultValue = Some(true))

  val USE_BINARY_CARBON_AGGREGATOR =
    SQLConfEntry.booleanConf("spark.sql.carbon.binary.aggregator", defaultValue = Some(true))

}

 /**
  * A trait that enables the setting and getting of mutable config parameters/hints.
  *
  */
class CarbonSQLConf extends SQLConf {

  override def dialect: String = {
    getConf(SQLConf.DIALECT,
      classOf[CarbonSQLDialect].getCanonicalName)
  }

  override def caseSensitiveAnalysis: Boolean = getConf(SQLConf.CASE_SENSITIVE, false)

  import CarbonSQLConf._

  private[sql] def pushComputation: Boolean = getConf(PUSH_COMPUTATION)

  private[sql] def useBinaryAggregation: Boolean = getConf(USE_BINARY_CARBON_AGGREGATOR)

}
