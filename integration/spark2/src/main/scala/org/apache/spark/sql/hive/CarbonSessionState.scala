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
package org.apache.spark.sql.hive

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.execution.CarbonLateDecodeStrategy
import org.apache.spark.sql.execution.command.DDLStrategy
import org.apache.spark.sql.optimizer.CarbonLateDecodeRule
import org.apache.spark.sql.parser.CarbonSparkSqlParser

/**
 * Session state implementation to override sql parser and adding strategies
 * @param sparkSession
 */
class CarbonSessionState(sparkSession: SparkSession) extends HiveSessionState(sparkSession) {

  override lazy val sqlParser: ParserInterface = new CarbonSparkSqlParser(conf)

  experimentalMethods.extraStrategies =
    Seq(new CarbonLateDecodeStrategy, new DDLStrategy(sparkSession))
  experimentalMethods.extraOptimizations = Seq(new CarbonLateDecodeRule)

}
