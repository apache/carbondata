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

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

import org.apache.carbondata.core.metadata.schema.table.CarbonTable

/**
 * Factory class to decide the Source class based on the Source Type.
 */
object SourceFactory extends Enumeration {

  type source = Value

  val KAFKA : SourceFactory.Value = Value("KAFKA")
  val DFS : SourceFactory.Value = Value("DFS")

  var source: Source = _

  def apply(
      sourceType: String,
      ssc: StreamingContext,
      sparkSession: SparkSession,
      carbonTable: CarbonTable): CarbonDStream = {
    SourceFactory.withName(sourceType.toUpperCase) match {
      case KAFKA =>
        source = new AvroKafkaSource(carbonTable)
        source.loadSchemaBasedOnConfiguredClass()
        source.getStream(ssc, sparkSession)
      case DFS =>
        source = new AvroDFSSource(carbonTable)
        source.loadSchemaBasedOnConfiguredClass()
        source.getStream(ssc, sparkSession)
      case other => throw new CarbonDataStreamerException(s"The source type $other is not yet " +
                                                          s"supported")
    }
  }
}
