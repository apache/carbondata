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

package org.apache.carbondata.store

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

/**
 * Registered as a database format via @link{org.apache.spark.sql.sources.DataSourceRegister}
 */
class CarbonSource extends RelationProvider with SchemaRelationProvider
  with DataSourceRegister {

  // format short name
  override def shortName(): String = "carbon"

  /**
   * create relation called for getting the schema when schema is not passed
   * example: df.format("carbpn").option("tableName", "carbonsession_table").load()
   *
   * @param sqlContext
   * @param parameters
   * @return : Instance of @link{CarbonSourceRelation}
   */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    CarbonSourceRelation(sqlContext.sparkSession, parameters, None)
  }

  /**
   * create relation with provided schema
   *
   * @param sqlContext
   * @param parameters
   * @param schema
   * @return Instance of @link{CarbonSourceRelation}
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    CarbonSourceRelation(sqlContext.sparkSession, parameters, Option(schema))
  }
}
