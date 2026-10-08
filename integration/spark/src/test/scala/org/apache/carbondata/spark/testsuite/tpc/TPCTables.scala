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

package org.apache.carbondata.spark.testsuite.tpc

import org.apache.spark.sql.SQLContext

class TPCTables(sqlContext: SQLContext) {
  val CSV = "csv"

  val CSVFILESUFFIX = ".csv"

  val tables: Seq[Table] = Seq.empty

  def createTableAndLoadData(format: String, database: String): Unit = {
    tables.foreach { table =>
      table.createTable(sqlContext, format, database)
      table.loadDataFromTempView(sqlContext, database)
    }
  }

  def createCSVTable(location: String): Unit = {
    tables.foreach { table =>
      val tableLocation = s"$location/${table.name}${CSVFILESUFFIX}"
      table.createTemporaryTable(sqlContext, tableLocation, CSV)
    }
  }
}
