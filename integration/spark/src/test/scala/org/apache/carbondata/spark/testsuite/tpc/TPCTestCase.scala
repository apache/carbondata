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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TPCTestCase extends QueryTest
    with Tpcds_2_4_Queries
    with BeforeAndAfterAll {
  val tpcdsPath = s"$resourcesPath/tpcds/"

  val tpchPath = s"$resourcesPath/tpch/"

  val CARBODNATA = "carbondata"

  val CARBONDATA_DB = "tpc_carbondata"

  val PARQUET = "parquet"

  val PARQUET_DB = "tpc_parquet"

  val databaseprefix = "tpc_"


  override def beforeAll: Unit = {
    sql(s"CREATE DATABASE IF NOT EXISTS $CARBONDATA_DB")
    sql(s"CREATE DATABASE IF NOT EXISTS $PARQUET_DB")
  }

  test("test TPC-DS") {
    val tables = new TPCDSTables(sqlContext)
    tables.createCSVTable(tpcdsPath)
    tables.createTableAndLoadData(CARBODNATA, CARBONDATA_DB)
    tables.createTableAndLoadData(PARQUET, PARQUET_DB)
    tpcds2_4Queries.foreach { q =>
      val r = sql(q, PARQUET).collect()
      assert(r.size > 0)
      checkAnswer(sql(q, CARBODNATA), r)
    }
  }

  def sql(query: String, format: String): DataFrame = {
    sql(s"USE ${getDatabaseName(format)}")
    sql(query)
  }

  def getDatabaseName(format: String): String = {
    s"$databaseprefix$format"
  }

  override def afterAll: Unit = {
    sql(s"DROP DATABASE IF EXISTS $CARBONDATA_DB cascade")
    sql(s"DROP DATABASE IF EXISTS $PARQUET_DB cascade")
    sql("USE default")
  }
}
