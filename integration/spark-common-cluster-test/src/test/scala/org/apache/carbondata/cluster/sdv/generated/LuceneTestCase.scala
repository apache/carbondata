
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

package org.apache.carbondata.cluster.sdv.generated

import org.apache.spark.sql.common.util.Include
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for LuceneDataMap Testcase to verify all scenarios
 */

class LuceneTestCase extends QueryTest with BeforeAndAfterAll {

  val csvPath = s"$resourcesPath/source.csv"

  override protected def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS datamap_main")
  }

  //Create Lucene DataMap With DMProperties(String DataType) on MainTable
  test("LuceneDataMap_TC001", Include) {
    sql("DROP TABLE IF EXISTS datamap_main")
    sql(
      "CREATE TABLE datamap_main (id Int, date date, country string,name String, phonetype " +
      "string, " +
      "serialname String,salary int ) STORED BY 'org.apache.carbondata.format'")
    sql(
      s"""
         | CREATE DATAMAP lucene_datamap ON TABLE datamap_main
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='country')
      """.stripMargin)
    checkExistence(sql("show datamap on table datamap_main"), true, "lucene_datamap")
    sql("drop datamap if exists lucene_datamap on table datamap_main")
  }

  //Create Lucene DataMap With DMProperties(Other DataTypes) on MainTable
  test("LuceneDataMap_TC002", Include) {
    sql("DROP TABLE IF EXISTS datamap_main")
    sql(
      "CREATE TABLE datamap_main (id Int, date date, country string,name String, phonetype " +
      "string, serialname String,salary int ) STORED BY 'org.apache.carbondata.format' ")
    val exception_otherdataType: Exception = intercept[Exception] {
      sql(
        s"""
           | CREATE DATAMAP lucene_datamap ON TABLE datamap_main
           | USING 'lucene'
           | DMProperties('INDEX_COLUMNS'='id')
      """.stripMargin)
    }
    assert(exception_otherdataType.getMessage
      .contains("Only String column is supported, column 'id' is INT type."))
  }

  //Create Lucene DataMap With DMProperties on MainTable and Load Data and Query
  ignore("LuceneDataMap_TC003", Include) {
    sql("DROP TABLE IF EXISTS datamap_main")
    sql(
      "CREATE TABLE datamap_main (id Int, date string, country string,name String, phonetype " +
      "string, " +
      "serialname String,salary int ) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES" +
      "('SORT_COLUMNS'='country,name','SORT_SCOPE'='LOCAL_SORT')")
    sql(
      s"""
         | CREATE DATAMAP lucene_datamap ON TABLE datamap_main
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='country')
      """.stripMargin)
    sql(s"LOAD DATA INPATH '$csvPath' INTO TABLE datamap_main")

    checkAnswer(sql("SELECT * FROM datamap_main WHERE TEXT_MATCH('country:china')"),
      sql("select * from datamap_main where country='china'"))
    sql("drop datamap if exists lucene_datamap on table datamap_main")
  }

  //Create Different Lucene DataMap With DMProperties on MainTable and filter using 'like','AND'
  // & 'OR'
  ignore("LuceneDataMap_TC004", Include) {
    sql("DROP TABLE IF EXISTS datamap_main")
    sql(
      "CREATE TABLE datamap_main (id Int, date string, country string,name String, phonetype " +
      "string, " +
      "serialname String,salary int ) STORED BY 'org.apache.carbondata.format' ")
    sql(
      s"""
         | CREATE DATAMAP lucene_datamap ON TABLE datamap_main
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='country,name,serialname')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$csvPath' INTO TABLE datamap_main")
    checkAnswer(sql("SELECT * FROM datamap_main WHERE TEXT_MATCH('country:ch*')"),
      sql("select * from datamap_main where country like 'ch%'"))
    checkAnswer(sql(
      "SELECT * FROM datamap_main WHERE TEXT_MATCH('country:ch* AND name:aa*')"),
      sql("select * from datamap_main where country like 'ch%' and name like 'aa%'"))
    checkAnswer(sql(
      "SELECT * FROM datamap_main WHERE TEXT_MATCH('country:u* OR name:aa*')"),
      sql("select * from datamap_main where country like 'u%'or name like 'aa%'"))
    checkAnswer(sql(
      "SELECT * FROM datamap_main WHERE TEXT_MATCH('country:u* OR (name:aaa1* AND name:aaa2*)')"),
      sql(
        "select * from datamap_main where country like 'u%' OR name like 'aaa1%' AND name like " +
        "'aaa2%'"))
    sql("drop datamap if exists lucene_datamap on table datamap_main")
  }

  //Create Different Lucene DataMap With DMProperties on MainTable and check Datamap after Delete
  // Segment
  ignore("LuceneDataMap_TC005", Include) {
    sql("DROP TABLE IF EXISTS datamap_main")
    sql(
      "CREATE TABLE datamap_main (id Int, date date, country string,name String, phonetype " +
      "string, " +
      "serialname String,salary int ) STORED BY 'org.apache.carbondata.format'")
    sql(
      s"""
         | CREATE DATAMAP lucene_datamap ON TABLE datamap_main
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='country,name')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$csvPath' INTO TABLE datamap_main")
    sql(s"LOAD DATA LOCAL INPATH '$csvPath' INTO TABLE datamap_main")
    checkAnswer(sql("SELECT COUNT(*) FROM datamap_main WHERE TEXT_MATCH('country:china')"),
      sql("select COUNT(*) from datamap_main where country='china'"))
    sql("delete from table datamap_main where SEGMENT.ID in (0) ")
    sql("clean files for table datamap_main")
    checkAnswer(sql("SELECT COUNT(*) FROM datamap_main WHERE TEXT_MATCH('country:china')"),
      sql("select COUNT(*) from datamap_main where country='china'"))
    sql("drop datamap if exists lucene_datamap on table datamap_main")
  }

  //Create Different Lucene DataMap With DMProperties on MainTable with different 'TBLProperties'
  // and Load Data with Differnt OPTIONS & Verify
  ignore("LuceneDataMap_TC006", Include) {
    sql("DROP TABLE IF EXISTS datamap_main")
    sql(
      "CREATE TABLE datamap_main (id Int, date string, country string,name String, phonetype " +
      "string, " +
      "serialname String,salary int ) STORED BY 'org.apache.carbondata.format' " +
      "TBLPROPERTIES('SORT_COLUMNS'='country,name','SORT_SCOPE'='LOCAL_SORT')")
    sql(
      s"""
         | CREATE DATAMAP lucene_datamap ON TABLE datamap_main
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='country')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$csvPath' INTO TABLE datamap_main OPTIONS('header'='false'," +
        s"'BAD_RECORDS_LOGGER_ENABLE'='FALSE','BAD_RECORDS_ACTION'='FORCE')")
    checkAnswer(sql("SELECT COUNT(*) FROM datamap_main WHERE TEXT_MATCH('country:china')"),
      sql("select COUNT(*) from datamap_main where country='china'"))
    sql("drop datamap if exists lucene_datamap on table datamap_main")
  }

  //Create LuceneDataMap With DMProperties on MainTable and Insert data and Update and
  // Verify
  ignore("LuceneDataMap_TC007", Include) {
    sql("DROP TABLE IF EXISTS datamap_main")
    sql(
      "CREATE TABLE datamap_main (id Int,country string,name String) STORED BY 'org.apache" +
      ".carbondata.format'")
    sql(
      s"""
         | CREATE DATAMAP lucene_datamap ON TABLE datamap_main
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='country,name')
      """.stripMargin)
    sql("insert into datamap_main select 1,'abc','aa'")
    sql("insert into datamap_main select 2,'def','ab'")
    sql("insert into datamap_main select 3,'ghi','ac'")
    sql("insert into datamap_main select 4,'jkl','ad'")
    checkAnswer(sql("SELECT * FROM datamap_main WHERE TEXT_MATCH('country:def')"),
      sql("select * from datamap_main where country='def'"))
    sql("update datamap_main set (country)=('fed') where id=2")
    checkAnswer(sql("SELECT COUNT(*) FROM datamap_main WHERE TEXT_MATCH('country:fed')"),
      sql("select COUNT(*) from datamap_main where country='fed'"))
    sql("drop datamap if exists lucene_datamap on table datamap_main")
  }

  override protected def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS datamap_main")
  }
}