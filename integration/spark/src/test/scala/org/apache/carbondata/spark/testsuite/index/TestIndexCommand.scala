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

package org.apache.carbondata.spark.testsuite.index

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.MetadataProcessException
import org.apache.carbondata.common.exceptions.sql.MalformedIndexCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestIndexCommand extends QueryTest with BeforeAndAfterAll {

  val testData = s"$resourcesPath/sample.csv"

  override def beforeAll {
    sql("drop table if exists indextest")
    sql("drop table if exists indexshowtest")
    sql("drop table if exists uniqdata")
    sql("create table indextest (a string, b string, c string) STORED AS carbondata")
  }

  val newClass = "org.apache.spark.sql.CarbonSource"

  test("test index create: don't support using non-exist class") {
    assert(intercept[MetadataProcessException] {
      sql(s"CREATE INDEX index1 ON indextest (a) AS '$newClass'")
    }.getMessage
      .contains(
        "failed to create IndexClassProvider 'org.apache.spark.sql.CarbonSource'"))
  }

  test("test index create with properties: don't support using non-exist class") {
    assert(intercept[MetadataProcessException] {
      sql(s"CREATE INDEX index2 ON indextest (a) AS '$newClass' PROPERTIES('key'='value')")
    }.getMessage
      .contains(
        "failed to create IndexClassProvider 'org.apache.spark.sql.CarbonSource'"))
  }

  test("test index create with existing name: don't support using non-exist class") {
    assert(intercept[MetadataProcessException] {
      sql(
        s"CREATE INDEX index2 ON indextest (a) AS '$newClass' PROPERTIES('key'='value')")
    }.getMessage
      .contains(
        "failed to create IndexClassProvider 'org.apache.spark.sql.CarbonSource'"))
  }

  test("test show indexes with no index") {
    sql("drop table if exists indexshowtest")
    sql("create table indexshowtest (a string, b string, c string) STORED AS carbondata")
    assert(sql("show indexes on indexshowtest").collect().length == 0)
  }

  test("test show indexes: show index property related information") {
    val tableName = "indexshowtest"
    val indexName = "bloomindex"
    val indexName2 = "bloomindex2"
    val indexName3 = "bloomindex3"
    sql(s"drop table if exists $tableName")
    // for index
    sql(s"create table $tableName (a string, b string, c string) STORED AS carbondata")
    sql(
      s"""
         | create index $indexName
         | on $tableName (a)
         | as 'bloomfilter'
         | PROPERTIES ('bloom_size'='32000', 'bloom_fpp'='0.001')
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName2
         | on table $tableName (b)
         | as 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName3
         | on table $tableName (c)
         | as 'bloomfilter'
       """.stripMargin)
    val result = sql(s"show indexes on $tableName").cache()
    checkAnswer(sql(s"show indexes on $tableName"),
      Seq(Row(indexName, "bloomfilter", "a",
        "'INDEX_COLUMNS'='a','bloom_fpp'='0.001','bloom_size'='32000'", "ENABLED", "NA"),
        Row(indexName2, "bloomfilter", "b", "'INDEX_COLUMNS'='b'", "ENABLED", "NA"),
        Row(indexName3, "bloomfilter", "c", "'INDEX_COLUMNS'='c'", "ENABLED", "NA")))
    result.unpersist()
    sql(s"drop table if exists $tableName")
  }

  test("test don't support lucene on binary data type") {
    val tableName = "indexshowtest20"
    sql(s"drop table if exists $tableName")

    sql(s"CREATE TABLE $tableName(id int, name string, city string, age string, image binary)" +
        s" STORED AS carbondata")

    sql(s"insert into $tableName  values(1,'a3','b3','c1','image2')")
    sql(s"insert into $tableName  values(2,'a3','b2','c2','image2')")
    sql(s"insert into $tableName  values(3,'a1','b2','c1','image3')")
    sql(
      s"""
         | CREATE INDEX agg10
         | ON $tableName (name)
         | AS 'lucene'
         | """.stripMargin)

    checkAnswer(sql(s"show indexes on $tableName"),
      Seq(Row("agg10", "lucene", "name", "'INDEX_COLUMNS'='name'", "ENABLED", "NA")))

    val e = intercept[MalformedIndexCommandException] {
      sql(
        s"""
           | CREATE INDEX agg1
           | ON $tableName (image)
           | AS 'lucene'
           | """.stripMargin)
    }
    assert(e.getMessage.contains("Only String column is supported, column 'image' is BINARY type."))
    checkAnswer(sql(s"show indexes on table $tableName"),
      Seq(Row("agg10", "lucene", "name", "'INDEX_COLUMNS'='name'", "ENABLED", "NA")))

    val pre = sql(
      s"""
         | select name,image, id
         | from $tableName
         | where name = 'a3'
             """.stripMargin)

    assert(2 == pre.collect().length)
    pre.collect().foreach { each =>
      assert(3 == each.length)
      assert("a3".equals(each.get(0)))
      assert("image2".equals(new String(each.getAs[Array[Byte]](1))))
      assert(2 == each.get(2) || 1 == each.get(2))
    }

    sql(s"drop table if exists $tableName")
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS maintable")
    sql("drop table if exists uniqdata")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE,
      CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE_DEFAULT)
    sql("drop table if exists indextest")
    sql("drop table if exists indexshowtest")
  }
}
