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

package org.apache.carbondata.spark.testsuite.datamap

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.MetadataProcessException
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestDataMapCommand extends QueryTest with BeforeAndAfterAll {

  val testData = s"$resourcesPath/sample.csv"

  override def beforeAll {
    sql("drop table if exists datamaptest")
    sql("drop table if exists datamapshowtest")
    sql("drop table if exists uniqdata")
    sql("create table datamaptest (a string, b string, c string) STORED AS carbondata")
  }

  val newClass = "org.apache.spark.sql.CarbonSource"

  test("test datamap create: don't support using non-exist class") {
    intercept[MetadataProcessException] {
      sql(s"CREATE DATAMAP datamap1 ON TABLE datamaptest USING '$newClass'")
    }
  }

  test("test datamap create with dmproperties: don't support using non-exist class") {
    intercept[MetadataProcessException] {
      sql(s"CREATE DATAMAP datamap2 ON TABLE datamaptest USING '$newClass' DMPROPERTIES('key'='value')")
    }
  }

  test("test datamap create with existing name: don't support using non-exist class") {
    intercept[MetadataProcessException] {
      sql(
        s"CREATE DATAMAP datamap2 ON TABLE datamaptest USING '$newClass' DMPROPERTIES('key'='value')")
    }
  }

  test("test show datamap with no datamap") {
    sql("drop table if exists datamapshowtest")
    sql("create table datamapshowtest (a string, b string, c string) STORED AS carbondata")
    assert(sql("show datamap on table datamapshowtest").collect().length == 0)
  }

  test("test show datamap: show datamap property related information") {
    val tableName = "datamapshowtest"
    val datamapName = "bloomdatamap"
    val datamapName2 = "bloomdatamap2"
    val datamapName3 = "bloomdatamap3"
    sql(s"drop table if exists $tableName")
    // for index datamap
    sql(s"create table $tableName (a string, b string, c string) STORED AS carbondata")
    sql(
      s"""
         | create datamap $datamapName on table $tableName using 'bloomfilter'
         | DMPROPERTIES ('index_columns'='a', 'bloom_size'='32000', 'bloom_fpp'='0.001')
       """.stripMargin)
    sql(
      s"""
         | create datamap $datamapName2 on table $tableName using 'bloomfilter'
         | DMPROPERTIES ('index_columns'='b')
       """.stripMargin)
    sql(
      s"""
         | create datamap $datamapName3 on table $tableName using 'bloomfilter'
         | DMPROPERTIES ('index_columns'='c')
       """.stripMargin)
    var result = sql(s"show datamap on table $tableName").cache()
    checkAnswer(sql(s"show datamap on table $tableName"),
      Seq(Row(datamapName, "bloomfilter", s"default.$tableName", "'bloom_fpp'='0.001','bloom_size'='32000','index_columns'='a'", "ENABLED", "NA"),
        Row(datamapName2, "bloomfilter", s"default.$tableName", "'index_columns'='b'","ENABLED", "NA"),
        Row(datamapName3, "bloomfilter", s"default.$tableName", "'index_columns'='c'", "ENABLED", "NA")))
    result.unpersist()
    sql(s"drop table if exists $tableName")

  }

    test("test don't support lucene on binary data type") {
        val tableName = "datamapshowtest20"
        sql(s"drop table if exists $tableName")

        sql(s"CREATE TABLE $tableName(id int, name string, city string, age string, image binary)" +
                s" STORED AS carbondata")

        sql(s"insert into $tableName  values(1,'a3','b3','c1','image2')")
        sql(s"insert into $tableName  values(2,'a3','b2','c2','image2')")
        sql(s"insert into $tableName  values(3,'a1','b2','c1','image3')")
        sql(
            s"""
               | CREATE DATAMAP agg10 ON TABLE $tableName USING 'lucene'
               | DMProperties('INDEX_COLUMNS'='name')
               | """.stripMargin)

        checkAnswer(sql(s"show datamap on table $tableName"),
            Seq(Row("agg10", "lucene", s"default.${tableName}", "'index_columns'='name'", "ENABLED", "NA")))

        val e = intercept[MalformedDataMapCommandException] {
            sql(
                s"""
                   | CREATE DATAMAP agg1 ON TABLE $tableName USING 'lucene'
                   | DMProperties('INDEX_COLUMNS'='image')
                   | """.stripMargin)
        }
        assert(e.getMessage.contains("Only String column is supported, column 'image' is BINARY type."))
        checkAnswer(sql(s"show datamap on table $tableName"),
            Seq(Row("agg10", "lucene", s"default.${tableName}", "'index_columns'='name'", "ENABLED", "NA")))

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
    sql("drop table if exists datamaptest")
    sql("drop table if exists datamapshowtest")
  }
}
