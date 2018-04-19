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

package org.apache.carbondata.datamap.lucene

import java.io.{File, PrintWriter}

import scala.util.Random

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.datamap.DataMapStoreManager

class LuceneFineGrainDataMapSuite extends QueryTest with BeforeAndAfterAll {

  val file2 = resourcesPath + "/datamap_input.csv"

  override protected def beforeAll(): Unit = {
    //n should be about 5000000 of reset if size is default 1024
    val n = 15000
    LuceneFineGrainDataMapSuite.createFile(file2)
    sql("create database if not exists lucene")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SYSTEM_FOLDER_LOCATION,
        CarbonEnv.getDatabaseLocation("lucene", sqlContext.sparkSession))
    sql("use lucene")
    sql("DROP TABLE IF EXISTS normal_test")
    sql(
      """
        | CREATE TABLE normal_test(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE normal_test OPTIONS('header'='false')")

    sql("DROP TABLE IF EXISTS datamap_test")
    sql(
      """
        | CREATE TABLE datamap_test(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
  }

  test("validate TEXT_COLUMNS DataMap property") {
    // require TEXT_COLUMNS
    var exception = intercept[MalformedDataMapCommandException](sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test
         | USING 'lucene'
      """.stripMargin))

    assertResult("Lucene DataMap require proper TEXT_COLUMNS property.")(exception.getMessage)

    // illegal argumnet.
    exception = intercept[MalformedDataMapCommandException](sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('text_COLUMNS'='name, ')
      """.stripMargin))

    assertResult("TEXT_COLUMNS contains illegal argument.")(exception.getMessage)

    // not exists
    exception = intercept[MalformedDataMapCommandException](sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('text_COLUMNS'='city,school')
    """.stripMargin))

    assertResult("TEXT_COLUMNS: school does not exist in table. Please check create DataMap statement.")(exception.getMessage)

    // duplicate columns
    exception = intercept[MalformedDataMapCommandException](sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('text_COLUMNS'='name,city,name')
      """.stripMargin))

    assertResult("TEXT_COLUMNS has duplicate columns :name")(exception.getMessage)

    // only support String DataType
    exception = intercept[MalformedDataMapCommandException](sql(
    s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('text_COLUMNS'='city,id')
      """.stripMargin))

    assertResult("TEXT_COLUMNS only supports String column. Unsupported column: id, DataType: INT")(exception.getMessage)
  }

  test("test lucene fine grain data map") {
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('TEXT_COLUMNS'='Name , cIty')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test OPTIONS('header'='false')")

    checkAnswer(sql("SELECT * FROM datamap_test WHERE TEXT_MATCH('name:n10')"), sql(s"select * from datamap_test where name='n10'"))
    checkAnswer(sql("SELECT * FROM datamap_test WHERE TEXT_MATCH('city:c020')"), sql(s"SELECT * FROM datamap_test WHERE city='c020'"))

    sql("drop datamap dm on table datamap_test")
  }

  test("test lucene fine grain data map drop") {
    sql("DROP TABLE IF EXISTS datamap_test1")
    sql(
      """
        | CREATE TABLE datamap_test1(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm12 ON TABLE datamap_test1
         | USING 'org.apache.carbondata.datamap.lucene.LuceneFineGrainDataMapFactory'
         | DMProperties('TEXT_COLUMNS'='Name , cIty')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test1 OPTIONS('header'='false')")

    checkAnswer(sql("SELECT * FROM datamap_test1 WHERE TEXT_MATCH('name:n10')"), sql(s"select * from datamap_test1 where name='n10'"))

    intercept[Exception] {
      sql("drop datamap dm12")
    }
    val schema = DataMapStoreManager.getInstance().getDataMapSchema("dm12")
    sql("drop datamap dm12 on table datamap_test1")
    intercept[Exception] {
      val schema = DataMapStoreManager.getInstance().getDataMapSchema("dm12")
    }
    sql("DROP TABLE IF EXISTS datamap_test1")
  }

  test("test lucene fine grain data map show") {
    sql("DROP TABLE IF EXISTS datamap_test2")
    sql("DROP TABLE IF EXISTS datamap_test3")
    sql(
      """
        | CREATE TABLE datamap_test2(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm122 ON TABLE datamap_test2
         | USING 'org.apache.carbondata.datamap.lucene.LuceneFineGrainDataMapFactory'
         | DMProperties('TEXT_COLUMNS'='Name , cIty')
      """.stripMargin)

    sql(
      """
        | CREATE TABLE datamap_test3(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm123 ON TABLE datamap_test3
         | USING 'org.apache.carbondata.datamap.lucene.LuceneFineGrainDataMapFactory'
         | DMProperties('TEXT_COLUMNS'='Name , cIty')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test2 OPTIONS('header'='false')")

    checkAnswer(sql("SELECT * FROM datamap_test2 WHERE TEXT_MATCH('name:n10')"), sql(s"select * from datamap_test2 where name='n10'"))

    assert(sql("show datamap on table datamap_test2").count() == 1)
    assert(sql("show datamap").count() == 2)
    sql("DROP TABLE IF EXISTS datamap_test2")
    sql("DROP TABLE IF EXISTS datamap_test3")
  }

  override protected def afterAll(): Unit = {
    LuceneFineGrainDataMapSuite.deleteFile(file2)
    sql("DROP TABLE IF EXISTS normal_test")
    sql("DROP TABLE IF EXISTS datamap_test")
    sql("DROP TABLE IF EXISTS datamap_test1")
    sql("DROP TABLE IF EXISTS datamap_test2")
    sql("DROP TABLE IF EXISTS datamap_test3")
    sql("use default")
    sql("drop database if exists lucene cascade")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SYSTEM_FOLDER_LOCATION,
          CarbonProperties.getStorePath)
  }
}

object LuceneFineGrainDataMapSuite {
  def createFile(fileName: String, line: Int = 10000, start: Int = 0) = {
    val write = new PrintWriter(new File(fileName))
    for (i <- start until (start + line)) {
      write.println(i + "," + "n" + i + "," + "c0" + i + "," + Random.nextInt(80))
    }
    write.close()
  }

  def deleteFile(fileName: String): Unit = {
      val file = new File(fileName)
      if (file.exists()) {
        file.delete()
      }
  }
}
