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
package org.apache.carbondata.integration.spark.testsuite.binary

import java.util.Arrays

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
  * Test cases for testing big decimal functionality on dimensions
  */
class TestBinaryDataType extends QueryTest with BeforeAndAfterAll {
    override def beforeAll {
    }

    test("create table and load data with binary column") {
        sql("DROP TABLE IF EXISTS binaryTable")
        sql(
            s"""
               | CREATE TABLE IF NOT EXISTS binaryTable (
               |    id int,
               |    label boolean,
               |    name string,
               |    image binary,
               |    autoLabel boolean)
               | STORED BY 'carbondata'
             """.stripMargin)
        sql(
            s"""
               | LOAD DATA LOCAL INPATH '$resourcesPath/binarydata.csv'
               | INTO TABLE binaryTable
               | OPTIONS('header'='false')
             """.stripMargin)
        checkAnswer(sql("SELECT COUNT(*) FROM binaryTable"), Seq(Row(3)))
        try {
            val df = sql("SELECT * FROM binaryTable").collect()
            assert(3 == df.length)
            df.foreach { each =>
                assert(5 == each.length)

                assert(Integer.valueOf(each(0).toString) > 0)
                assert(each(1).toString.equalsIgnoreCase("false") || (each(1).toString.equalsIgnoreCase("true")))
                assert(each(2).toString.contains(".png"))

                val bytes20 = each.getAs[Array[Byte]](3).slice(0, 20)
                val imageName = each(2).toString
                val expectedBytes = firstBytes20.get(imageName).get
                assert(Arrays.equals(expectedBytes, bytes20), "incorrect numeric value for flattened image")

                assert(each(4).toString.equalsIgnoreCase("false") || (each(4).toString.equalsIgnoreCase("true")))
                assert(5 == each.length)
            }
        } catch {
            case e:Exception=>
                e.printStackTrace()
                assert(false)
        }
    }

    private val firstBytes20 = Map("1.png" -> Array[Byte](-119, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 1, 74),
        "2.png" -> Array[Byte](-119, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 2, -11),
        "3.png" -> Array[Byte](-119, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 1, 54)
    )

    test("create no sort table and load data with binary column") {
        sql("DROP TABLE IF EXISTS binaryTable")
        sql(
            s"""
               | CREATE TABLE IF NOT EXISTS binaryTable (
               |    id INT,
               |    label boolean,
               |    name STRING,
               |    image BINARY,
               |    autoLabel boolean)
               | STORED BY 'carbondata'
               | TBLPROPERTIES('SORT_COLUMNS'='')
             """.stripMargin)
        sql(
            s"""
               | LOAD DATA LOCAL INPATH '$resourcesPath/binarydata.csv'
               | INTO TABLE binaryTable
               | OPTIONS('header'='false')
             """.stripMargin)
        checkAnswer(sql("SELECT COUNT(*) FROM binaryTable"), Seq(Row(3)))
    }

    override def afterAll {
    }
}