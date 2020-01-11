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

package org.apache.carbondata.examples

import org.apache.spark.sql.{SaveMode, SparkSession}

import org.apache.carbondata.examples.util.ExampleUtils

case class StructElement(school: Array[String], age: Int)

case class StructElement1(school: Array[String], school1: Array[String], age: Int)

case class ComplexTypeData(id: Int, name: String, city: String, salary: Float, file: StructElement)

case class ComplexTypeData1(id: Int,
    name: String,
    city: String,
    salary: Float,
    file: StructElement1)

case class ComplexTypeData2(id: Int, name: String, city: String, salary: Float, file: Array[String])

// scalastyle:off println
object DataFrameComplexTypeExample {

  def main(args: Array[String]) {

    val spark = ExampleUtils.createSparkSession("DataFrameComplexTypeExample", 4)
    exampleBody(spark)
    spark.close()
  }

  def exampleBody(spark: SparkSession): Unit = {
    val complexTypeNoDictionaryTableName = s"complex_type_noDictionary_table"
    val complexTypeNoDictionaryTableNameArray = s"complex_type_noDictionary_array_table"

    import spark.implicits._

    // drop table if exists previously
    spark.sql(s"DROP TABLE IF EXISTS ${ complexTypeNoDictionaryTableName }")
    spark.sql(s"DROP TABLE IF EXISTS ${ complexTypeNoDictionaryTableNameArray }")

    spark.sql(
      s"""
         | CREATE TABLE ${ complexTypeNoDictionaryTableNameArray }(
         | id INT,
         | name STRING,
         | city STRING,
         | salary FLOAT,
         | file array<string>
         | )
         | STORED AS carbondata
         | TBLPROPERTIES(
         | 'sort_columns'='name')
         | """.stripMargin)


    spark.sql(
      s"""
         | CREATE TABLE ${ complexTypeNoDictionaryTableName }(
         | id INT,
         | name STRING,
         | city STRING,
         | salary FLOAT,
         | file struct<school:array<string>, school1:array<string>, age:int>
         | )
         | STORED AS carbondata
         | TBLPROPERTIES(
         | 'sort_columns'='name')
         | """.stripMargin)


    val sc = spark.sparkContext

    // generate data
    val df2 = sc.parallelize(Seq(
      ComplexTypeData2(1, "index_1", "city_1", 10000.0f, Array("struct_11", "struct_12")),
      ComplexTypeData2(2, "index_2", "city_2", 20000.0f, Array("struct_21", "struct_22")),
      ComplexTypeData2(3, "index_3", "city_3", 30000.0f, Array("struct_31", "struct_32"))
    )).toDF

    // generate data
    val df1 = sc.parallelize(Seq(
      ComplexTypeData1(1, "index_1", "city_1", 10000.0f,
        StructElement1(Array("struct_11", "struct_12"), Array("struct_11", "struct_12"), 10)),
      ComplexTypeData1(2, "index_2", "city_2", 20000.0f,
        StructElement1(Array("struct_21", "struct_22"), Array("struct_11", "struct_12"), 20)),
      ComplexTypeData1(3, "index_3", "city_3", 30000.0f,
        StructElement1(Array("struct_31", "struct_32"), Array("struct_11", "struct_12"), 30))
    )).toDF

    df1.printSchema()
    df1.write
      .format("carbondata")
      .option("tableName", complexTypeNoDictionaryTableName)
      .mode(SaveMode.Append)
      .save()

    df2.printSchema()
    df2.write
      .format("carbondata")
      .option("tableName", complexTypeNoDictionaryTableNameArray)
      .mode(SaveMode.Append)
      .save()

    spark.sql(s"select count(*) from ${ complexTypeNoDictionaryTableName }")
      .show(100, truncate = false)

    spark.sql(s"select * from ${ complexTypeNoDictionaryTableName } order by id desc")
      .show(300, truncate = false)

    spark.sql(s"select * " +
              s"from ${ complexTypeNoDictionaryTableName } " +
              s"where id = 100000001 or id = 1 limit 100").show(100, truncate = false)

    spark.sql(s"select * " +
              s"from ${ complexTypeNoDictionaryTableName } " +
              s"where id > 10 limit 100").show(100, truncate = false)

    // show segments
    spark.sql(s"SHOW SEGMENTS FOR TABLE ${ complexTypeNoDictionaryTableName }").show(false)

    // drop table
    spark.sql(s"DROP TABLE IF EXISTS ${ complexTypeNoDictionaryTableName }")

    spark.sql(s"select count(*) from ${ complexTypeNoDictionaryTableNameArray }")
      .show(100, truncate = false)

    spark.sql(s"select * from ${ complexTypeNoDictionaryTableNameArray } order by id desc")
      .show(300, truncate = false)

    spark.sql(s"select * " +
              s"from ${ complexTypeNoDictionaryTableNameArray } " +
              s"where id = 100000001 or id = 1 limit 100").show(100, truncate = false)

    spark.sql(s"select * " +
              s"from ${ complexTypeNoDictionaryTableNameArray } " +
              s"where id > 10 limit 100").show(100, truncate = false)

    // show segments
    spark.sql(s"SHOW SEGMENTS FOR TABLE ${ complexTypeNoDictionaryTableNameArray }").show(false)

    // drop table
    spark.sql(s"DROP TABLE IF EXISTS ${ complexTypeNoDictionaryTableNameArray }")
  }
}
// scalastyle:on println
