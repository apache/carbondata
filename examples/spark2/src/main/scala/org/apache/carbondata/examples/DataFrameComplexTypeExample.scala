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

import org.apache.spark.sql.SaveMode

case class StructElement(school: Array[String], age: Int)
case class ComplexTypeData(id: Int, name: String, city: String, salary: Float, file: StructElement)

// scalastyle:off println
object DataFrameComplexTypeExample {

  def main(args: Array[String]) {

    val spark = ExampleUtils.createCarbonSession("DataFrameComplexTypeExample", 4)
    val complexTableName = s"complex_type_table"

    import spark.implicits._

    // drop table if exists previously
    spark.sql(s"DROP TABLE IF EXISTS ${ complexTableName }")
    spark.sql(
      s"""
         | CREATE TABLE ${ complexTableName }(
         | id INT,
         | name STRING,
         | city STRING,
         | salary FLOAT,
         | file struct<school:array<string>, age:int>
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES(
         | 'sort_columns'='name',
         | 'dictionary_include'='city')
         | """.stripMargin)

    val sc = spark.sparkContext
    // generate data
    val df = sc.parallelize(Seq(
        ComplexTypeData(1, "index_1", "city_1", 10000.0f,
            StructElement(Array("struct_11", "struct_12"), 10)),
        ComplexTypeData(2, "index_2", "city_2", 20000.0f,
            StructElement(Array("struct_21", "struct_22"), 20)),
        ComplexTypeData(3, "index_3", "city_3", 30000.0f,
            StructElement(Array("struct_31", "struct_32"), 30))
      )).toDF
    df.printSchema()
    df.write
      .format("carbondata")
      .option("tableName", complexTableName)
      .option("single_pass", "true")
      .mode(SaveMode.Append)
      .save()

    spark.sql(s"select count(*) from ${ complexTableName }").show(100, truncate = false)

    spark.sql(s"select * from ${ complexTableName } order by id desc").show(300, truncate = false)

    spark.sql(s"select * " +
              s"from ${ complexTableName } " +
              s"where id = 100000001 or id = 1 limit 100").show(100, truncate = false)

    spark.sql(s"select * " +
              s"from ${ complexTableName } " +
              s"where id > 10 limit 100").show(100, truncate = false)

    // show segments
    spark.sql(s"SHOW SEGMENTS FOR TABLE ${complexTableName}").show(false)

    spark.stop()
  }
}
// scalastyle:on println
