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

package org.apache.carbondata.spark.testsuite.hadooprelation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for hadoop fs relation
 *
 */
class HadoopFSRelationTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists hadoopfsrelation")
    sql("drop table if exists hadoopfsrelation_hive")
    sql(
      "CREATE TABLE hadoopfsrelation (empno int, empname String, designation " +
      "String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno " +
      "int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate " +
      "Timestamp,attendance int,utilization int,salary int)" +
      "STORED BY 'org.apache.carbondata.format'")
    sql(
      s"LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE hadoopfsrelation " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')")
    
    sql("CREATE TABLE hadoopfsrelation_hive (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int)row format delimited fields terminated by ','")
    
    sql(
      s"LOAD DATA local inpath '$resourcesPath/datawithoutheader.csv' INTO TABLE hadoopfsrelation_hive ")
  }

  test("hadoopfsrelation select all test") {
    val rdd = sqlContext.read.format("org.apache.spark.sql.CarbonSource")
      .option("tableName", "hadoopfsrelation").load()
    assert(rdd.collect().length > 0)
  }

  test("hadoopfsrelation filters test") {
    val rdd: DataFrame = sqlContext.read.format("org.apache.spark.sql.CarbonSource")
      .option("tableName", "hadoopfsrelation").load()
      .select("empno", "empname", "utilization").where("empname in ('arvind','ayushi')")
    checkAnswer(
      rdd,
      sql("select empno,empname,utilization from hadoopfsrelation_hive where empname in ('arvind','ayushi')"))
  }

  override def afterAll {
    sql("drop table hadoopfsrelation")
    sql("drop table hadoopfsrelation_hive")
  }
}