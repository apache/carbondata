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
package org.apache.carbondata.spark.testsuite.partition

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.TestQueryExecutor
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestCompactionForPartitionTable extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    dropTable
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    sql(
      """
        | CREATE TABLE originTable (empno int, empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE originTable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
  }

  test("minor compaction") {
    sql("create table part_minor_compact(a String, b int) partitioned by (c int) stored by 'carbondata' tblproperties('PARTITION_TYPE'='LIST','LIST_INFO'='1,2')")
    sql("insert into part_minor_compact select 'a', 2, 3 from originTable limit 1")
    sql("insert into part_minor_compact select 'b', 3, 4 from originTable limit 1")
    sql("insert into part_minor_compact select 'c', 4, 5 from originTable limit 1")
    sql("insert into part_minor_compact select 'd', 1, 2 from originTable limit 1")

    checkAnswer(sql("select * from part_minor_compact where c = 4"), Seq(Row("b", 3, 4)))

    sql("alter table part_minor_compact compact 'minor'")

    checkAnswer(sql("select * from part_minor_compact where c = 4"), Seq(Row("b", 3, 4)))
  }

  test("major compaction") {
    sql("create table part_major_compact(a String, b int) partitioned by (c int) stored by 'carbondata' tblproperties('PARTITION_TYPE'='LIST','LIST_INFO'='1,2')")
    sql("insert into part_major_compact select 'a', 2, 3 from originTable limit 1")
    sql("insert into part_major_compact select 'b', 3, 4 from originTable limit 1")
    sql("insert into part_major_compact select 'c', 4, 5 from originTable limit 1")
    sql("insert into part_major_compact select 'd', 1, 2 from originTable limit 1")

    checkAnswer(sql("select * from part_major_compact where c = 4"), Seq(Row("b", 3, 4)))

    sql("alter table part_major_compact compact 'major'")

    checkAnswer(sql("select * from part_major_compact where c = 4"), Seq(Row("b", 3, 4)))
  }

  override def afterAll = {
    dropTable
  }

  def dropTable = {
    sql("drop table if exists part_minor_compact")
    sql("drop table if exists part_major_compact")
    sql("drop table if exists originTable")
  }

}
