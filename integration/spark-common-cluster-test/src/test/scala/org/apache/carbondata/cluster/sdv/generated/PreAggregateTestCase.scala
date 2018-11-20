
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

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.{Include, QueryTest}
import org.junit.Assert
import org.scalatest.BeforeAndAfterEach

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties


class PreAggregateTestCase extends QueryTest with BeforeAndAfterEach {
  val csvPath = s"$resourcesPath/source.csv"

  override def beforeEach: Unit = {
    sql("drop table if exists PreAggMain")
    sql("drop table if exists AggMain")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
    sql("CREATE TABLE PreAggMain (id Int, date date, country string, phonetype string, " +
        "serialname String,salary int ) STORED BY 'org.apache.carbondata.format' " +
        "tblproperties('dictionary_include'='country')")
    sql("CREATE TABLE AggMain (id Int, date date, country string, phonetype string, " +
        "serialname String,salary int ) STORED BY 'org.apache.carbondata.format'" +
        "tblproperties('dictionary_include'='country')")
    sql("create datamap PreAggSum on table PreAggMain using 'preaggregate' as " +
        "select country,sum(salary) as sum from PreAggMain group by country")
    sql("create datamap PreAggAvg on table PreAggMain using 'preaggregate' as " +
        "select country,avg(salary) as avg from PreAggMain group by country")
    sql("create datamap PreAggCount on table PreAggMain using 'preaggregate' as " +
        "select country,count(salary) as count from PreAggMain group by country")
    sql("create datamap PreAggMin on table PreAggMain using 'preaggregate' as " +
        "select country,min(salary) as min from PreAggMain group by country")
    sql("create datamap PreAggMax on table PreAggMain using 'preaggregate' as " +
        "select country,max(salary) as max from PreAggMain group by country")
  }

  //test to check existence of datamap
  test("PreAggregateTestCase_TC001", Include) {
    Assert.assertEquals(sql("show datamap on table PreAggMain").count(), 5)
    checkExistence(sql("Describe formatted PreAggMain_PreAggSum"), true, "Dictionary")
  }

  //check for load data should reflects in all preaggregate tables
  test("PreAggregateTestCase_TC002", Include) {
    sql(s"LOAD DATA INPATH '$csvPath' into table PreAggMain ").collect
    sql(s"LOAD DATA INPATH '$csvPath' into table AggMain ").collect

    val expectedSum = sql("select country,sum(salary) as sum from AggMain group by country")
    checkAnswer(sql("select * from PreAggMain_PreAggSum"), expectedSum)

    val expectedAvg = sql("select country,sum(salary),count(salary) from AggMain group by country")
    checkAnswer(sql("select * from PreAggMain_PreAggAvg"), expectedAvg)

    val expectedCount = sql("select country,count(salary) as count from AggMain group by country")
    checkAnswer(sql("select * from PreAggMain_PreAggCount"), expectedCount)

    val expectedMin = sql("select country,min(salary) as min from AggMain group by country")
    checkAnswer(sql("select * from PreAggMain_PreAggMin"), expectedMin)

    val expectedMax = sql("select country,max(salary) as max from AggMain group by country")
    checkAnswer(sql("select * from PreAggMain_PreAggMax"), expectedMax)
  }


  //test for incremental load
  test("PreAggregateTestCase_TC003", Include) {
    sql(s"LOAD DATA INPATH '$csvPath' into table PreAggMain").collect
    sql(s"LOAD DATA INPATH '$csvPath' into table PreAggMain").collect
    sql(s"LOAD DATA INPATH '$csvPath' into table AggMain").collect

    val expectedSum = sql("select country,sum(salary) as sum from " +
                          "AggMain group by country")
    checkAnswer(sql("select * from PreAggMain_PreAggSum"), expectedSum.union(expectedSum))

    val expectedAvg = sql("select country,sum(salary),count(salary) from AggMain group by country")
    checkAnswer(sql("select * from PreAggMain_PreAggAvg"), expectedAvg.union(expectedAvg))

    val expectedCount = sql("select country,count(salary) as count from AggMain group by country")
    checkAnswer(sql("select * from PreAggMain_PreAggCount"), expectedCount.union(expectedCount))

    val expectedMin = sql("select country,min(salary) as min from AggMain group by country")
    checkAnswer(sql("select * from PreAggMain_PreAggMin"), expectedMin.union(expectedMin))

    val expectedMax = sql("select country,max(salary) as max from AggMain group by country")
    checkAnswer(sql("select * from PreAggMain_PreAggMax"), expectedMax.union(expectedMax))
  }

  //test for creating datamap having data from all segment after incremental load
  test("PreAggregateTestCase_TC004", Include) {
    sql("insert into PreAggMain values(1,'2015/7/23','country1','phone197','ASD69643',15000)")
    sql("insert into PreAggMain values(2,'2015/8/23','country2','phone197','ASD69643',10000)")
    sql("insert into PreAggMain values(3,'2005/7/28','country1','phone197','ASD69643',5000)")
    sql("create datamap testDataMap on table PreAggMain using 'preaggregate' as " +
        "select country,sum(salary) as sum from PreAggMain group by country")
    checkAnswer(sql("select * from PreAggMain_testDataMap"),
      Seq(Row("country1", 20000), Row("country2", 10000)))
  }

  //test for insert overwrite in main table
  test("PreAggregateTestCase_TC005", Include) {
    sql("insert into PreAggMain values(1,'2015/7/23','country1','phone197','ASD696',15000)")
    sql("insert into PreAggMain values(2,'2003/8/13','country2','phone197','AD6943',10000)")
    sql("insert overwrite table PreAggMain values(3,'2005/7/28','country3','phone197','ASD69643',5000)")
    sql("create datamap testDataMap on table PreAggMain using 'preaggregate' as " +
        "select country,sum(salary) as sum from PreAggMain group by country")
    checkAnswer(sql("select * from PreAggMain_testDataMap"), Seq(Row("country3", 5000)))
  }

  // test output for join query with preaggregate and without preaggregate
  test("PreAggregateTestCase_TC006", Include) {
    sql(s"LOAD DATA INPATH '$csvPath' into table PreAggMain").collect
    sql(s"LOAD DATA INPATH '$csvPath' into table AggMain").collect
    val actual = sql("select t1.country,sum(id) from PreAggMain t1 join " +
                     "(select country as newcountry,sum(salary) as sum from PreAggMain group by " +
                     "country )t2 on t1.country=t2.newcountry group by country")

    val expected = sql("select t1.country,sum(id) from AggMain t1 join " +
                       "(select country as newcountry,sum(salary) as sum from AggMain group by " +
                       "country )t2 on t1.country=t2.newcountry group by country")
    checkAnswer(actual, expected)
  }

  test("PreAggregateTestCase_TC007", Include) {
    sql(s"LOAD DATA INPATH '$csvPath' into table PreAggMain").collect
    sql(s"LOAD DATA INPATH '$csvPath' into table AggMain").collect
    val actual = sql("select t1.country,count(t1.country) from PreAggMain t1 join " +
                     " (select country,count(salary) as count from PreAggMain group by country )" +
                     "t2 on t1.country=t2.country group by t1.country")

    val expected = sql("select t1.country,count(t1.country) from AggMain t1 join " +
                       "(select country,count(salary) as count from AggMain group by country )t2 " +
                       "on t1.country=t2.country group by t1.country")
    checkAnswer(actual, expected)
  }

  //test to verify correct data in preaggregate table
  test("PreAggregateTestCase_TC008", Include) {
    sql(s"LOAD DATA INPATH '$csvPath' into table PreAggMain").collect
    sql(s"LOAD DATA INPATH '$csvPath' into table AggMain").collect
    sql("create datamap testDatamap on table PreAggMain using 'preaggregate' as " +
        "select sum(CASE WHEN country='china' THEN id ELSE 0 END) as sum,country from " +
        "PreAggMain group by country")
    val actual = sql("select * from PreAggMain_testDatamap")
    val expected = sql(
      "select sum(CASE WHEN country='china' THEN id ELSE 0 END) as sum,country " +
      "from AggMain group by country")
    checkAnswer(actual, expected)
  }

  //test for select using in clause in preaggregate table
  test("PreAggregateTestCase_TC009", Include) {
    sql(s"LOAD DATA INPATH '$csvPath' into table PreAggMain").collect
    sql(s"LOAD DATA INPATH '$csvPath' into table AggMain").collect
    sql("create datamap testDatamap on table PreAggMain using 'preaggregate' as " +
        "select sum(CASE WHEN id in (10,11,12) THEN salary ELSE 0 END) as sum from PreAggMain " +
        "group by country")
    val actual = sql("select * from PreAggMain_testDatamap")
    val expected = sql(
      "select sum(CASE WHEN id in (10,11,12) THEN salary ELSE 0 END) as sum,country from AggMain " +
      "group by country")
    checkAnswer(actual, expected)
  }

  //test to check data using preaggregate and without preaggregate
  test("PreAggregateTestCase_TC010", Include) {
    sql(s"LOAD DATA INPATH '$csvPath' into table PreAggMain").collect
    sql(s"LOAD DATA INPATH '$csvPath' into table AggMain").collect

    val actual = sql(
      "select count(CASE WHEN country='usa' THEN id ELSE 0 END) as count,country from PreAggMain " +
      "group by country")
    val expected = sql(
      "select count(CASE WHEN country='usa' THEN id ELSE 0 END) as count,country from AggMain group" +
      " by country")
    checkAnswer(actual, expected)
  }

  //test to check data using preaggregate and without preaggregate with in clause
  test("PreAggregateTestCase_TC011", Include) {
    sql(s"LOAD DATA INPATH '$csvPath' into table PreAggMain").collect
    sql(s"LOAD DATA INPATH '$csvPath' into table AggMain").collect
    sql("create datamap testDatamap on table PreAggMain using 'preaggregate' as " +
        "select sum(CASE WHEN id in (12,13,14) THEN salary ELSE 0 END) as sum from PreAggMain " +
        "group by country")
    val actual = sql(
      "select sum(CASE WHEN id in (12,13,14) THEN salary ELSE 0 END) as sum,country from " +
      "PreAggMain group by country")
    val expected = sql(
      "select sum(CASE WHEN id in (12,13,14) THEN salary ELSE 0 END) as sum,country from AggMain " +
      "group by country")
    checkAnswer(actual, expected)
  }

  test("Test CleanUp of Pre_aggregate tables") {
    sql("drop table if exists maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("create datamap ag1 on table maintable using 'preaggregate' as select name,sum(price) from maintable group by name")
    sql("insert into table maintable select 'abcd',22,3000")
    sql("insert into table maintable select 'abcd',22,3000")
    sql("insert into table maintable select 'abcd',22,3000")
    sql("alter table maintable compact 'minor'")
    sql("clean files for table maintable")
    assert(sql("show segments for table maintable").collect().head.get(0).toString.contains("0.1"))
  }

  override def afterEach: Unit = {
    sql("drop table if exists mainTable")
  }

}
