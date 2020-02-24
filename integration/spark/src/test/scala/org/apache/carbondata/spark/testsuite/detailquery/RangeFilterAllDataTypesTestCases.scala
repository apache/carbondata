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

package org.apache.carbondata.spark.testsuite.detailquery

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.TimeStampGranularityConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

/**
 * Test Class for Range Filters.
 */
class RangeFilterMyTests extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    //For the Hive table creation and data loading
    sql("drop table if exists filtertestTable")
    sql("drop table if exists NO_DICTIONARY_HIVE_1")
    sql("drop table if exists NO_DICTIONARY_CARBON_1")
    sql("drop table if exists NO_DICTIONARY_CARBON_2")
    sql("drop table if exists NO_DICTIONARY_HIVE_6")
    sql("drop table if exists dictionary_hive_6")
    sql("drop table if exists NO_DICTIONARY_HIVE_7")
    sql("drop table if exists NO_DICTIONARY_CARBON_6")
    sql("drop table if exists NO_DICTIONARY_CARBON")
    sql("drop table if exists NO_DICTIONARY_HIVE")
    sql("drop table if exists complexcarbontable")

    sql("drop table if exists DICTIONARY_CARBON_6")
    sql("drop table if exists NO_DICTIONARY_CARBON_7")
    sql("drop table if exists NO_DICTIONARY_CARBON_8")
    sql("drop table if exists NO_DICTIONARY_HIVE_8")

    //For Carbon cube creation.
    sql("CREATE TABLE DICTIONARY_CARBON_6 (empno string, " +
        "doj Timestamp, workgroupcategory Int, empname String,workgroupcategoryname String, " +
        "deptno Int, deptname String, projectcode Int, projectjoindate Timestamp, " +
        "projectenddate Timestamp, designation String,attendance Int,utilization " +
        "Int,salary Int) STORED AS carbondata "
    )
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE DICTIONARY_CARBON_6 " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )

    sql(
      "create table DICTIONARY_HIVE_6(empno string,empname string,designation string,doj " +
      "Timestamp,workgroupcategory int, " +
      "workgroupcategoryname string,deptno int, deptname string, projectcode int, " +
      "projectjoindate Timestamp,projectenddate Timestamp,attendance int, "
      + "utilization int,salary int) row format delimited fields terminated by ',' " +
      "tblproperties(\"skip.header.line.count\"=\"1\") " +
      ""
    )

    sql(
      s"load data local inpath '$resourcesPath/datawithoutheader.csv' into table " +
      "DICTIONARY_HIVE_6"
    );

    sql("CREATE TABLE NO_DICTIONARY_CARBON_6 (empno string, " +
        "doj Timestamp, workgroupcategory Int, empname String,workgroupcategoryname String, " +
        "deptno Int, deptname String, projectcode Int, projectjoindate Timestamp, " +
        "projectenddate Timestamp, designation String,attendance Int,utilization " +
        "Int,salary Int) STORED AS carbondata "
    )
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE NO_DICTIONARY_CARBON_6 " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )

    sql(
      "create table NO_DICTIONARY_HIVE_6(empno string,empname string,designation string,doj " +
      "Timestamp,workgroupcategory int, " +
      "workgroupcategoryname string,deptno int, deptname string, projectcode int, " +
      "projectjoindate Timestamp,projectenddate Timestamp,attendance int, "
      + "utilization int,salary int) row format delimited fields terminated by ',' " +
      "tblproperties(\"skip.header.line.count\"=\"1\") " +
      ""
    )
    sql(
      s"load data local inpath '$resourcesPath/datawithoutheader.csv' into table " +
      "NO_DICTIONARY_HIVE_6"
    );

    sql("CREATE TABLE NO_DICTIONARY_CARBON (empno string, " +
        "doj Timestamp, workgroupcategory Int, empname String,workgroupcategoryname String, " +
        "deptno Int, deptname String, projectcode Int, projectjoindate Timestamp, " +
        "projectenddate Timestamp, designation String,attendance Int,utilization " +
        "Int,salary Int) STORED AS carbondata "
    )
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE NO_DICTIONARY_CARBON " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )

    sql(
      "create table NO_DICTIONARY_HIVE(empno string,empname string,designation string,doj " +
      "Timestamp,workgroupcategory int, " +
      "workgroupcategoryname string,deptno int, deptname string, projectcode int, " +
      "projectjoindate Timestamp,projectenddate Timestamp,attendance int, "
      + "utilization int,salary int) row format delimited fields terminated by ',' " +
      "tblproperties(\"skip.header.line.count\"=\"1\") " +
      ""
    )
    sql(
      s"load data local inpath '$resourcesPath/datawithoutheader.csv' into table " +
      "NO_DICTIONARY_HIVE"
    );

    sql("CREATE TABLE NO_DICTIONARY_CARBON_8 (empno string, " +
        "doj Timestamp, workgroupcategory Int, empname String,workgroupcategoryname String, " +
        "deptno Int, deptname String, projectcode Int, projectjoindate Timestamp, " +
        "projectenddate Timestamp, designation String,attendance Int,utilization " +
        "Int,salary Int) STORED AS carbondata "
    )
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/rangedata.csv' INTO TABLE NO_DICTIONARY_CARBON_8 " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )

    sql(
      "create table NO_DICTIONARY_HIVE_8(empno string,empname string,designation string,doj " +
      "Timestamp,workgroupcategory int, " +
      "workgroupcategoryname string,deptno int, deptname string, projectcode int, " +
      "projectjoindate Timestamp,projectenddate Timestamp,attendance int, "
      + "utilization int,salary int) row format delimited fields terminated by ',' " +
      "tblproperties(\"skip.header.line.count\"=\"1\") " +
      ""
    )
    sql(
      s"load data local inpath '$resourcesPath/datawithoutheader.csv' into table " +
      "NO_DICTIONARY_HIVE_8"
    );

    sql("create table complexcarbontable(deviceInformationId int, channelsId string," +
        "ROMSize string, purchasedate string, mobile struct<imei:string, imsi:string>," +
        "MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
        "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>," +
        "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
        "double,contractNumber double) " +
        "STORED AS carbondata "
    )
    //CarbonProperties.getInstance()
    //  .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")

    sql(
      s"LOAD DATA local inpath '$resourcesPath/complexdata.csv' INTO table " +
      "complexcarbontable " +
      "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId," +
      "ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber'," +
      "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')"
    )




    try {
      CarbonProperties.getInstance()
        .addProperty(TimeStampGranularityConstants.CARBON_CUTOFF_TIMESTAMP, "2000-12-13 02:10.00")
      CarbonProperties.getInstance()
        .addProperty(TimeStampGranularityConstants.CARBON_TIME_GRANULARITY,
          TimeStampGranularityConstants.TIME_GRAN_SEC.toString
        )
      CarbonProperties.getInstance().addProperty("carbon.direct.dictionary", "true")
      sql("drop table if exists directDictionaryTable")
      sql("drop table if exists directDictionaryTable_hive")
      sql(
        "CREATE TABLE if not exists directDictionaryTable (empno int,doj Timestamp, salary int) " +
        "STORED AS carbondata"
      )

      sql(
        "CREATE TABLE if not exists directDictionaryTable_hive (empno int,doj Timestamp, salary int) " +
        "row format delimited fields terminated by ','"
      )

      val csvFilePath = s"$resourcesPath/rangedatasample.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE directDictionaryTable OPTIONS" +
          "('DELIMITER'= ',', 'QUOTECHAR'= '\"')")
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE directDictionaryTable_hive")

    } catch {
      case x: Throwable =>
        CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
            CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    }
  }

  test("test for dictionary columns"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where workgroupcategory > 1 and workgroupcategory < 5"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_6 where workgroupcategory > 1 and workgroupcategory < 5")
    )
  }

  test("test for dictionary columns OR "){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where workgroupcategory > 1 or workgroupcategory < 5"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_6 where workgroupcategory > 1 or workgroupcategory < 5")
    )
  }

  test("test for dictionary columns OR AND"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where workgroupcategory > 1 or workgroupcategory < 5 and workgroupcategory > 3"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_6 where workgroupcategory > 1 or workgroupcategory < 5 and workgroupcategory > 3")
    )
  }

  test("test for dictionary columns OR AND OR"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where workgroupcategory > 1 or workgroupcategory < 5 and workgroupcategory > 3 or workgroupcategory < 10"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_6 where workgroupcategory > 1 or workgroupcategory < 5 and workgroupcategory > 3 or workgroupcategory < 10")
    )
  }

  test("test range filter for direct dictionary"){
    checkAnswer(
      sql("select doj from directDictionaryTable where doj > '2016-03-14 15:00:17'"),
      sql("select doj from directDictionaryTable_hive where doj > '2016-03-14 15:00:17'")
    )
  }

  test("test range filter for direct dictionary and"){
    checkAnswer(
      sql("select doj from directDictionaryTable where doj > '2016-03-14 15:00:16' and doj < '2016-03-14 15:00:18'"),
      Seq(Row(Timestamp.valueOf("2016-03-14 15:00:17.0"))
      )
    )
  }

  test("test range filter for direct dictionary equality"){
    checkAnswer(
      sql("select doj from directDictionaryTable where doj = '2016-03-14 15:00:16'"),
      Seq(Row(Timestamp.valueOf("2016-03-14 15:00:16.0"))
      )
    )
  }

  test("test range filter for less than filter"){
    sql("drop table if exists timestampTable")
    sql("create table timestampTable (timestampCol timestamp) STORED AS carbondata ")
    sql(s"load data local inpath '$resourcesPath/timestamp.csv' into table timestampTable")
    checkAnswer(sql("select * from timestampTable where timestampCol='1970-01-01 05:30:00'"),
      sql("select * from timestampTable where timestampCol<='1970-01-01 05:30:00'"))
    sql("drop table if exists timestampTable")
  }

  test("test range filter for direct dictionary not equality"){
    checkAnswer(
      sql("select doj from directDictionaryTable where doj != '2016-03-14 15:00:16'"),
      Seq(Row(Timestamp.valueOf("2016-03-14 15:00:09.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:10.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:11.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:12.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:13.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:14.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:15.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:17.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:18.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:19.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:20.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:24.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:25.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:31.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:35.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:38.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:39.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:49.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:50.0")))
    )
  }

  test("test range filter for direct dictionary and with explicit casts"){
    checkAnswer(
      sql("select doj from directDictionaryTable where doj > cast ('2016-03-14 15:00:16' as timestamp) and doj < cast ('2016-03-14 15:00:18' as timestamp)"),
      Seq(Row(Timestamp.valueOf("2016-03-14 15:00:17.0"))
      )
    )
  }

  /*
  Commented this test case
  test("test range filter for direct dictionary and with DirectVals as long") {
    checkAnswer(
      sql(
        "select doj from directDictionaryTable where doj > cast (1457992816l as timestamp) and doj < cast (1457992818l as timestamp)"),
      Seq(Row(Timestamp.valueOf("2016-03-14 15:00:17.0"))
      )
    )
  }
  */


  // Test of Cast Optimization
  test("test range filter for different Timestamp formats"){
    checkAnswer(
      sql("select count(*) from directDictionaryTable where doj = '2016-03-14 15:00:180000000'"),
      Seq(Row(0)
      )
    )
  }

  test("test range filter for different Timestamp formats1"){
    checkAnswer(
      sql("select count(*) from directDictionaryTable where doj = '03-03-14 15:00:18'"),
      Seq(Row(0)
      )
    )
  }

  test("test range filter for different Timestamp formats2"){
    checkAnswer(
      sql("select count(*) from directDictionaryTable where doj = '2016-03-14'"),
      Seq(Row(0)
      )
    )
  }

  test("test range filter for different Timestamp formats3"){
    checkAnswer(
      sql("select count(*) from directDictionaryTable where doj > '2016-03-14 15:00:18.000'"),
      sql("select count(*) from directDictionaryTable_hive where doj > '2016-03-14 15:00:18.000'")
      )
  }

  test("test range filter for different Timestamp In format "){
    checkAnswer(
      sql("select doj from directDictionaryTable where doj in ('2016-03-14 15:00:18', '2016-03-14 15:00:17')"),
      Seq(Row(Timestamp.valueOf("2016-03-14 15:00:17.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:18.0")))
    )
  }

  /*
  test("test range filter for different Timestamp Not In format 5"){
    sql("select doj from directDictionaryTable where doj not in (null, '2016-03-14 15:00:18', '2016-03-14 15:00:17','2016-03-14 15:00:11', '2016-03-14 15:00:12')").show(200, false)
    checkAnswer(
      sql("select doj from directDictionaryTable where doj Not in (null, '2016-03-14 15:00:18', '2016-03-14 15:00:17','2016-03-14 15:00:11', '2016-03-14 15:00:12')"),
      Seq(Row(Timestamp.valueOf("2016-03-14 15:00:09.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:10.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:13.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:14.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:15.0")),
        Row(Timestamp.valueOf("2016-03-14 15:00:16.0")))
    )
  }
  */


  test("test range filter for direct dictionary and boundary"){
    checkAnswer(
      sql("select count(*) from directDictionaryTable where doj > '2016-03-14 15:00:18.0' and doj < '2016-03-14 15:00:09.0'"),
      Seq(Row(0)
      )
    )
  }

  test("test range filter for direct dictionary and boundary 2"){
    checkAnswer(
      sql("select count(*) from directDictionaryTable where doj > '2016-03-14 15:00:23.0' and doj < '2016-03-14 15:00:60.0'"),
      sql("select count(*) from directDictionaryTable_hive where doj > '2016-03-14 15:00:23.0' and doj < '2016-03-14 15:00:60.0'")
    )
  }


  test("test range filter for direct dictionary and boundary 3"){
    checkAnswer(
      sql("select count(*) from directDictionaryTable where doj >= '2016-03-14 15:00:23.0' and doj < '2016-03-14 15:00:60.0'"),
      sql("select count(*) from directDictionaryTable_hive where doj >= '2016-03-14 15:00:23.0' and doj < '2016-03-14 15:00:60.0'")
    )
  }


  test("test range filter for direct dictionary and boundary 4"){
    checkAnswer(
      sql("select count(*) from directDictionaryTable where doj > '2016-03-14 15:00:23.0' and doj < '2016-03-14 15:00:40.0'"),
      sql("select count(*) from directDictionaryTable_hive where doj > '2016-03-14 15:00:23.0' and doj < '2016-03-14 15:00:40.0'")
    )
  }

  test("test range filter for direct dictionary and boundary 5"){
    checkAnswer(
      sql("select count(*) from directDictionaryTable where doj > '2016-03-14 15:00:23.0' and doj <= '2016-03-14 15:00:40.0'"),
      sql("select count(*) from directDictionaryTable_hive where doj > '2016-03-14 15:00:23.0' and doj <= '2016-03-14 15:00:40.0'")
    )
  }

  test("test range filter for direct dictionary more values after filter"){
    checkAnswer(
      sql("select doj from directDictionaryTable where doj > '2016-03-14 15:00:09'"),
      sql("select doj from directDictionaryTable_hive where doj > '2016-03-14 15:00:09'")
    )
  }

  test("test range filter for direct dictionary or condition"){
    checkAnswer(
      sql("select doj from directDictionaryTable where doj > '2016-03-14 15:00:09' or doj > '2016-03-14 15:00:15'"),
      sql("select doj from directDictionaryTable_hive where doj > '2016-03-14 15:00:09' or doj > '2016-03-14 15:00:15'")
    )
  }

  test("test range filter for direct dictionary or and condition"){
    checkAnswer(
      sql("select doj from directDictionaryTable where doj > '2016-03-14 15:00:09' or doj > '2016-03-14 15:00:15' and doj < '2016-03-14 15:00:13'"),
      sql("select doj from directDictionaryTable_hive where doj > '2016-03-14 15:00:09' or doj > '2016-03-14 15:00:15' and doj < '2016-03-14 15:00:13'")
    )
  }

  test("test range filter for direct dictionary with no data in csv"){
    checkAnswer(
      sql("select doj from directDictionaryTable where doj > '2016-03-14 15:05:09' or doj > '2016-03-14 15:05:15' and doj < '2016-03-14 15:50:13'"),
      sql("select doj from directDictionaryTable_hive where doj > '2016-03-14 15:05:09' or doj > '2016-03-14 15:05:15' and doj < '2016-03-14 15:50:13'")
    )
  }

  // use cast for range
  test("test range filter for direct dictionary cast"){
    checkAnswer(
      sql("select doj from directDictionaryTable where doj > cast ('2016-03-14 15:00:17' as timestamp)"),
      sql("select doj from directDictionaryTable_hive where doj > cast ('2016-03-14 15:00:17' as timestamp)")
    )
  }

  test("test range filter for direct dictionary cast and"){
    checkAnswer(
      sql("select doj from directDictionaryTable where doj > cast ('2016-03-14 15:00:16' as timestamp) and doj < cast ('2016-03-14 15:00:18' as timestamp)"),
      Seq(Row(Timestamp.valueOf("2016-03-14 15:00:17.0"))
      )
    )
  }

  test("test range filter for direct dictionary and boundary cast "){
    checkAnswer(
      sql("select count(*) from directDictionaryTable where doj > cast ('2016-03-14 15:00:18.0' as timestamp) and doj < cast ('2016-03-14 15:00:09.0' as timestamp)"),
      Seq(Row(0)
      )
    )
  }

  test("test range filter for direct dictionary and boundary 2 cast "){
    checkAnswer(
      sql("select count(*) from directDictionaryTable where doj > cast('2016-03-14 15:00:23.0' as timestamp) and doj < cast ('2016-03-14 15:00:60.0' as timestamp)"),
      sql("select count(*) from directDictionaryTable_hive where doj > cast('2016-03-14 15:00:23.0' as timestamp) and doj < cast ('2016-03-14 15:00:60.0' as timestamp)")
    )
  }


  test("test range filter for direct dictionary and boundary 3 cast"){
    checkAnswer(
      sql("select count(*) from directDictionaryTable where doj >= cast('2016-03-14 15:00:23.0' as timestamp) and doj < cast('2016-03-14 15:00:60.0' as timestamp)"),
      sql("select count(*) from directDictionaryTable_hive where doj >= cast('2016-03-14 15:00:23.0' as timestamp) and doj < cast('2016-03-14 15:00:60.0' as timestamp)")
    )
  }


  test("test range filter for direct dictionary and boundary 4 cast"){
    checkAnswer(
      sql("select count(*) from directDictionaryTable where doj > cast('2016-03-14 15:00:23.0' as timestamp) and doj < cast('2016-03-14 15:00:40.0' as timestamp)"),
      sql("select count(*) from directDictionaryTable_hive where doj > cast('2016-03-14 15:00:23.0' as timestamp) and doj < cast('2016-03-14 15:00:40.0' as timestamp)")
    )
  }

  test("test range filter for direct dictionary and boundary 5 cast "){
    checkAnswer(
      sql("select count(*) from directDictionaryTable where doj > cast('2016-03-14 15:00:23.0' as timestamp) and doj <= cast('2016-03-14 15:00:40.0' as timestamp)"),
      sql("select count(*) from directDictionaryTable_hive where doj > cast('2016-03-14 15:00:23.0' as timestamp) and doj <= cast('2016-03-14 15:00:40.0' as timestamp)")
    )
  }

  test("test range filter for direct dictionary more values after filter cast"){
    checkAnswer(
      sql("select doj from directDictionaryTable where doj > cast('2016-03-14 15:00:09' as timestamp)"),
      sql("select doj from directDictionaryTable_hive where doj > cast('2016-03-14 15:00:09' as timestamp)")
    )
  }

  test("test range filter for direct dictionary or condition cast"){
    checkAnswer(
      sql("select doj from directDictionaryTable where doj > cast('2016-03-14 15:00:09' as timestamp) or doj > cast('2016-03-14 15:00:15' as timestamp)"),
      sql("select doj from directDictionaryTable_hive where doj > cast('2016-03-14 15:00:09' as timestamp) or doj > cast('2016-03-14 15:00:15' as timestamp)")
    )
  }

  test("test range filter for direct dictionary or and condition cast"){
    checkAnswer(
      sql("select doj from directDictionaryTable where doj > cast('2016-03-14 15:00:09' as timestamp) or doj > cast('2016-03-14 15:00:15' as timestamp) and doj < cast('2016-03-14 15:00:13' as timestamp)"),
      sql("select doj from directDictionaryTable_hive where doj > cast('2016-03-14 15:00:09' as timestamp) or doj > cast('2016-03-14 15:00:15' as timestamp) and doj < cast('2016-03-14 15:00:13' as timestamp)")
    )
  }

  test("test range filter for direct dictionary with no data in csv cast"){
    checkAnswer(
      sql("select doj from directDictionaryTable where doj > cast('2016-03-14 15:05:09' as timestamp) or doj > cast('2016-03-14 15:05:15' as timestamp) and doj < cast('2016-03-14 15:50:13' as timestamp)"),
      sql("select doj from directDictionaryTable_hive where doj > cast('2016-03-14 15:05:09' as timestamp) or doj > cast('2016-03-14 15:05:15' as timestamp) and doj < cast('2016-03-14 15:50:13' as timestamp)")
    )
  }


  test("test range filter for measure in dictionary include"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where deptno > 10 "),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_6 where deptno > 10 ")
    )
  }

  test("test range filter for measure in dictionary include and condition"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where deptno > 10 and deptno < 10"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_6 where deptno > 10 and deptno < 10")
    )
  }

  test("test range filter for measure in dictionary include or condition"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where deptno > 10 or deptno < 10"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_6 where deptno > 10 or deptno < 10")
    )
  }

  test("test range filter for measure in dictionary include or and condition"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where deptno > 10 or deptno < 15 and deptno >12"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_6 where deptno > 10 or deptno < 10 and deptno >12")
    )
  }

  test("test range filter for measure in dictionary include or and condition 1"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where deptno > 10 or deptno < 15 and deptno > 12"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_6 where deptno > 10 or deptno < 15 and deptno > 12")
    )
  }

  test("test range filter for measure in dictionary include boundary values"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where deptno > 14 or deptno < 10 and deptno > 12"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_6 where deptno > 14 or deptno < 10 and deptno > 12")
    )
  }

  test("test range filter for measure in dictionary include same values and"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where deptno > 14 and deptno < 14"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_6 where deptno > 14 and deptno < 14")
    )
  }

  test("test range filter for measure in dictionary include same values or"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where deptno > 14 or deptno < 14"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_6 where deptno > 14 or deptno < 14")
    )
  }

  test("test for dictionary exclude columns"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '11' and empno < '15'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_6 where empno > '11' and empno < '15'")
    )
  }

  test("test for dictionary exclude columns or condition"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '11' or empno > '15'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_6 where empno > '11' or empno > '15'")
    )
  }

  test("test for dictionary exclude columns or and condition"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '11' or empno > '20' and empno < '18'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_6 where empno > '11' or empno > '20' and empno < '18'")
    )
  }

  test("test for dictionary exclude columns boundary condition"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno < '11' or empno > '20'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_6 where empno < '11' or empno > '20'")
    )
  }

  test("test range filter for multiple columns and condition"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '11' and workgroupcategory > 2"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_6 where empno > '11' and workgroupcategory > 2")
    )
  }

  test("test range filter for multiple columns or condition"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '11' or workgroupcategory > 2"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_6 where empno > '11' or workgroupcategory > 2")
    )
  }

  test("test range filter for multiplecolumns conditions"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_8 where empno > '13' and workgroupcategory < 3 "),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_8 where empno > '13' and workgroupcategory < 3 ")
    )
  }

  test("test range filter No Dictionary Range"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_8 where empno > '13' and empno < '17'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_8 where empno > '13' and empno < '17'")
    )
  }

  test("test range filter for more columns conditions"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_8 where empno > '13' and workgroupcategory < 3 and deptno > 12 and empno < '17'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_8 where empno > '13' and workgroupcategory < 3 and deptno >12 and empno < '17'")
    )
  }

  test("test range filter for multiple columns and or combination"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_8 where empno > '13' or workgroupcategory < 3 and deptno > 12 and projectcode > 928478 and empno > '17' "),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_8 where empno > '13' or workgroupcategory < 3 and deptno >12 and projectcode > 928478 and empno > '17'")
    )
  }

  test("test range filter for more columns boundary conditions"){
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_8 where empno > '13' and empno < '17' and workgroupcategory < 1 and deptno > 14 "),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_8 where empno > '13' and empno < '17' and workgroupcategory < 1 and deptno > 14 ")
    )
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table if exists filtertestTable")
    sql("drop table if exists NO_DICTIONARY_HIVE_1")
    sql("drop table if exists NO_DICTIONARY_CARBON_1")
    sql("drop table if exists NO_DICTIONARY_CARBON_2")
    sql("drop table if exists NO_DICTIONARY_HIVE_6")
    sql("drop table if exists directdictionarytable")
    sql("drop table if exists dictionary_hive_6")
    sql("drop table if exists NO_DICTIONARY_HIVE_7")
    sql("drop table if exists NO_DICTIONARY_CARBON_6")
    sql("drop table if exists NO_DICTIONARY_CARBON")
    sql("drop table if exists NO_DICTIONARY_HIVE")
    sql("drop table if exists complexcarbontable")

    sql("drop table if exists DICTIONARY_CARBON_6")
    sql("drop table if exists NO_DICTIONARY_CARBON_7")
    sql("drop table if exists NO_DICTIONARY_CARBON_8")
    sql("drop table if exists NO_DICTIONARY_HIVE_8")
    sql("drop table if exists directdictionarytable_hive")
    //sql("drop cube NO_DICTIONARY_CARBON_1")
  }
}