package org.apache.carbondata.spark.testsuite.joinquery

import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.execution.joins.BroadCastFilterPushJoin
import org.apache.spark.sql.Row
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.spark.sql.test.util.QueryTest

class EquiJoinTestCase extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("drop table if exists employee_hive")
    sql("drop table if exists mobile_hive")
    sql("drop table if exists employee")
    sql("drop table if exists mobile")
    sql("drop table if exists bit_int2")
    sql("drop table if exists big_int23")
    //loading to hive table
    sql(
      "create table employee_hive (empid string,empname string,mobilename string,mobilecolor " +
      "string,salary int)row format delimited fields terminated by ','")
    sql(
      "create table mobile_hive (mobileid string,mobilename string, mobilecolor string, sales " +
      "int)row format delimited fields terminated by ','")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/join/employee.csv' into table employee_hive")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/join/mobile.csv' into table mobile_hive")
    //loading to carbon table
    sql(
      "create table employee (empid string,empname string,mobilename string,mobilecolor string," +
      "salary int) stored by 'org.apache.carbondata.format'")
    sql(
      "create table employee_dictexc (empid string,empname string,mobilename string,mobilecolor " +
      "string,salary int) stored by 'org.apache.carbondata.format' tblproperties" +
      "('dictionary_exclude'='mobilename')")
    sql(
      "create table mobile (mobileid string,mobilename string, mobilecolor string, sales int) " +
      "stored by 'org.apache.carbondata.format'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/join/employee.csv' into table employee options" +
        s"('FILEHEADER'='empid,empname,mobilename,mobilecolor,salary'," +
        s"'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/join/employee.csv' into table employee_dictexc " +
        s"options('FILEHEADER'='empid,empname,mobilename,mobilecolor,salary'," +
        s"'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/join/mobile.csv' into table mobile options" +
        s"('FILEHEADER'='mobileid,mobilename,mobilecolor,sales'," +
        s"'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")
  }

  test("test equal join query with filter and sort") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table if exists big_int2")
    sql("drop table if exists big_int23")
    sql(
      """
        CREATE TABLE big_int2(imei string,age int,task bigint,name string,country string,city
        string,sale int,num double,
        level decimal(10,3),quest bigint,productdate timestamp,enddate timestamp,PointId double,
        score decimal(10,3))
        STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('dictionary_include' = 'name,
        productdate,sale,num,level,quest,age,task',
        'DICTIONARY_EXCLUDE'= 'imei')
      """)
    sql(s"LOAD DATA INPATH '$resourcesPath/join/big_int.csv' INTO TABLE big_int2 options " +
        s"('DELIMITER'=',', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")

    sql(
      """
        CREATE TABLE big_int23(imei string,age int,task bigint,name string,country string,city
        string,sale int,num double,
        level decimal(10,3),quest bigint,productdate timestamp,enddate timestamp,PointId double,
        score decimal(10,3))
        STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('dictionary_include' = 'name,
        productdate,sale,num,level,quest,age,task',
        'DICTIONARY_EXCLUDE'= 'imei')
      """)
    sql(s"LOAD DATA INPATH '$resourcesPath/join/big_int.csv' INTO TABLE big_int23 options " +
        s"('DELIMITER'=',', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")

    checkAnswer(sql(
      """
         select b.imei,b.country from big_int2 a left outer join big_int23 b on a.imei=b.imei and
          a.age=b.age and a.task=b.task
         and a.name=b.name and a.city=b.city and a.country=b.country and a.sale=b.sale and a
         .num=b.num and a.level=b.level
         and a.quest=b.quest and a.productdate=b.productdate and a.enddate=b.enddate and a
         .pointid=b.pointid and a.score=b.score order by imei limit 100
      """),
      Seq(Row(null, null),
        Row(null, null),
        Row("imei0", "china"),
        Row("imei1", "America"),
        Row("imei2", "china"),
        Row("imei3", "America")))
  }

  //  test("test equijoin query") {
  //    val df = sql(
  //      "select employee.empname,mobile.mobilename from employee,mobile where employee.mobilename =" +
  //      " mobile.mobilename")
  //    var broadcastJoinExists = false
  //    df.queryExecution.sparkPlan.collect {
  //      case bcf: BroadCastFilterPushJoin =>
  //        broadcastJoinExists = true
  //    }
  //    if (!broadcastJoinExists) {
  //      assert(false)
  //    }
  //    checkAnswer(df,
  //      sql(
  //        "select employee_hive.empname,mobile_hive.mobilename from employee_hive,mobile_hive where" +
  //        " employee_hive.mobilename = mobile_hive.mobilename"))
  //  }

  test("test equijoin query for dictionary exclude column") {
    val df = sql(
      "select employee_dictexc.empname,mobile.mobilename from employee_dictexc,mobile where " +
      "employee_dictexc.mobilename = mobile.mobilename")
    var broadcastJoinExists = false
    df.queryExecution.sparkPlan.collect {
      case bcf: BroadCastFilterPushJoin =>
        broadcastJoinExists = true
    }
    if (!broadcastJoinExists) {
      assert(false)
    }
    sql(
      "select employee_hive.empname,mobile_hive.mobilename from employee_hive,mobile_hive where" +
      " employee_hive.mobilename = mobile_hive.mobilename").show(500)
    assert(true)
    //    checkAnswer(df,
    //      sql(
    //        "select employee_hive.empname,mobile_hive.mobilename from employee_hive,mobile_hive where" +
    //        " employee_hive.mobilename = mobile_hive.mobilename"))
  }
  test("test equijoin query for measure column") {
    val df = sql("select e.empname,m.empname from employee e join employee m on e.salary=m.salary")
    var broadcastJoinExists = false
    df.queryExecution.sparkPlan.collect {
      case bcf: BroadCastFilterPushJoin =>
        broadcastJoinExists = true
    }
    if (!broadcastJoinExists) {
      assert(false)
    }
    //    checkAnswer(df,
    //      sql(
    //        "select e.empname,m.empname from employee_hive e join employee_hive m on e.salary=m" +
    //        ".salary"))
    assert(true)
  }

  override def afterAll {
    sql("drop table if exists employee_dictexc")
    sql("drop table if exists bit_int2")
    sql("drop table if exists big_int23")
    sql("drop table employee_hive")
    sql("drop table mobile_hive")
    sql("drop table employee")
    sql("drop table mobile")
  }
}
