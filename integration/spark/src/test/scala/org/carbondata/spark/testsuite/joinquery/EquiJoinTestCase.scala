package org.carbondata.spark.testsuite.joinquery

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.execution.joins.BroadCastFilterPushJoin

class EquiJoinTestCase extends QueryTest with BeforeAndAfterAll  {
   override def beforeAll {
    sql("drop table if exists employee_hive")
    sql("drop table if exists mobile_hive")
    sql("drop table if exists employee")
    sql("drop table if exists mobile")
    //loading to hive table
    sql("create table employee_hive (empid string,empname string,mobilename string,mobilecolor string,salary int)row format delimited fields terminated by ','")
    sql("create table mobile_hive (mobileid string,mobilename string, mobilecolor string, sales int)row format delimited fields terminated by ','");
    sql("LOAD DATA LOCAL INPATH './src/test/resources/join/employee.csv' into table employee_hive")
    sql("LOAD DATA LOCAL INPATH './src/test/resources/join/mobile.csv' into table mobile_hive")
    //loading to carbon table
    sql("create table employee (empid string,empname string,mobilename string,mobilecolor string,salary int) stored by 'org.apache.carbondata.format'")
    sql("create table mobile (mobileid string,mobilename string, mobilecolor string, sales int) stored by 'org.apache.carbondata.format'");
    sql("LOAD DATA LOCAL INPATH './src/test/resources/join/employee.csv' into table employee options('FILEHEADER'='empid,empname,mobilename,mobilecolor,salary')")
    sql("LOAD DATA LOCAL INPATH './src/test/resources/join/mobile.csv' into table mobile options('FILEHEADER'='mobileid,mobilename,mobilecolor,sales')")
   }
   
   test("test equijoin query") {
     val df = sql("select employee.empname,mobile.mobilename from employee,mobile where employee.mobilename = mobile.mobilename")
     var broadcastJoinExists = false
     df.queryExecution.sparkPlan.collect {
       case bcf: BroadCastFilterPushJoin =>
         broadcastJoinExists = true
     }
     if (!broadcastJoinExists) {
       assert(false)
     }
      checkAnswer(df,
          sql("select employee_hive.empname,mobile_hive.mobilename from employee_hive,mobile_hive where employee_hive.mobilename = mobile_hive.mobilename"))
  }
  override def afterAll {
    sql("drop table employee_hive")
    sql("drop table mobile_hive")
    sql("drop table employee")
    sql("drop table mobile")
  }
}