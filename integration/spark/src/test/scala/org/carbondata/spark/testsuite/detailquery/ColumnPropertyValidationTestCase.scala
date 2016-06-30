package org.carbondata.spark.testsuite.detailquery;

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.Row

class ColumnPropertyValidationTestCase extends QueryTest with BeforeAndAfterAll {
  test("Validate ColumnProperties_ valid key") {
     try {
       sql("create table employee(empname String,empid String,city String,country String,gender String,salary Double) stored by 'org.apache.carbondata.format' tblproperties('columnproperties.gender.key'='value')")
       assert(true)
       sql("drop table employee")
     } catch {
       case e =>assert(false)
     }
  }
  test("Validate Dictionary include _ invalid key") {
     try {
       sql("create table employee(empname String,empid String,city String,country String,gender String,salary Double) stored by 'org.apache.carbondata.format' tblproperties('columnproperties.invalid.key'='value')")
       assert(false)
       sql("drop table employee")
     } catch {
       case e =>assert(true)
     }
  }
  
}
