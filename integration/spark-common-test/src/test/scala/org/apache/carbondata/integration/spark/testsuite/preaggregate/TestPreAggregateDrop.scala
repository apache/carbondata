package org.apache.carbondata.integration.spark.testsuite.preaggregate

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestPreAggregateDrop extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists maintable")
    sql("create table maintable (a string, b string, c string) stored by 'carbondata'")
  }

  test("create and drop preaggregate table") {
    sql(
      "create table preagg1 stored BY 'carbondata' tblproperties('parent'='maintable') as select" +
      " a,sum(b) from maintable group by a")
    sql("drop table if exists preagg1")
    checkExistence(sql("show tables"), false, "preagg1")
  }

  test("dropping 1 aggregate table should not drop others") {
    sql(
      "create table preagg1 stored BY 'carbondata' tblproperties('parent'='maintable') as select" +
      " a,sum(b) from maintable group by a")
    sql(
      "create table preagg2 stored BY 'carbondata' tblproperties('parent'='maintable') as select" +
      " a,sum(c) from maintable group by a")
    sql("drop table if exists preagg2")
    checkExistence(sql("show tables"), false, "preagg2")
  }
  
  test("drop main table and check if preaggreagte is deleted") {
    sql(
      "create table preagg2 stored BY 'carbondata' tblproperties('parent'='maintable') as select" +
      " a,sum(c) from maintable group by a")
    sql("drop table if exists maintable")
    checkExistence(sql("show tables"), false, "preagg1", "maintable", "preagg2")
  }

  override def afterAll() {
    sql("drop table if exists maintable")
  }
  
}
