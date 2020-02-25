package org.apache.carbondata.spark.testsuite.secondaryindex

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, Ignore}

@Ignore
class TestSIWithPartition extends QueryTest with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    sql("drop table if exists uniqdata1")
    sql(
      "CREATE TABLE uniqdata1 (CUST_ID INT,CUST_NAME STRING,DOB timestamp,DOJ timestamp," +
      "BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 DECIMAL(30, 10)," +
      "DECIMAL_COLUMN2 DECIMAL(36, 10),Double_COLUMN1 double, Double_COLUMN2 double," +
      "INTEGER_COLUMN1 int) PARTITIONED BY(ACTIVE_EMUI_VERSION string) STORED AS carbondata " +
      "TBLPROPERTIES('TABLE_BLOCKSIZE'='256 MB')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data_2000.csv' INTO " +
        "TABLE uniqdata1 partition(ACTIVE_EMUI_VERSION='abc') OPTIONS('DELIMITER'=',', " +
        "'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID," +
        "CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1," +
        "DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data_2000.csv' INTO " +
        "TABLE uniqdata1 partition(ACTIVE_EMUI_VERSION='abc') OPTIONS('DELIMITER'=',', " +
        "'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID," +
        "CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1," +
        "DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data_2000.csv' INTO " +
        "TABLE uniqdata1 partition(ACTIVE_EMUI_VERSION='abc') OPTIONS('DELIMITER'=',', " +
        "'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID," +
        "CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1," +
        "DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data_2000.csv' INTO " +
        "TABLE uniqdata1 partition(ACTIVE_EMUI_VERSION='abc') OPTIONS('DELIMITER'=',', " +
        "'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID," +
        "CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1," +
        "DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
  }

  test("TESTING SI on partition column") {
    intercept[UnsupportedOperationException] {
      sql("create index indextable1 on table uniqdata1 (ACTIVE_EMUI_VERSION) AS 'carbondata'")
    }
  }

  test("TESTING SI on normal column") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    sql("select * from uniqdata1 where ni(CUST_NAME='CUST_NAME_00108')").show()
  }

  test("TESTING SI on normal  + partition column") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    sql("select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = 'abc'")
      .show()
  }


  test("TESTING SI on MAJOR compaction") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    sql("alter table uniqdata1 compact 'minor'")
    sql("select * from uniqdata1 where ACTIVE_EMUI_VERSION = 'abc' and " +
        "CUST_NAME='CUST_NAME_00108'").show()
    sql("drop index if exists indextable1 on uniqdata1")
  }

  test("TESTING SI on MINOR compaction") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data_2000.csv' INTO " +
        "TABLE uniqdata1 partition(ACTIVE_EMUI_VERSION='abc') OPTIONS('DELIMITER'=',', " +
        "'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID," +
        "CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1," +
        "DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    sql("alter table uniqdata1 compact 'minor'")
    sql("select * from uniqdata1 where CUST_NAME='CUST_NAME_00108'").show()

    sql("drop index if exists indextable1 on uniqdata1")
    sql("drop table if exists uniqdata1")
  }

  override protected def afterAll(): Unit = {

  }
}
