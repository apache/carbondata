package org.apache.carbondata.spark.testsuite.filterexpr

import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties


class TestInExpression extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists uniqdata")
    sql("drop table if exists uniqdataHive")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
    val csvFilePath = s"$resourcesPath/filter/inExpression.csv"

    sql(
      """
           CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB
           timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1
           decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2
           double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES
           ("TABLE_BLOCKSIZE"= "256 MB")
      """)

    sql(
      s"""
          LOAD DATA LOCAL INPATH '$csvFilePath' into table uniqdata OPTIONS('DELIMITER'=',' ,'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""")

    sql(
      """
           CREATE TABLE uniqdataHive (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,
           DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,
           DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double,
           Double_COLUMN2 double,INTEGER_COLUMN1 int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
      """)
    sql(
      s"""
          LOAD DATA LOCAL INPATH '$csvFilePath' into table uniqdataHive
           """)
  }

  test(
    "select CUST_ID,CUST_NAME,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1,INTEGER_COLUMN1 " +
    "from uniqdata where CUST_ID in ('10020','10030','10032','10035','10040','10060','',NULL,' ')" +
    " or INTEGER_COLUMN1 in (1021,1031,1032,1033,'',NULL,' ') or DECIMAL_COLUMN1 in ('12345679921" +
    ".1234000000','12345679931.1234000000','12345679936.1234000000','',NULL,' ')") {
    checkAnswer(
      sql(
        "select CUST_ID,CUST_NAME,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1," +
        "INTEGER_COLUMN1 from uniqdata where CUST_ID in ('10020','10030','10032','10035','10040'," +
        "'10060','',NULL,' ') or INTEGER_COLUMN1 in (1021,1031,1032,1033,'',NULL,' ') or " +
        "DECIMAL_COLUMN1 in ('12345679921.1234000000','12345679931.1234000000','12345679936" +
        ".1234000000','',NULL,' ')"),
      sql(
        "select CUST_ID,CUST_NAME,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1," +
        "INTEGER_COLUMN1 from uniqdataHive where CUST_ID in ('10020','10030','10032','10035'," +
        "'10040'," +
        "'10060','',NULL,' ') or INTEGER_COLUMN1 in (1021,1031,1032,1033,'',NULL,' ') or " +
        "DECIMAL_COLUMN1 in ('12345679921.1234000000','12345679931.1234000000','12345679936" +
        ".1234000000','',NULL,' ')"))
  }

  override def afterAll {
    sql("drop table if exists uniqdata")
    sql("drop table if exists uniqdataHive")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }
}
