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
package org.apache.carbondata.spark.testsuite.secondaryindex

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.secondaryindex.command.ErrorMessage
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * test cases with secondary index and agg queries
 */
class TestSecondaryIndexWithAggQueries extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    afterAll
  }

  test("test agg queries with secondary index") {
    sql("create table source (c1 string,c2 string,c3 string,c5 string) STORED AS CARBONDATA")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/secindex/dest.csv' INTO table source""")
   sql("create index index_source1 on table source (c2) AS 'carbondata'")
    checkAnswer(
      sql("select count(*) from source where c2='1' and c3 = 'aa' and c5 = 'aaa' "),
      Seq(Row(1))
    )
   sql("create index index_source2 on table source (c3) AS 'carbondata'")

    checkAnswer(
      sql("select count(*) from source where c2='zc' and c3 = 'gf' and c5 = 'fd' "),
      Seq(Row(0))
    )
       sql("create index index_source3 on table source (c5) AS 'carbondata'")
      checkAnswer(
        sql("select count(*) from source where c2='2' and c3 = 'bb' and c5 = 'bbb' "),
        Seq(Row(1))
      )
  }

  test("pushing down filter for broadcast join with badrecord") {
    sql("drop table if exists catalog_returns")
    sql("drop table if exists date_dim")
    sql("create table catalog_returns(cr_returned_date_sk int,cr_returned_time_sk int," +
        "cr_item_sk int,cr_refunded_customer_sk int,cr_refunded_cdemo_sk int," +
        "cr_refunded_hdemo_sk int,cr_refunded_addr_sk int,cr_returning_customer_sk int," +
        "cr_returning_cdemo_sk int,cr_returning_hdemo_sk int,cr_returning_addr_sk int," +
        "cr_call_center_sk int,cr_catalog_page_sk int,cr_ship_mode_sk int," +
        "cr_warehouse_sk int,cr_reason_sk int,cr_order_number int," +
        "cr_return_quantity int,cr_return_amount double,cr_return_tax double," +
        "cr_return_amt_inc_tax double,cr_fee double,cr_return_ship_cost double," +
        "cr_refunded_cash double,cr_reversed_charge double,cr_store_credit double," +
        "cr_net_loss double) STORED AS carbondata TBLPROPERTIES ( " +
        "'table_blocksize'='64')")
    sql(
      "create table date_dim ( d_date_sk int, d_date_id string, d_date date, d_month_seq int, " +
      "d_week_seq int, d_quarter_seq int, d_year int, d_dow  int, d_moy  int, d_dom  int, d_qoy  " +
      "int, d_fy_year int, d_fy_quarter_seq    int, d_fy_week_seq int, d_day_name  string, " +
      "d_quarter_name string, d_holiday string, d_weekend string, d_following_holiday string, " +
      "d_first_dom int, d_last_dom  int, d_same_day_ly int, d_same_day_lq int, d_current_day " +
      "string, d_current_week string, d_current_month string, d_current_quarter   string, " +
      "d_current_year string ) STORED AS carbondata  TBLPROPERTIES ( " +
      "'table_blocksize'='64')")
    sql(
      "insert into catalog_returns select 2450926,45816,9112,18601,79799,6189,57583,18601,797995," +
      "4703,57583,8,10,2,2,13,2,47,3888.31,23.29,4121.69,134.9,357.24,186.64,124.43,1673.42,22.24")
    sql(
      "insert into date_dim select 2424832,'AAAAAAAAAAAAFCAA','1926-11-12',322,1402,108,1926,5," +
      "11,12,4,1926,108,1402,'Friday','1926Q4','N','Y','N2424821',2425124,2424467,2424740,12,'N'," +
      "'N','N','N','2018'")
    checkAnswer(sql(
      "SELECT sum(cr_return_amount) AS returns, sum(cr_net_loss) AS profit_loss FROM " +
      "catalog_returns, date_dim WHERE cr_returned_date_sk = d_date_sk AND d_date BETWEEN cast" +
      "('2000-08-03]' AS DATE) AND (cast('2000-08-03' AS DATE) + INTERVAL 30 days)"),Seq(Row(null,null)))
  }

  test("pushing down filter for broadcast join with correct record") {
    sql("drop table if exists catalog_return")
    sql("drop table if exists date_dims")
    sql("create table catalog_return(cr_returned_date_sk int,cr_return_amount double," +
        "cr_net_loss double) STORED AS carbondata TBLPROPERTIES ( " +
        "'table_blocksize'='64')")
    sql(
      "create table date_dims ( d_date_sk int, c1 string,d_date int ) STORED AS carbondata" +
      "  TBLPROPERTIES ( 'table_blocksize'='64')")
    sql("insert into catalog_return select 2450,458.16,91.12")
    sql("insert into date_dims select 2450,'AAAAAAAAAAAAFCAA',5")
    checkAnswer(sql(
      "SELECT sum(cr_return_amount) AS returns, sum(cr_net_loss) AS profit_loss FROM " +
      "catalog_return, date_dims WHERE cr_returned_date_sk = d_date_sk AND d_date BETWEEN 1 AND " +
      "10"),
      Seq(Row(458.16, 91.12)))
  }

  test("test datamap on SI table") {
    sql("drop table if exists test_si_1")
    sql(
      "CREATE TABLE test_si_1 (id int,name string,salary float,dob date,address string) STORED AS " +
      "carbondata")
    sql("insert into test_si_1 select 1,'aa',23423.334,'2009-09-09','df'")
    sql("insert into test_si_1 select 2,'bb',4454.454,'2009-09-09','bang'")
    sql(
      "CREATE DATAMAP dm_test_si_11 ON TABLE test_si_1 USING 'bloomfilter' DMPROPERTIES " +
      "('INDEX_COLUMNS' = 'address', 'BLOOM_SIZE'='640000', 'BLOOM_FPP'='0.00001')")
    sql("create index si_test_si_1 on table test_si_1(address) AS 'carbondata'")
    val exceptionMessage = intercept[ErrorMessage] {
      sql(
        "CREATE DATAMAP dm_on_si ON TABLE si_test_si_1  USING 'bloomfilter' DMPROPERTIES " +
        "('INDEX_COLUMNS' = 'address', 'BLOOM_SIZE'='640000', 'BLOOM_FPP'='0.00001')")
    }.getMessage
    assert(exceptionMessage.contains("Datamap creation on Secondary Index table is not supported"))
  }

  test("test CTAS when use cast in select with SI table present on main table") {
    sql("drop table if exists cast_si")
    sql("drop index if exists index5 on cast_si")
    sql("create table if not exists cast_si (RECORD_ID bigint,CDR_ID string,LOCATION_CODE int,USER_NUM string) STORED AS carbondata " +
        "TBLPROPERTIES('table_blocksize'='256','SORT_SCOPE'='NO_SORT')")
    sql("create index index5 on table cast_si(USER_NUM) AS 'carbondata' tblproperties('table_blocksize' = '256')")
    sql("insert into cast_si select  1, 'gb3e5135-5533-4ee7-51b3-F61F1355b471', 2, '26557544541'")
    sql("create table ctas_cast select cast(location_code as string) as location_code from cast_si where ((user_num in ('26557544541')))")
    checkAnswer(sql("select count(*) from cast_si where ((user_num in ('26557544541')))"), sql("select count(*) from ctas_cast"))
  }

  test("test clean files for index for marked for delete segments") {
    sql("drop table if exists clean")
    sql("create table clean(name string, age int, add string) STORED AS carbondata")
    sql("create index clean_index on table clean(add) AS 'carbondata'")
    sql("insert into clean select 'ca',5,'de'")
    sql("insert into clean select 'ca',5,'de'")
    sql("delete from table clean where segment.id in (0)")
    val showSegments1 = sql("show segments for table clean").collect
    showSegments1.find(_.get(0).toString.contains("0")) match {
      case Some(row) => assert(row.get(1).toString.contains("Marked for Delete"))
      case None => assert(false)
    }
    val showSegments2 = sql("show segments for table clean_index").collect
    showSegments2.find(_.get(0).toString.contains("0")) match {
      case Some(row) => assert(row.get(1).toString.contains("Marked for Delete"))
      case None => assert(false)
    }
    sql("clean files for table clean")
    val mainTable = CarbonEnv.getCarbonTable(Some("default"), "clean")(sqlContext.sparkSession)
    val indexTable = CarbonEnv.getCarbonTable(Some("default"), "clean_index")(sqlContext.sparkSession)
    assert(!FileFactory.isFileExist(mainTable.getSegmentPath("0")))
    assert(!FileFactory.isFileExist(indexTable.getSegmentPath("0")))
    assert(FileFactory.isFileExist(mainTable.getSegmentPath("1")))
    assert(FileFactory.isFileExist(indexTable.getSegmentPath("1")))
  }

  override def afterAll: Unit = {
    sql("drop table if exists source")
    sql("drop table if exists catalog_return")
    sql("drop table if exists date_dims")
    sql("drop table if exists catalog_returns")
    sql("drop table if exists date_dim")
    sql("drop table if exists test_si_1")
    sql("drop table if exists test_pre_agg")
    sql("drop table if exists cast_si")
    sql("drop table if exists clean")
  }

}

