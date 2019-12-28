
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

import org.apache.spark.sql.common.util._
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for QueriesSparkBlockDistTestCase to verify all scenerios
 */

class QueriesSparkBlockDistTestCase extends QueryTest with BeforeAndAfterAll {
         

  //BlockDist_PTS001_TC002123
  test("BlockDist_PTS001_TC002123", Include) {
    sql("drop table if exists flow_carbon_256b")
    sql("drop table if exists flow_carbon_256b_hive")
    sql(s"""CREATE TABLE IF NOT EXISTS  flow_carbon_256b ( txn_dte     String, dt  String, txn_bk      String, txn_br      String, own_bk      String, own_br      String, opp_bk      String, bus_opr_cde String, opt_prd_cde String, cus_no      String, cus_ac      String, opp_ac_nme  String, opp_ac      String, bv_no       String, aco_ac      String, ac_dte      String, txn_cnt     int,     jrn_par     int,     mfm_jrn_no  String,     cbn_jrn_no  String,     ibs_jrn_no  String,     vch_no      String, vch_seq     String,     srv_cde     String, bus_cd_no   String, id_flg      String, bv_cde      String, txn_time    String, txn_tlr     String, ety_tlr     String, ety_bk      String, ety_br      String, bus_pss_no  String, chk_flg     String, chk_tlr     String, chk_jrn_no  String,     bus_sys_no  String, txn_sub_cde String, fin_bus_cde String, fin_bus_sub_cde     String, chl         String, tml_id      String, sus_no      String, sus_seq     String,     cho_seq     String,     itm_itm     String, itm_sub     String, itm_sss     String, dc_flg      String, amt         decimal(15,2), bal         decimal(15,2), ccy         String, spv_flg     String, vch_vld_dte String, pst_bk      String, pst_br      String, ec_flg      String, aco_tlr     String, gen_flg     String, his_rec_sum_flg     String, his_flg     String, vch_typ     String, val_dte     String, opp_ac_flg  String, cmb_flg     String, ass_vch_flg String, cus_pps_flg String, bus_rmk_cde String, vch_bus_rmk String, tec_rmk_cde String, vch_tec_rmk String, rsv_ara     String, gems_last_upd_d     String, gems_last_upd_d_bat String, maps_date   String, maps_job    String ) STORED BY 'org.apache.carbondata.format' """).collect

    sql(s"""CREATE TABLE IF NOT EXISTS  flow_carbon_256b_hive ( txn_dte     String, dt  String, txn_bk      String, txn_br      String, own_bk      String, own_br      String, opp_bk      String, bus_opr_cde String, opt_prd_cde String, cus_no      String, cus_ac      String, opp_ac_nme  String, opp_ac      String, bv_no       String, aco_ac      String, ac_dte      String, txn_cnt     int,     jrn_par     int,     mfm_jrn_no  String,     cbn_jrn_no  String,     ibs_jrn_no  String,     vch_no      String, vch_seq     String,     srv_cde     String, bus_cd_no   String, id_flg      String, bv_cde      String, txn_time    String, txn_tlr     String, ety_tlr     String, ety_bk      String, ety_br      String, bus_pss_no  String, chk_flg     String, chk_tlr     String, chk_jrn_no  String,     bus_sys_no  String, txn_sub_cde String, fin_bus_cde String, fin_bus_sub_cde     String, chl         String, tml_id      String, sus_no      String, sus_seq     String,     cho_seq     String,     itm_itm     String, itm_sub     String, itm_sss     String, dc_flg      String, amt         decimal(15,2), bal         decimal(15,2), ccy         String, spv_flg     String, vch_vld_dte String, pst_bk      String, pst_br      String, ec_flg      String, aco_tlr     String, gen_flg     String, his_rec_sum_flg     String, his_flg     String, vch_typ     String, val_dte     String, opp_ac_flg  String, cmb_flg     String, ass_vch_flg String, cus_pps_flg String, bus_rmk_cde String, vch_bus_rmk String, tec_rmk_cde String, vch_tec_rmk String, rsv_ara     String, gems_last_upd_d     String, gems_last_upd_d_bat String, maps_date   String, maps_job    String )  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'""").collect

    sql(s"""LOAD DATA  inpath '$resourcesPath/Data/cmb/data.csv' into table  flow_carbon_256b options('BAD_RECORDS_ACTION'='FORCE','DELIMITER'=',', 'QUOTECHAR'='"','FILEHEADER'='txn_dte,dt,txn_bk,txn_br,own_bk,own_br,opp_bk,bus_opr_cde,opt_prd_cde,cus_no,cus_ac,opp_ac_nme,opp_ac,bv_no,aco_ac,ac_dte,txn_cnt,jrn_par,mfm_jrn_no,cbn_jrn_no,ibs_jrn_no,vch_no,vch_seq,srv_cde,bus_cd_no,id_flg,bv_cde,txn_time,txn_tlr,ety_tlr,ety_bk,ety_br,bus_pss_no,chk_flg,chk_tlr,chk_jrn_no,bus_sys_no,txn_sub_cde,fin_bus_cde,fin_bus_sub_cde,chl,tml_id,sus_no,sus_seq,cho_seq,itm_itm,itm_sub,itm_sss,dc_flg,amt,bal,ccy,spv_flg,vch_vld_dte,pst_bk,pst_br,ec_flg,aco_tlr,gen_flg,his_rec_sum_flg,his_flg,vch_typ,val_dte,opp_ac_flg,cmb_flg,ass_vch_flg,cus_pps_flg,bus_rmk_cde,vch_bus_rmk,tec_rmk_cde,vch_tec_rmk,rsv_ara,gems_last_upd_d,gems_last_upd_d_bat,maps_date,maps_job')""").collect
    sql(s"""insert overwrite table flow_carbon_256b_hive select * from flow_carbon_256b""").collect

  }


  //BlockDist_PTS001_TC001
  test("BlockDist_PTS001_TC001", Include) {

    checkAnswer(s"""select * from flow_carbon_256b where txn_dte>='20140101' and txn_dte <= '20140601' and txn_bk ='00000000121' order by  txn_dte limit 1000""",
      s"""select * from flow_carbon_256b_hive where txn_dte>='20140101' and txn_dte <= '20140601' and txn_bk ='00000000121' order by  txn_dte limit 1000""", "QueriesSparkBlockDistTestCase_BlockDist_PTS001_TC001")

  }


  //BlockDist_PTS001_TC002
  test("BlockDist_PTS001_TC002", Include) {

    checkAnswer(s"""select * from flow_carbon_256b where own_br ='00000000515' and txn_dte>='20140101' and txn_dte <= '20150101' order by own_br limit 1000""",
      s"""select * from flow_carbon_256b_hive where own_br ='00000000515' and txn_dte>='20140101' and txn_dte <= '20150101' order by own_br limit 1000""", "QueriesSparkBlockDistTestCase_BlockDist_PTS001_TC002")

  }


  //BlockDist_PTS001_TC003
  test("BlockDist_PTS001_TC003", Include) {

    checkAnswer(s"""select * from flow_carbon_256b where opt_prd_cde ='2889' and txn_dte>='20140101' and txn_dte <= '20160101' order by opt_prd_cde limit 1000""",
      s"""select * from flow_carbon_256b_hive where opt_prd_cde ='2889' and txn_dte>='20140101' and txn_dte <= '20160101' order by opt_prd_cde limit 1000""", "QueriesSparkBlockDistTestCase_BlockDist_PTS001_TC003")

  }


  //BlockDist_PTS002_TC001
  test("BlockDist_PTS002_TC001", Include) {

    checkAnswer(s"""select  *  from flow_carbon_256b where  cus_ac like '%22262135060488208%' and (txn_dte>='20150101' and txn_dte<='20160101') and  txn_bk IN ('00000000215', '00000000025','00000000086') OR own_bk IN ('00000000001','01511999999','00000000180') order by cus_ac  limit 1000""",
      s"""select  *  from flow_carbon_256b_hive where  cus_ac like '%22262135060488208%' and (txn_dte>='20150101' and txn_dte<='20160101') and  txn_bk IN ('00000000215', '00000000025','00000000086') OR own_bk IN ('00000000001','01511999999','00000000180') order by cus_ac  limit 1000""", "QueriesSparkBlockDistTestCase_BlockDist_PTS002_TC001")

  }


  //BlockDist_PTS003_TC001
  test("BlockDist_PTS003_TC001", Include) {

    checkAnswer(s"""select own_br, count(opt_prd_cde)  from flow_carbon_256b group by own_br limit 1000""",
      s"""select own_br, count(opt_prd_cde)  from flow_carbon_256b_hive group by own_br limit 1000""", "QueriesSparkBlockDistTestCase_BlockDist_PTS003_TC001")

  }


  //BlockDist_PTS003_TC002
  test("BlockDist_PTS003_TC002", Include) {

    checkAnswer(s"""select  own_br, count(distinct opt_prd_cde)  from flow_carbon_256b where own_br like '6%' group by own_br limit 1000""",
      s"""select  own_br, count(distinct opt_prd_cde)  from flow_carbon_256b_hive where own_br like '6%' group by own_br limit 1000""", "QueriesSparkBlockDistTestCase_BlockDist_PTS003_TC002")

  }


  //BlockDist_PTS003_TC003
  test("BlockDist_PTS003_TC003", Include) {

    checkAnswer(s"""select  own_br, count(distinct opt_prd_cde)  from flow_carbon_256b group by own_br limit 1000""",
      s"""select  own_br, count(distinct opt_prd_cde)  from flow_carbon_256b_hive group by own_br limit 1000""", "QueriesSparkBlockDistTestCase_BlockDist_PTS003_TC003")

  }


  //BlockDist_PTS003_TC004
  test("BlockDist_PTS003_TC004", Include) {

    checkAnswer(s"""select own_br, count(1) as cn from flow_carbon_256b group by own_br having cn>1""",
      s"""select own_br, count(1) as cn from flow_carbon_256b_hive group by own_br having cn>1""", "QueriesSparkBlockDistTestCase_BlockDist_PTS003_TC004")

  }


  //BlockDist_PTS004_TC001
  test("BlockDist_PTS004_TC001", Include) {

    checkAnswer(s"""select  *  from flow_carbon_256b where  cus_ac  like '622262135067246539%'  and (txn_dte>='20150101' and txn_dte<='20160101') and txn_bk IN ('00000000000', '00000000001','00000000002') OR own_bk IN ('00000000424','00000001383','00000001942','00000001262') limit 1000""",
      s"""select  *  from flow_carbon_256b_hive where  cus_ac  like '622262135067246539%'  and (txn_dte>='20150101' and txn_dte<='20160101') and txn_bk IN ('00000000000', '00000000001','00000000002') OR own_bk IN ('00000000424','00000001383','00000001942','00000001262') limit 1000""", "QueriesSparkBlockDistTestCase_BlockDist_PTS004_TC001")

  }


  //BlockDist_PTS004_TC002
  test("BlockDist_PTS004_TC002", Include) {

    checkAnswer(s"""select own_br, sum(txn_cnt) as cn from flow_carbon_256b group by own_br having cn>1 limit 1000""",
      s"""select own_br, sum(txn_cnt) as cn from flow_carbon_256b_hive group by own_br having cn>1 limit 1000""", "QueriesSparkBlockDistTestCase_BlockDist_PTS004_TC002")

  }


  //BlockDist_PTS004_TC003
  test("BlockDist_PTS004_TC003", Include) {

    checkAnswer(s"""select  * from flow_carbon_256b where cus_ac = '6222621350672465397' and txn_bk IN ('00000000000', '00000000001','00000000002') OR own_bk IN ('00000000124','00000000175','00000000034','00000000231','00000000167','00000000182','00000000206') or opp_bk='1491999999107' and  (txn_dte>='20140101' and txn_dte<='20140630')  limit 1000""",
      s"""select  * from flow_carbon_256b_hive where cus_ac = '6222621350672465397' and txn_bk IN ('00000000000', '00000000001','00000000002') OR own_bk IN ('00000000124','00000000175','00000000034','00000000231','00000000167','00000000182','00000000206') or opp_bk='1491999999107' and  (txn_dte>='20140101' and txn_dte<='20140630')  limit 1000""", "QueriesSparkBlockDistTestCase_BlockDist_PTS004_TC003")

  }


  //BlockDist_PTS005_TC001
  test("BlockDist_PTS005_TC001", Include) {

    checkAnswer(s"""select  vch_seq, sum(amt)  from flow_carbon_256b group by vch_seq limit 1000""",
      s"""select  vch_seq, sum(amt)  from flow_carbon_256b_hive group by vch_seq limit 1000""", "QueriesSparkBlockDistTestCase_BlockDist_PTS005_TC001")

  }


  //BlockDist_PTS005_TC003
  test("BlockDist_PTS005_TC003", Include) {

    checkAnswer(s"""select  vch_seq, count(distinct cus_ac) * sum(amt) AS Total from flow_carbon_256b group by vch_seq limit 1000""",
      s"""select  vch_seq, count(distinct cus_ac) * sum(amt) AS Total from flow_carbon_256b_hive group by vch_seq limit 1000""", "QueriesSparkBlockDistTestCase_BlockDist_PTS005_TC003")

  }


  //BlockDist_PTS006_TC001
  test("BlockDist_PTS006_TC001", Include) {

    checkAnswer(s"""select  vch_seq, COALESCE(txn_cnt, jrn_par) Value from flow_carbon_256b group by vch_seq,txn_cnt,jrn_par limit 1000""",
      s"""select  vch_seq, COALESCE(txn_cnt, jrn_par) Value from flow_carbon_256b_hive group by vch_seq,txn_cnt,jrn_par limit 1000""", "QueriesSparkBlockDistTestCase_BlockDist_PTS006_TC001")

  }


  //BlockDist_PTS007_TC001
  test("BlockDist_PTS007_TC001", Include) {

    checkAnswer(s"""select * from flow_carbon_256b  where cus_no = '62226009239386397' and dt>='20140301' and dt<='20140330' order by amt desc limit 1000""",
      s"""select * from flow_carbon_256b_hive  where cus_no = '62226009239386397' and dt>='20140301' and dt<='20140330' order by amt desc limit 1000""", "QueriesSparkBlockDistTestCase_BlockDist_PTS007_TC001")

  }


  //BlockDist_PTS007_TC002
  test("BlockDist_PTS007_TC002", Include) {

    checkAnswer(s"""select cus_ac from flow_carbon_256b where jrn_par is not null order by cus_ac limit 1000""",
      s"""select cus_ac from flow_carbon_256b_hive where jrn_par is not null order by cus_ac limit 1000""", "QueriesSparkBlockDistTestCase_BlockDist_PTS007_TC002")

  }


  //BlockDist_PTS007_TC003
  test("BlockDist_PTS007_TC003", Include) {

    checkAnswer(s"""select cus_ac from flow_carbon_256b where jrn_par is  null order by cus_ac limit 1000""",
      s"""select cus_ac from flow_carbon_256b_hive where jrn_par is  null order by cus_ac limit 1000""", "QueriesSparkBlockDistTestCase_BlockDist_PTS007_TC003")

  }


  //BlockDist_PTS008_TC001
  test("BlockDist_PTS008_TC001", Include) {

    checkAnswer(s"""select txn_bk, MAX(distinct cus_ac) from flow_carbon_256b group by txn_bk, cus_ac""",
      s"""select txn_bk, MAX(distinct cus_ac) from flow_carbon_256b_hive group by txn_bk, cus_ac""", "QueriesSparkBlockDistTestCase_BlockDist_PTS008_TC001")

  }


  //BlockDist_PTS008_TC002
  test("BlockDist_PTS008_TC002", Include) {

    checkAnswer(s"""select txn_bk, count(distinct cus_ac) from flow_carbon_256b group by txn_bk, cus_ac""",
      s"""select txn_bk, count(distinct cus_ac) from flow_carbon_256b_hive group by txn_bk, cus_ac""", "QueriesSparkBlockDistTestCase_BlockDist_PTS008_TC002")

  }


  //BlockDist_PTS008_TC003
  test("BlockDist_PTS008_TC003", Include) {

    checkAnswer(s"""select distinct(txn_bk) AS TXN_BK, avg(cus_ac) from flow_carbon_256b group by txn_bk,cus_ac""",
      s"""select distinct(txn_bk) AS TXN_BK, avg(cus_ac) from flow_carbon_256b_hive group by txn_bk,cus_ac""", "QueriesSparkBlockDistTestCase_BlockDist_PTS008_TC003")

  }


  //BlockDist_PTS008_TC004
  test("BlockDist_PTS008_TC004", Include) {

    checkAnswer(s"""select txn_bk, LAST(cus_ac) from flow_carbon_256b group by txn_bk,cus_ac""",
      s"""select txn_bk, LAST(cus_ac) from flow_carbon_256b_hive group by txn_bk,cus_ac""", "QueriesSparkBlockDistTestCase_BlockDist_PTS008_TC004")

  }


  //BlockDist_PTS008_TC005
  test("BlockDist_PTS008_TC005", Include) {

    checkAnswer(s"""select txn_bk, FIRST(cus_ac) from flow_carbon_256b group by txn_bk,cus_ac""",
      s"""select txn_bk, FIRST(cus_ac) from flow_carbon_256b_hive group by txn_bk,cus_ac""", "QueriesSparkBlockDistTestCase_BlockDist_PTS008_TC005")

  }


  //BlockDist_PTS009_TC001
  test("BlockDist_PTS009_TC001", Include) {

    checkAnswer(s"""select txn_bk, percentile_approx(cast(txn_cnt as double) ,0.2) from flow_carbon_256b group by txn_bk,cus_ac""",
      s"""select txn_bk, percentile_approx(cast(txn_cnt as double) ,0.2) from flow_carbon_256b_hive group by txn_bk,cus_ac""", "QueriesSparkBlockDistTestCase_BlockDist_PTS009_TC001")

  }


  //BlockDist_PTS009_TC002
  test("BlockDist_PTS009_TC002", Include) {

    checkAnswer(s"""select txn_bk, collect_set(cus_ac) from flow_carbon_256b group by txn_bk,cus_ac""",
      s"""select txn_bk, collect_set(cus_ac) from flow_carbon_256b_hive group by txn_bk,cus_ac""", "QueriesSparkBlockDistTestCase_BlockDist_PTS009_TC002")

  }


  //BlockDist_PTS009_TC003
  test("BlockDist_PTS009_TC003", Include) {

    checkAnswer(s"""select txn_bk, variance(cus_ac) from flow_carbon_256b group by txn_bk,cus_ac limit 1000""",
      s"""select txn_bk, variance(cus_ac) from flow_carbon_256b_hive group by txn_bk,cus_ac limit 1000""", "QueriesSparkBlockDistTestCase_BlockDist_PTS009_TC003")

  }


  //BlockDist_PTS010_TC001
  test("BlockDist_PTS010_TC001", Include) {

    checkAnswer(s"""select txn_bk, (txn_cnt + jrn_par) AS Result from flow_carbon_256b group by txn_bk,txn_cnt,jrn_par limit 1000""",
      s"""select txn_bk, (txn_cnt + jrn_par) AS Result from flow_carbon_256b_hive group by txn_bk,txn_cnt,jrn_par limit 1000""", "QueriesSparkBlockDistTestCase_BlockDist_PTS010_TC001")
  }

  override def afterAll {
    sql("drop table if exists flow_carbon_256b")
    sql("drop table if exists flow_carbon_256b_hive")
  }
}