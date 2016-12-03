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

package org.apache.spark.carbondata

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.carbondata.core.constants.CarbonCommonConstants

// scalastyle:off
class SJSQuerySuite extends FunSuite with BeforeAndAfterAll {

  var spark: SparkSession = null

  override def beforeAll(): Unit = {
    spark = SparkSession
    .builder()
    .master("local[4]")
    .appName("CarbonExample")
    .enableHiveSupport()
    .config(CarbonCommonConstants.STORE_LOCATION,
    s"/user/hive/warehouse/store")
    .getOrCreate()


    CarbonEnv.init(spark.sqlContext)
    CarbonEnv.get.carbonMetastore.cleanStore()

//    spark.sql("set spark.sql.crossJoin.enabled = true")

    spark.sql("drop table if exists  dwcjk")

    spark.sql("drop table if exists  dwwtk")

    spark.sql("drop table if exists  dwzqxx")

    spark.sql("drop table if exists  swtnialk")

    spark.sql("drop table if exists  tsquotat")

    spark.sql("drop table if exists  wwtnbrch")

    spark.sql("drop table if exists  wwtnishc")

    spark.sql("drop table if exists wwtnmmbr")

    spark.sql("drop table if exists  wwtnseat")

    spark.sql("drop table if exists  SWTNBCSA")

    spark.sql(
      """
        | create table dwcjk(
        | CJRQ STRING,--成交日期
        | CJXH CHAR (10),--成交序号
        | ZQDH CHAR (6),--证券代号
        | BXWDH CHAR (6),--买方席位
        | BGDDM CHAR (10) ,--买方股东
        | BHTXH CHAR (10) ,--买方合同序号
        | BRZRQ CHAR (1),--买方融资融券
        | BPCBZ CHAR (1),--买方平仓标志
        | SXWDH CHAR (6),--卖方席位
        | SGDDM CHAR (10) ,--卖方股东
        | SHTXH CHAR (10) ,--卖方合同序号
        | SRZRQ CHAR (1) ,--卖方融资融券
        | SPCBZ CHAR (1) ,--卖方平仓标志
        | CJGS DECIMAL (9, 0) ,--成交数量
        | CJJG DECIMAL (9, 3) ,--成交价格
        | CJSJ DECIMAL (8, 0) ,--成交时间
        | YWLB CHAR (1) ,--业务类别
        | MMLB CHAR (1) ,--买卖类别
        | EBBZ CHAR (1) ,--二版标志
        | FILLER CHAR (1)  --保留字段
        |)
      """.stripMargin)

    spark.sql(
      """
        |create table dwwtk(
        | JLHM DECIMAL (10,0) ,--记录号码
        | WTRQ STRING ,--委托日期
        | WTSJ DECIMAL (8, 0) ,--委托时间
        | ZQDH CHAR (6) ,--证券代号
        | XWDH CHAR (6) ,--席位代号
        | GDDM CHAR (10) ,--股东代码
        | HTXH CHAR (10) ,--合同序号
        | WTSL DECIMAL (9, 0) ,--委托数量
        | WTJG DECIMAL (9, 3) ,--委托价格
        | RZRQ CHAR (1) ,--融资融券
        | PCBZ CHAR (1) ,--平仓标志
        | DFXW CHAR (6) ,--对方席位
        | DFGD CHAR (10) ,--对方股东
        | WTSL2 DECIMAL (9, 0) ,--委托数量2
        | WTJG2 DECIMAL (9, 3) ,--委托价格2
        | MMLB CHAR (1) ,--买卖类别
        | CXQQ CHAR (1) ,--撤消请求
        | WTLB CHAR (1) ,--委托类别
        | EBBZ CHAR (1) ,--二板标志
        | FILLER CHAR (1)  --保留字段
        |)
      """.stripMargin)

    spark.sql(
      """
        |create table dwzqxx (
        | ZQDH CHAR (6) , --证券代码
        | ZQJC CHAR (14) , --证券简称
        | YWJC CHAR (20) , --英文简称
        | ZYDW DECIMAL (4, 0) , --交易单位
        | GPLB CHAR (2) , --股票类别
        | HYLB CHAR (3) , --行业类别
        | CQCX CHAR (1) , --除权除息标志
        | HBZL CHAR (2) , --货币种类
        | MGMZ DECIMAL (9, 3) , --每股面值
        | ZFXL DECIMAL (18, 0) , --总发行量
        | KLTG DECIMAL (18, 0) , --可流通股
        | SNLR DECIMAL (9, 4) , --上年每股利润
        | BNLR DECIMAL (9, 4) , --本年每股利润
        | JSFL DECIMAL (9, 6) , --经手费率
        | YHSL DECIMAL (9, 6) , --印花税率
        | GHFL DECIMAL (9, 6) , --过户费率
        | SSRQ DECIMAL (8, 0) , --上市日期
        | DQRQ DECIMAL (8, 0) , --到期日/交割日
        | MBXL DECIMAL (9, 0) , --每笔限量
        | DW DECIMAL (9, 3) , --档位
        | FDCF CHAR (6) , --分档存放
        | LJTP DECIMAL (4, 0) , --累计停牌日
        | TPBZ CHAR (1) , --停牌标志
        | ZSBZ CHAR (1) , --纳入指数计算
        | ZHYDRQ DECIMAL (8, 0) , --最后异动日期
        | JYZT CHAR (1) , --交易状态
        | FHFS CHAR (1) , --发行方式
        | GPJB CHAR (1) , --股票级别
        | EBBZ CHAR (1) , --板块标志
        | CHFS CHAR (1) , --撮合方式
        | JHFW DECIMAL (9, 3) , --集合竞价范围
        | LXFW DECIMAL (9, 3) , --连续竞价范围
        | SZFD DECIMAL (9, 5) , --上涨幅度
        | XDFD DECIMAL (9, 5) , --下跌幅度
        | SZXJ DECIMAL (9, 3) , --上涨限价
        | XDXJ DECIMAL (9, 3) , --下跌限价
        | ZTJG DECIMAL (9, 3) , --涨停价
        | DTJG DECIMAL (9, 3) , --跌停价
        | CLLB DECIMAL (4, 0) , --处理类别
        | ZHBL DECIMAL (4, 2) , --折合比例
        | JLH DECIMAL (4, 0) , --记录号
        | DBQSDM CHAR (6) , --代办券商代码
        | KSZSJS CHAR (1) , --开市指数计算标志
        | ZRFS CHAR (1) , --转让方式
        | ZQZLBZ CHAR (2) , --证券种类标识
        | BSLDW DECIMAL (9, 0) , --买数量单位
        | SSLDW DECIMAL (9, 0) , --卖数量单位
        | SPFS CHAR (1) , --收盘方式
        | TNJYR CHAR (1) , --T+n交易
        | XYSSQR CHAR (1) , --协议实时确认
        | BZ CHAR (10) --备注字段
        |)
      """.stripMargin)

    spark.sql(
      """
        |create table swtnialk(
        | INV_CD CHAR (10)  , --股东代码
        | INV_NM VARCHAR (128), --股东名称
        | INV_KIND CHAR (1) , --投资者种类
        | INV_TYP CHAR (2) , --投资者类别
        | ACCT_KIND CHAR (2) , --帐户类别
        | ACCT_TYP CHAR (1) , --帐户分类
        | IDENT_NUM VARCHAR (64) , --证件号码
        | IDENT_TYP CHAR (1) , --证件类别
        | CNTRY_CD CHAR (3) , --国家代码
        | PRV_CD CHAR (6) , --省代码
        | CITY_CD CHAR (6) , --市代码
        | STPRV_CD CHAR (6) , --统计省市代码
        | SEX_FLG CHAR (1) , --股东性别
        | BORN_DT STRING , --出生日期
        | EDBG_TYP CHAR (2) , --学历类型
        | PROF_TYP CHAR (2) , --职业性质
        | OPEN_DT STRING , --开户日期
        | CNCL_FLG CHAR (1) , --注销标志
        | CNCL_DT STRING , --注销日期
        | INV_STS CHAR (1) , --账户状态
        | STATE_FLG CHAR (1) , --国有属性
        | LIST_FLG CHAR (1) , --上市属性
        | CAP_TYP CHAR (1) , --资本属性
        | UIDENT_NUM VARCHAR (64) --统一证件号码
        |)
      """.stripMargin)

    spark.sql(
      """
        |create table tsquotat(
        | TRD_DT STRING , --成交日期
        | TANDEM_TM DECIMAL (6, 0) , --交易主机时间
        | AS400_TM DECIMAL (6, 0) , --监察主机时间
        | ADJUST_TM DECIMAL (6, 0) , --调整刷新时间
        | SEC_CD CHAR (6) , --证券代号
        | LCLOS_PRC DECIMAL (11, 4) , --昨日收盘
        | OPEN_PRC DECIMAL (11, 4) , --今日开盘
        | TRD_VOL DECIMAL (13, 0) , --成交数量
        | TRD_TRNOVR DECIMAL (17, 3) , --成交金额
        | DEAL_NUM DECIMAL (13, 0) , --总成交笔数
        | HMTCH_PRC DECIMAL (11, 4) , --最高成交
        | LMTCH_PRC DECIMAL (11, 4) , --最低成交
        | LAST_PRC DECIMAL (11, 4) , --最近成交
        | DSELL_PRC DECIMAL (9, 3) , --最高申报
        | DBUY_PRC DECIMAL (9, 3) , --最低申报
        | PE_RT1 DECIMAL (9, 2) , --市盈率1
        | PE_RT2 DECIMAL (9, 2) , --市盈率2
        | FLUTUATE_1 DECIMAL (9, 3) , --升跌1
        | FLUTUATE_2 DECIMAL (9, 3) , --升跌2
        | HLD_QTY DECIMAL (13, 0) , --合约持仓量
        | BUY_PRC1 DECIMAL (9, 3) , --买价位1
        | BUY_QTY1 DECIMAL (13, 0) , --买数量1
        | BUY_PRC2 DECIMAL (9, 3) , --买价位2
        | BUY_QTY2 DECIMAL (13, 0) , --买数量2
        | BUY_PRC3 DECIMAL (9, 3) , --买价位3
        | BUY_QTY3 DECIMAL (13, 0) , --买数量3
        | BUY_PRC4 DECIMAL (9, 3) , --买价位4
        | BUY_QTY4 DECIMAL (13, 0) , --买数量4
        | BUY_PRC5 DECIMAL (9, 3) , --买价位5
        | BUY_QTY5 DECIMAL (13, 0) , --买数量5
        | BUY_PRC6 DECIMAL (9, 3) , --买价位6
        | BUY_QTY6 DECIMAL (13, 0) , --买数量6
        | BUY_PRC7 DECIMAL (9, 3) , --买价位7
        | BUY_QTY7 DECIMAL (13, 0) , --买数量7
        | BUY_PRC8 DECIMAL (9, 3) , --买价位8
        | BUY_QTY8 DECIMAL (13, 0) , --买数量8
        | SELL_PRC1 DECIMAL (9, 3) , --卖价位1
        | SELL_QTY1 DECIMAL (13, 0) , --卖数量1
        | SELL_PRC2 DECIMAL (9, 3) , --卖价位2
        | SELL_QTY2 DECIMAL (13, 0) , --卖数量3
        | SELL_PRC3 DECIMAL (9, 3) , --卖价位3
        | SELL_QTY3 DECIMAL (13, 0) , --卖数量3
        | SELL_PRC4 DECIMAL (9, 3) , --卖价位4
        | SELL_QTY4 DECIMAL (13, 0) , --卖数量4
        | SELL_PRC5 DECIMAL (9, 3) , --卖价位5
        | SELL_QTY5 DECIMAL (13, 0) , --卖数量5
        | SELL_PRC6 DECIMAL (9, 3) , --卖价位6
        | SELL_QTY6 DECIMAL (13, 0) , --卖数量6
        | SELL_PRC7 DECIMAL (9, 3) , --卖价位7
        | SELL_QTY7 DECIMAL (13, 0) , --卖数量7
        | SELL_PRC8 DECIMAL (9, 3) , --卖价位8
        | SELL_QTY8 DECIMAL (13, 0) , --卖数量8
        | PKG_TYP CHAR (10) --行情包类别
        |)
      """.stripMargin)

    spark.sql(
      """
        |create table wwtnbrch(
        | BRCH_CD CHAR (6)  , --营业部代码
        | BRCH_FDT STRING , --营业部起始日期
        | BRCH_EDT STRING , --营业部终止日期
        | BRCH_NM VARCHAR (200), --营业部名称
        | BRCH_SNM VARCHAR (128), --营业部简称
        | BRCH_TYP CHAR (1) , --营业部类别代码
        | BRCHTYP_NM VARCHAR (32) , --营业部类别名称
        | ISSPEC_FLG CHAR (1) , --是否特殊营业部
        | MBR_CD CHAR (6) , --会员代码
        | CITY_CD CHAR (6) , --市代码
        | PRV_CD CHAR (6) , --省代码
        | CNTRY_CD CHAR (3) , --国家代码
        | OPEN_DT STRING , --开业日期
        | BRCH_ZIP VARCHAR (22) , --邮政编码
        | MAIL_ADDR VARCHAR (108), --通信地址
        | REG_ADDR VARCHAR (150), --注册地址
        | CNCT_PHN VARCHAR (50) , --咨询电话
        | COMBRCH_CD CHAR (8) , --证监会营业部代码
        | BRCHID_CD CHAR (2) --营业部识别码
        |)
      """.stripMargin)

    spark.sql(
      """
        |create table wwtnishc(
        | SEC_CD CHAR (6) , --证券代码
        | SEAT_CD CHAR (6) , --席位代码
        | INV_CD CHAR (10) , --股东代码
        | SHRNAT_CD CHAR (2) , --股份性质代码
        | REC_FDT STRING , --记录起始日期
        | REC_EDT STRING , --记录终止日期
        | SHR_QTY DECIMAL (13, 0), --持有数量
        | CHG_QTY DECIMAL (13, 0) --变更数量
        |)
      """.stripMargin)

    spark.sql(
      """
        |create table wwtnmmbr(
        | MBR_CD CHAR (6)  , --会员代码
        | MBR_FDT STRING  , --会员起始日期
        | MBR_EDT STRING , --会员终止日期
        | MBR_NM VARCHAR (100) , --会员名称
        | MBR_SNM VARCHAR (32) , --会员简称
        | SPELL_ABBR VARCHAR (32) , --拼音缩写
        | MBR_ENM VARCHAR (100) , --会员英文名称
        | MBR_TYP CHAR (2) , --会员类别代码
        | MBRTYP_NM VARCHAR (64) , --会员类别名称
        | MBR_STS CHAR (1) , --会籍状态
        | VRTMBR_FLG CHAR (1) , --虚拟会员标志
        | COMMBR_CD CHAR (8) , --证监会会员代码
        | LIST_FLG CHAR (1) , --是否上市公司
        | SEC_CD CHAR (6) , --证券代码
        | LIST_LOC VARCHAR (20) , --上市地点
        | CITY_CD CHAR (6) , --市代码
        | PRV_CD CHAR (6) , --省代码
        | CNTRY_CD CHAR (3) , --国家代码
        | SUPV_CD CHAR (3) , --证监局编码
        | JOIN_DT STRING , --入会日期
        | OPEN_DT STRING , --开业日期
        | APP_DT STRING , --申请日期
        | SETUP_DT STRING , --设立日期
        | LIC_NUM VARCHAR (16) , --营业执照号码
        | REGCAP_AMT DECIMAL (20, 4), --注册资本
        | REG_ADDR VARCHAR (150) , --注册地址
        | MBR_ZIP VARCHAR (22) , --邮政编码
        | MAIL_ADDR VARCHAR (108) , --通信地址
        | MBR_PHN VARCHAR (25) , --会员电话
        | MBR_FAX VARCHAR (50) , --会员传真
        | MBR_EML VARCHAR (64) , --电子邮件
        | MBR_URL VARCHAR (40) , --网页地址
        | LGLPRN_NM VARCHAR (64) , --法定代表人
        | LGLPRN_PHN VARCHAR (25) , --法人联系电话
        | CNTCT_NM VARCHAR (64) , --联络人
        | CNTCT_ZIP VARCHAR (22) , --联络人邮编
        | CNTCT_ADDR VARCHAR (108) , --联络人地址
        | CNTCT_PHN VARCHAR (25) , --联络人电话
        | CNTCT_FAX VARCHAR (50) , --联络人传真
        | CNTCT_EML VARCHAR (64) --联络人电子邮件
        |)
      """.stripMargin)

    spark.sql(
      """
        |create table wwtnseat(
        | SEAT_CD CHAR (6)  , --席位代码
        | SEAT_FDT STRING  , --席位起始日期
        | SEAT_EDT STRING , --席位终止日期
        | SEAT_NM VARCHAR (128), --席位名称
        | SEAT_SNM VARCHAR (128), --席位简称
        | SEAT_TYP CHAR (2) , --席位类别
        | MBR_CD CHAR (6) , --会员代码
        | PBRCH_CD CHAR (6) , --主营业部代码
        | CSHSEAT_CD CHAR (6) , --资金结算席位代码
        | CLRSEAT_CD CHAR (6) , --股份结算席位代码
        | APP_DT STRING , --申请日期
        | INIT_DT STRING , --首次开通日期
        | SEAT_STS CHAR (1) , --席位状态
        | ISPEC_FLG CHAR (1) , --机构专用标志
        | ISPEC_TYP CHAR (1) , --机构专用类别
        | --ISPEC_DESC VARCHAR (16) , --机构专用描述
        | UMBR_CD CHAR (6) --使用方会员代码
        |)
      """.stripMargin)

    spark.sql(
      """
        |create table SWTNBCSA(
        | INV_CD CHAR(10) , --股东代码
        | CSEAT_CD CHAR(6) , --股份结算单元代码
        | BRCH_CD CHAR(6) , --营业部代码
        | MBR_CD CHAR(6), --会员代码
        | SRC_FLG CHAR(1), --信息来源标志
        | REC_FDT STRING --记录起始日期
        |)
      """.stripMargin)

  }

  test("4.2.2") {
    spark.sql(
      """
        |select  a.zqdh,                  --证券代号
        |        zqjc,                    --证券简称
        |        a.ybdm,                  --买卖合同序号
        |        brch_nm,                 --营业部名称
        |        brchid_cd,               --营业部代码
        |        a.jydy,                  --买卖席位代码
        |        a.gddm,                  --股东代码
        |        inv_nm,                  --股东名称
        |        a.mmlb,                  --买卖类别
        |        cjgs,                    --成交总量
        |        a.cjje,                  --成交总额
        |        cjjj                     --成交均价
        |  from(
        |   select *
        |     from (select  zqdh,            --证券代号
        |                   bhtxh as ybdm,   --买方合同序号
        |                   bxwdh as jydy,   --买方席位代号
        |                   bgddm as gddm,   --买方股东代码
        |                   mmlb,            --买卖类别
        |                   sum(cjgs) as  cjgs,    --成交数量
        |                   sum(cjgs*cjjg) as cjje,--sum(成交数量*成交价格)=买入总额
        |                   case when sum(cjgs)=0 then 0 else sum(cjgs*cjjg)/sum(cjgs) end as cjjj --买入均价
        |             from dwcjk             --成交库
        |            where     cjsj>=1   --cjsj成交时间 > 开始时间
        |                  and cjsj<=2  --cjsj成交时间 < 结束时间
        |                  and mmlb<>'C'     --买方类别不为'C'
        |                  and cjrq>="2015-04-30 00:00:00" --cjrq成交日期 > 开始日期
        |                  and cjrq<="2015-05-30 00:00:00" --cjrq成交日期 < 结束日期
        |            group by zqdh, bxwdh, bgddm, bhtxh, mmlb  --按查出的结果汇总成交量
        |            order by cjgs desc     --按成交数据从大到小排列
        |            limit 10000) cjk1
        |       union all
        |   select *
        |     from (select zqdh,             --证券代号
        |                  shtxh as ybdm,    --卖方合同序号
        |                  sxwdh as jydy,    --卖方席位代号
        |                  sgddm as gddm,    --卖方股东代码
        |                  mmlb,             --买卖类别
        |                  sum(cjgs)as cjgs, --成交数量
        |                  sum(cjgs*cjjg) as cjje,  --sum(成交数量*成交价格)=卖出总额
        |                  case when sum(cjgs)=0 then 0 else -sum(cjgs*cjjg)/sum(cjgs)end as cjjj --卖出均价
        |             from dwcjk           --成交库
        |            where     cjsj>=1
        |                  and cjsj<=2
        |                  and mmlb<>'C'
        |                  and cjrq>="2015-04-30 00:00:00"
        |                  and cjrq<="2015-05-30 00:00:00"
        |            group by zqdh, sxwdh, sgddm, shtxh, mmlb
        |            order by cjgs desc
        |            limit 10000) cjk2
        |     ) a
        |    left join dwzqxx c on a.zqdh=c.zqdh  --证券信息
        |    left join wwtnbrch on a.ybdm=brch_cd --营业部资料
        |    left join swtnialk on a.gddm=inv_cd --买卖股东信息
      """.stripMargin).collect()
  }

  test("4.2.3") {
    spark.sql(
      """
        |select  a.zqdh,     --证券代号
        |        a.bgddm,    --买方股东代码
        |        min(a.cjxh) as cjxh,   --成交序号
        |        sum(a.cjgs) as cjgs,   --成交数量
        |        b.inv_cd,              --股东代码
        |        xw.seat_nm,            --席位代号
        |        zh.inv_nm,             --投资者名称
        |        zh.inv_kind            --投资者种类
        |  from dwcjk a
        |       join wwtnishc b on a.zqdh = b.sec_cd --二级股份持有变更表
        |       and a.bgddm=b.inv_cd
        |       and a.cjrq between b.rec_fdt and b.rec_edt
        |       join dwzqxx c on b.sec_cd =c.zqdh    --证券信息库 ON 证券代码
        |       left join wwtnseat xw on a.BXWDH = xw.seat_cd --席位资料表 ON 席位代码
        |       and xw.seat_edt="2015-04-30 00:00:00"
        |       left join wwtnmmbr hy on xw.mbr_cd=hy.mbr_cd  --会员资料表 ON 会员代码
        |       and hy.mbr_edt="2015-04-30 00:00:00"
        |       left join swtnialk zh on zh.inv_cd=a.bgddm    --投资者概要表 ON 股东代码
        | where     a.cjsj>=1                              --成交时间
        |       and a.cjsj<=2
        |       and a.cjgs>=123456
        |       and a.zqdh="zqdh123"                            --证券代号
        |       and a.cjrq>="2015-05-30 00:00:00"
        |       and a.cjrq<="2015-05-30 00:00:00"                          --成交日期
        | group by a.zqdh, a.bgddm, b.inv_cd, zh.inv_nm, zh.inv_kind,xw.seat_nm
        | order by sum(a.cjgs) desc
        | limit 10000
        |
      """.stripMargin).collect()
  }

  test("4.2.4") {
    spark.sql(
      """
        |WITH T_CJK AS(            --证券投资人成交总额
        |SELECT BGDDM AS GDDM,     --买方股东代码
        |       SUM(CJGS*CJJG) AS CJJE  --成交价额
        |  FROM DWCJK
        | WHERE     cjrq>="2015-04-30 00:00:00"    --成交日期
        |       AND cjrq<="2015-05-30 00:00:00"
        |       AND cjsj>=1--成交时间
        |       AND cjsj<=2
        |       AND MMLB!='C'
        | GROUP BY BGDDM	           --买方股东代码
        |UNION ALL
        |SELECT SGDDM AS GDDS,     --卖方股东代码
        |       SUM(CJGS*CJJG) AS CJJE  --成交价额
        |  FROM DWCJK
        | where     cjrq>="2015-05-30 00:00:00"
        |       AND cjrq<="2015-05-30 00:00:00"
        |       AND cjsj>=1
        |       AND cjsj<=2
        |       AND MMLB!='C'
        | GROUP BY SGDDM
        |),
        |T_RN AS (                --投资人成交总额
        |SELECT TZZ.UIDENT_NUM,SUM(CJK.CJJE)AS CJJE
        |  FROM T_CJK CJK JOIN SWTNIALK TZZ ON CJK.GDDM=TZZ.INV_CD --投资者概要表
        | GROUP BY TZZ.UIDENT_NUM --投资人证件号
        | ORDER BY CJJE desc
        | limit 5000
        |),
        |T_GDDM as(               --成交总额最前N行的股东代码
        |select SWTNIALK.inv_cd
        |  from T_RN join SWTNIALK on (t_rn.UIDENT_NUM=SWTNIALK.UIDENT_NUM) -- map join
        |)
        |--成交额前N个投资人的证券成交信息
        |SELECT CJK.*
        |  FROM DWCJK CJK JOIN T_GDDM RN ON (RN.INV_CD = CJK.BGDDM OR RN.INV_CD =CJK.SGDDM) --根据投资人的股东代码关联买方和卖方股东代码
        |       AND cjrq>="2015-04-30 00:00:00"
        |       AND cjrq<="2015-05-30 00:00:00"
        |       AND cjsj>=1
        |       AND cjsj<=2
        |       AND MMLB!='C' -- mapjoin
      """.stripMargin).collect
  }

  test("4.2.5") {
    spark.sql(
      """
        |--实现该场景描述的功能
        |WITH T_CJK AS (                      --证券代码买卖每日成交总额列表
        |SELECT CJRQ,                         --成交日期
        |       ZQDH,                         --证券代码
        |       SUBSTR(BHTXH,1,2) AS YYBSBM,  --买方合同序号前两位
        |       BXWDH AS XWDH,                --买方席位代码
        |       SUM(CJGS*CJJG) AS CJJE,       --成交总额=(成交数量*成交价格)
        |       'B' AS FLAG                   --买方标志
        |  FROM DWCJK                         --成交库
        | WHERE      cjrq>="2015-04-30 00:00:00"              --成交日期
        |        and cjrq<="2015-05-30 00:00:00"
        |        and cjsj>=1          --成交时间
        |        and cjsj<=2
        |	and ZQDH="zqdh123"              --证券代号
        |        AND CJGS>0                   --成交数量
        |        AND CJJG>0                   --成交价格
        | GROUP BY CJRQ, ZQDH,SUBSTR(BHTXH,1,2),BXWDH --时间区间为可调参数
        |  UNION ALL
        |SELECT CJRQ,
        |       ZQDH,
        |       SUBSTR(SHTXH,1,2) AS YYBSBM,
        |       SXWDH AS XWDH,
        |       SUM(CJGS*CJJG) AS CJJE,
        |       'S'AS FLAG           --卖方标志
        |  FROM DWCJK
        | WHERE     cjrq>="2015-05-30 00:00:00"
        |       and cjrq<="2015-05-30 00:00:00"
        |       and cjsj>=1
        |       and cjsj<=2
        |       and ZQDH="qqdh123"
        |       AND CJGS>0
        |       AND CJJG>0
        |  GROUP BY CJRQ, ZQDH,SUBSTR(SHTXH,1,2),SXWDH ----时间区间为可调参数
        |),
        |
        |T_YYBCJ AS(        --上表中增加营业部分支机构代码
        |SELECT CJK.ZQDH, BRCH.BRCH_CD, CJK.FLAG, SUM(CJK.CJJE) AS CJJE
        |  FROM T_CJK CJK LEFT JOIN WWTNSEAT SEAT ON (CJK.XWDH=SEAT.SEAT_CD) --营业部资料表
        |       AND (CJK.CJRQ BETWEEN SEAT.SEAT_FDT AND SEAT.SEAT_EDT)
        |       LEFT JOIN WWTNBRCH BRCH ON (SEAT.MBR_CD=BRCH.MBR_CD)
        |       AND (CJK.YYBSBM=BRCH.BRCHID_CD)
        |       AND (BRCH.ISSPEC_FLG='0')
        |       AND (CJK.CJRQ BETWEEN BRCH.BRCH_FDT AND BRCH.BRCH_EDT)
        | GROUP BY CJK.ZQDH,BRCH.BRCH_CD,CJK.FLAG
        |)
        |
        |--生成组内排序序号
        |SELECT  ZQDH,
        |        BRCH_CD,
        |        SUM(CASE WHEN FLAG='B' THEN CJJE ELSE 0 END) BRJE,
        |        DENSE_RANK() OVER(PARTITION BY ZQDH ORDER BY SUM(CASE WHEN FLAG='B' THEN CJJE ELSE 0 END)DESC) BRJE_RN,
        |        SUM(CASE WHEN FLAG='S' THEN CJJE ELSE 0 END )BCJE,
        |        DENSE_RANK() OVER(PARTITION BY ZQDH ORDER BY SUM(CASE WHEN FLAG='S' THEN CJJE ELSE 0 END)DESC) BCJE_RN
        |  FROM  T_YYBCJ YYCJ
        | GROUP BY ZQDH, BRCH_CD
        | ORDER BY ZQDH,BRJE_RN,BCJE_RN
      """.stripMargin).collect()
  }
  test("4.2.6") {
    spark.sql(
      """
        |--计算每天每个投资者买入、卖出金额
        |with cjje as --按类别分类每日成交额列表
        |(
        |  (
        |  select '买入' cjtyp,
        |         cjrq,
        |         bGDDM GDDM,
        |         sum(CJGS*CJJG) cjje,
        |        (case
        |         when ZQDH between '000001' and '0  999' then '1 主板'
        |         when ZQDH like '002%' then '2 中小板'
        |         else '3 创业板'end)  sec_typ
        |    from DWCJK
        |   where     cjrq between "2015-04-30 00:00:00" and "2015-05-30 00:00:00"
        |         and cjsj between 1 and 2
        |         and (ZQDH like '00%' or ZQDH like '30%')
        |         group by 1,cjrq,BGDDM,zqdh
        |   )
        |union all
        |  (
        |  select '卖出' cjtyp,
        |         cjrq,
        |         sGDDM GDDM,sum(CJGS*CJJG) cjje,
        |         (case
        |         when ZQDH between '000001' and '001999' then '1 主板'
        |         when ZQDH like '002%' then '2 中小板' else '3 创业板' end)sec_typ
        |    from DWCJK
        |   where     cjrq between "2015-05-30 00:00:00" and "2015-05-30 00:00:00"
        |         and cjsj between 1 and 2
        |         and (ZQDH like '00%' or ZQDH like '30%')
        |         group by 1,cjrq,SGDDM,zqdh
        |   )
        | )
        |--计算各类投资者每天三板块交易金额
        |select cjtyp,
        |       cjrq,
        |       (case
        |        when inv_typ in('63','64','65')then 'A.证券投资基金'
        |        when inv_typ in('11','13')then 'I.证券自营'
        |        when inv_typ in('61')then 'J.证券公司集合理财'
        |        when inv_typ in('62')then 'B.基金专户'
        |        when inv_typ in('15','68')then 'G.信托'
        |        when inv_typ in('17','69')then 'F.保险'
        |        when inv_typ in('66','67')then 'C.社保基金'
        |        when inv_typ in('70')then 'H.企业年金'
        |        when inv_typ in('71')then 'D.QFII'
        |        when inv_typ in('72')then 'E.RQFII'
        |        when inv_typ in('76')then 'L.私募基金'
        |        when inv_typ in('00')then 'N.个人'
        |        else 'M.一般机构' end) typ,
        |        sec_typ,
        |        sum(cjje) cjje
        |  from cjje a,SWTNIALK b
        | where a.GDDM=b.inv_cd
        | group by cjtyp,cjrq,3,sec_typ
      """.stripMargin).collect()
  }

  test("4.2.7") {
    spark.sql(
      """
        |--所有交易日列表：
        |with dt as(             --找到所有的交易日期
        |select distinct trd_dt
        |  from TSQUOTAT         --交易广播行情
        | where trd_dt>="2015-04-30 00:00:00" and trd_dt<="2015-05-30 00:00:00")
        |,
        |dt1 as(                 --给交易日期加上序号
        |select trd_dt,row_number() over (order by trd_dt) rank
        |  from dt)
        |,
        |--每个交易日和后一个交易日
        |dt2 as(                 --连续的交易日期信息表
        |select a.trd_dt,b.trd_dt next_dt
        |  from dt1 a, dt1 b
        | where a.rank + 1 = b.rank)
        |,
        |--取出每天的昨收盘价
        |QUOTAT as(
        |select distinct trd_dt,sec_cd,LCLOS_PRC --成交日期,证券代号, 昨日收盘
        |  from TSQUOTAT
        | where trd_dt>="2015-04-30 00:00:00" and trd_dt<="2015-04-30 00:00:00")
        |,
        |--计算每天每个投资者三个板块的持股市值（持有数量*第二天的做收盘价）
        |ishc as(
        |select trd_dt,       --交易日期
        |       inv_cd,       --股东代码
        |       sec_typ,  --证券代码分类
        |       sum ((SHR_QTY + CHG_QTY) * LCLOS_PRC) shr_val --当天持有市值
        |       from
        |  ( select a.trd_dt as trd_dt,       --交易日期
        |        b.inv_cd as inv_cd,       --股东代码
        |        (case when b.sec_cd between '000001' and '001999' then '1 主板'
        |              when b.sec_cd like '002%' then '2 中小板'
        |              else '3 创业板' end) sec_typ,  --证券代码分类
        |         b.SHR_QTY, b.CHG_QTY , c.LCLOS_PRC
        |  from dt2 a, wwtnishc b, QUOTAT c
        | where a.trd_dt between rec_fdt and rec_edt
        |       and a.next_dt=c.trd_dt
        |       and b.sec_cd=c.sec_cd
        |       and (b.sec_cd like '00%' or b.sec_cd like '30%')) T
        | group by trd_dt, inv_cd, sec_typ
        |)
        |
        |--计算各类别投资者按板块持股市值
        |select
        |      trd_dt,        --交易日期
        |			 typ, --投资者类别
        |      sec_typ,         --证券代码类别
        |      sum(shr_val)
        |  from (select  a.trd_dt,        --交易日期
        |      (case  when inv_typ in('63','64','65')      then 'A.证券投资基金'
        |             when inv_typ in('11','13')           then 'I.证券自营'
        |             when inv_typ in('61')                then 'J.证券公司集合理财'
        |             when inv_typ in('62')                then 'B.基金专户'
        |             when inv_typ in('15','68')           then 'G.信托'
        |             when inv_typ in('17','69')           then 'F.保险'
        |             when inv_typ in('66','67')           then 'C.社保基金'
        |             when inv_typ in('70')                then 'H.企业年金'
        |             when inv_typ in('71')                then 'D.QFII'
        |             when inv_typ in('72')                then 'E.RQFII'
        |             when inv_typ in('76')                then 'L.私募基金'
        |             when inv_typ in('00')                then 'N.个人'
        |             else                                      'M.一般机构' end)
        |			 typ, --投资者类别
        |       sec_typ,         --证券代码类别
        |      shr_val
        |  from ishc a, SWTNIALK b
        | where a.inv_cd = b.inv_cd) T1
        | group by trd_dt, typ, sec_typ
      """.stripMargin).collect
  }
  test("4.2.8") {
    spark.sql(
      """
        |--每秒委托记录
        |with t_order as(
        |select WTRQ,cast(WTSJ/100 as int) WTSJ,GDDM,count(JLHM)rec_seq    --委托日期，委托时间，股东代码，委托记录
        |  from DWWTK        --委托库
        | where     WTRQ between "2015-04-30 00:00:00" and "2015-05-30 00:00:00"  --委托日期
        |       and wtsj between 1 and 2  --委托时间
        |       and MMLB in('B','S')                  --买卖类别
        | group by WTRQ, 2,GDDM           --委托日期，委托时间，股东代码
        |),
        |--每天委托记录
        |temp as(
        |select WTRQ,GDDM,count(WTSJ)tm, sum(rec_seq) rec_seq_cnt
        |  from t_order
        |  where rec_seq > 5
        | group by WTRQ,GDDM
        |),
        |--每天委托记录
        |temp2 as(
        |select WTRQ,GDDM,count(WTSJ)tm, sum(rec_seq) rec_seq_cnt
        |  from t_order
        | group by WTRQ,GDDM
        |)
        |
        |select WTRQ,GDDM
        |  from temp
        | where tm>=10
        |union
        |select WTRQ,GDDM
        |  from temp2
        | where rec_seq_cnt>=100
        |
        |
      """.stripMargin).collect()
  }

}