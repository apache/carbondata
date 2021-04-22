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
package org.apache.carbondata.spark.testsuite.describeTable

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedIndexCommandException}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * test class for describe table .
 */
class TestDescribeTable extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("DROP TABLE IF EXISTS Desc1")
    sql("DROP TABLE IF EXISTS Desc2")
    sql("drop table if exists a")
    sql("CREATE TABLE Desc1(" +
        "Dec1Col1 String, Dec1Col2 String, Dec1Col3 int, Dec1Col4 double) STORED AS carbondata")
    sql("DESC Desc1")
    sql("DROP TABLE Desc1")
    sql("CREATE TABLE Desc1(" +
        "Dec2Col1 BigInt, Dec2Col2 String, Dec2Col3 Bigint, Dec2Col4 Decimal) STORED AS carbondata")
    sql("CREATE TABLE Desc2(" +
        "Dec2Col1 BigInt, Dec2Col2 String, Dec2Col3 Bigint, Dec2Col4 Decimal) STORED AS carbondata")
    sql("drop table if exists complexcarbontable")
    sql("create table complexcarbontable(deviceInformationId int, " +
        "channelsId map<string,string>, mobile struct<imei:string, imsi:string>," +
        "MAC array<string> COMMENT 'this is an array'," +
        "locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
        "ActiveProvince:string, Activecity:map<string,string>, " +
        "ActiveDistrict:string, ActiveStreet:array<string>>>," +
        "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
        "double,contractNumber struct<num:double,contract:map<string,array<string>>>," +
        "decimalColumn map<string,struct<d:decimal(10,3), s:struct<im:string>>>) " +
        "STORED AS carbondata"
    )
  }

  test("test describe table") {
    checkAnswer(sql("DESC Desc1"), Seq(Row("dec2col1", "bigint", null),
      Row("dec2col2", "string", null),
      Row("dec2col3", "bigint", null),
      Row("dec2col4", "decimal(10,0)", null)))
  }

  test("test describe formatted table") {
    checkExistence(sql("DESC FORMATTED Desc1"), true, "Table Block Size")
  }

  test("test describe formatted for partition table") {
    sql("create table a(a string) partitioned by (b int) STORED AS carbondata")
    sql("insert into a values('a',1)")
    sql("insert into a values('a',2)")
    val desc = sql("describe formatted a").collect()
    assert(desc(desc.indexWhere(_.get(0).toString.contains("#Partition")) + 2)
      .get(0).toString.contains("b"))
    val descPar = sql("describe formatted a partition(b=1)").collect
    descPar.find(_.get(0).toString.contains("Partition Value:")) match {
      case Some(row) => assert(row.get(1).toString.contains("1"))
      case None => fail("Partition Value not found in describe formatted")
    }
    descPar.find(_.get(0).toString.contains("Location:")) match {
      case Some(row) => assert(row.get(1).toString.contains("target/warehouse/a/b=1"))
      case None => fail("Partition Location not found in describe formatted")
    }
    assert(descPar.exists(_.toString().contains("Partition Parameters:")))
  }

  test("test describe column field name") {
    // describe primitive column
    var desc = sql("describe column deviceInformationId on complexcarbontable").collect()
    assert(desc(0).get(0).asInstanceOf[String].trim.equals("deviceInformationId"))
    assert(desc(0).get(1).asInstanceOf[String].trim.equals("integer"))

    // describe simple map
    /*
    +----------------------------+---------+-------+
    |col_name                    |data_type|comment|
    +----------------------------+---------+-------+
    |channelsId                  |map      |null   |
    |## Children of channelsId:  |         |       |
    |key                         |string   |null   |
    |value                       |string   |null   |
    +----------------------------+---------+-------+
    */
    desc = sql("desc column channelsId on complexcarbontable").collect()
    assert(desc(2).get(0).asInstanceOf[String].trim.equals("key"))
    assert(desc(2).get(1).asInstanceOf[String].trim.equals("string"))
    assert(desc(3).get(0).asInstanceOf[String].trim.equals("value"))
    assert(desc(3).get(1).asInstanceOf[String].trim.equals("string"))

    // describe struct
    desc = sql("describe column mobile on complexcarbontable").collect()
    assert(desc(2).get(0).asInstanceOf[String].trim.equals("imei"))
    assert(desc(3).get(0).asInstanceOf[String].trim.equals("imsi"))

    // describe array
    desc = sql("describe column MAC on complexcarbontable").collect()
    assert(desc(0).get(2).asInstanceOf[String].trim.equals("this is an array"))
    assert(desc(2).get(0).asInstanceOf[String].trim.equals("item"))
    assert(desc(2).get(1).asInstanceOf[String].trim.equals("string"))

    // describe struct<string,array<string>>
    desc = sql("describe column proddate on complexcarbontable").collect()
    assert(desc(2).get(0).asInstanceOf[String].trim.equals("productiondate"))
    assert(desc(2).get(1).asInstanceOf[String].trim.equals("string"))
    assert(desc(3).get(0).asInstanceOf[String].trim.equals("activedeactivedate"))
    assert(desc(3).get(1).asInstanceOf[String].trim.equals("array<string>"))

    desc = sql("describe column proddate.activeDeactivedate on complexcarbontable").collect()
    assert(desc(2).get(0).asInstanceOf[String].trim.equals("item"))
    assert(desc(2).get(1).asInstanceOf[String].trim.equals("string"))

    // describe array<struct<int,string,string,map<string,string>,string,array<string>>>
    desc = sql("describe column locationinfo on complexcarbontable").collect()
    assert(desc(2).get(0).asInstanceOf[String].trim.equals("item"))
    assert(desc(2).get(1).asInstanceOf[String].trim.equals(
        "struct<activeareaid:int,activecountry:string,activeprovince:string," +
        "activecity:map<string,string>,activedistrict:string,activestreet:array<string>>"))

    desc = sql("describe column locationinfo.item on complexcarbontable").collect()
    assert(desc(5).get(0).asInstanceOf[String].trim.equals("activecity"))
    assert(desc(5).get(1).asInstanceOf[String].trim.equals("map<string,string>"))
    assert(desc(7).get(0).asInstanceOf[String].trim.equals("activestreet"))
    assert(desc(7).get(1).asInstanceOf[String].trim.equals("array<string>"))

    desc = sql("describe column locationinfo.item.Activecity on complexcarbontable").collect()
    assert(desc(2).get(0).asInstanceOf[String].trim.equals("key"))
    assert(desc(3).get(0).asInstanceOf[String].trim.equals("value"))

    desc = sql("describe column locationinfo.item.ActiveStreet on complexcarbontable").collect()
    assert(desc(2).get(0).asInstanceOf[String].trim.equals("item"))
    assert(desc(2).get(1).asInstanceOf[String].trim.equals("string"))

    // describe struct<double,map<string,array<string>>>
    desc = sql("describe column contractNumber on complexcarbontable").collect()
    assert(desc(2).get(0).asInstanceOf[String].trim.equals("num"))
    assert(desc(2).get(1).asInstanceOf[String].trim.equals("double"))
    assert(desc(3).get(0).asInstanceOf[String].trim.equals("contract"))
    assert(desc(3).get(1).asInstanceOf[String].trim.equals("map<string,array<string>>"))

    desc = sql("describe column contractNumber.contract on complexcarbontable").collect()
    assert(desc(3).get(0).asInstanceOf[String].trim.equals("value"))
    assert(desc(3).get(1).asInstanceOf[String].trim.equals("array<string>"))

    desc = sql("describe column contractNumber.contract.value on complexcarbontable").collect()
    assert(desc(2).get(0).asInstanceOf[String].trim.equals("item"))
    assert(desc(2).get(1).asInstanceOf[String].trim.equals("string"))

    // describe map<string,struct<decimal(10,3),struct<string>>>
    desc = sql("describe column decimalcolumn on complexcarbontable").collect()
    assert(desc(3).get(0).asInstanceOf[String].trim.equals("value"))
    assert(desc(3).get(1).asInstanceOf[String].trim
      .equals("struct<d:decimal(10,3),s:struct<im:string>>"))

    desc = sql("describe column decimalcolumn.value on complexcarbontable").collect()
    assert(desc(3).get(0).asInstanceOf[String].trim.equals("s"))
    assert(desc(3).get(1).asInstanceOf[String].trim.equals("struct<im:string>"))

    desc = sql("describe column decimalcolumn.value.s on complexcarbontable").collect()
    assert(desc(2).get(0).asInstanceOf[String].trim.equals("im"))
    assert(desc(2).get(1).asInstanceOf[String].trim.equals("string"))
  }

  test("test describe with invalid table and field names") {
    // describe column with invalid table name
    var exception1 = intercept[NoSuchTableException](sql(
      "describe column decimalcolumn on invalidTable"))
    assert(exception1.getMessage.contains(
      "Table or view 'invalidTable' not found in database 'default'"))

    // describe short with invalid table name
    exception1 = intercept[NoSuchTableException](sql(
      "describe short invalidTable"))
    assert(exception1.getMessage.contains(
      "Table or view 'invalidTable' not found in database 'default'"))

    // describe column with invalid field name
    var exception2 = intercept[MalformedCarbonCommandException](sql(
      "describe column invalidField on complexcarbontable"))
    assert(exception2.getMessage.contains(
      "Column invalidField does not exists in the table default.complexcarbontable"))

    // describe column with invalid child names
    exception2 = intercept[MalformedCarbonCommandException](sql(
      "describe column MAC.one on complexcarbontable"))
    assert(exception2.getMessage.contains(
      "one is invalid child name for column MAC of table: complexcarbontable"))

    exception2 = intercept[MalformedCarbonCommandException](sql(
      "describe column deviceInformationId.x on complexcarbontable"))
    assert(exception2.getMessage.contains(
      "x is invalid child name for column deviceInformationId of table: complexcarbontable"))

    exception2 = intercept[MalformedCarbonCommandException](sql(
      "describe column mobile.imei.x on complexcarbontable"))
    assert(exception2.getMessage.contains(
      "x is invalid child name for column mobile.imei of table: complexcarbontable"))

    exception2 = intercept[MalformedCarbonCommandException](sql(
      "describe column MAC.item.x on complexcarbontable"))
    assert(exception2.getMessage.contains(
      "x is invalid child name for column MAC.item of table: complexcarbontable"))

    exception2 = intercept[MalformedCarbonCommandException](sql(
      "describe column channelsId.key.x on complexcarbontable"))
    assert(exception2.getMessage.contains(
      "x is invalid child name for column channelsId.key of table: complexcarbontable"))
  }

  test("test describe short table format") {
    val desc = sql("desc short complexcarbontable").collect()
    assert(desc(1).get(0).asInstanceOf[String].trim.equals("channelsid"))
    assert(desc(1).get(1).asInstanceOf[String].trim.equals("map<..>"))
    assert(desc(2).get(0).asInstanceOf[String].trim.equals("mobile"))
    assert(desc(2).get(1).asInstanceOf[String].trim.equals("struct<..>"))
    assert(desc(3).get(0).asInstanceOf[String].trim.equals("mac"))
    assert(desc(3).get(1).asInstanceOf[String].trim.equals("array<..>"))
  }

  override def afterAll: Unit = {
    sql("DROP TABLE Desc1")
    sql("DROP TABLE Desc2")
    sql("drop table if exists a")
    sql("drop table if exists b")
    sql("drop table if exists complexcarbontable")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR,
      CarbonCommonConstants.DEFAULT_COMPRESSOR)
  }

}
