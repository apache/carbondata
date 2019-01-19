package org.apache.carbondata.integration.spark.testsuite.complexType

import java.sql.Timestamp

import scala.collection.mutable

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test class of testing projection with complex data type
 *
 */

class TestComplexDataType extends QueryTest with BeforeAndAfterAll {

  val badRecordAction = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION)

  override def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS table1")
    sql("DROP TABLE IF EXISTS test")
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS table1")
    sql("DROP TABLE IF EXISTS test")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, badRecordAction)
  }

  test("test Projection PushDown for Struct - Integer type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll string,person Struct<detail:int>) stored by " +
      "'carbondata'")
    sql("insert into table1 values('abc',1)")
    checkAnswer(sql("select roll,person,person.detail from table1"),
      Seq(Row("abc", Row(1), 1)))
    checkAnswer(sql("select person,person.detail from table1"),
      Seq(Row(Row(1), 1)))
    checkAnswer(sql("select roll,person from table1"), Seq(Row("abc", Row(1))))
    checkAnswer(sql("select roll from table1"), Seq(Row("abc")))
  }

  test("test projection pushDown for Array") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll string,person array<int>) stored by " +
      "'carbondata'")
    sql("insert into table1 values('abc','1\0012\0013')")
    sql("select * from table1").show(false)
    checkAnswer(sql("select roll,person from table1"),
      Seq(Row("abc", mutable.WrappedArray.make(Array(1, 2, 3)))))
  }

  test("test Projection PushDown for StructofArray - Integer type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:array<int>>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,'1\0022')")
    checkAnswer(sql("select person.detail[0] from table1"), Seq(Row(1)))
    checkAnswer(sql("select person.detail[1] from table1"), Seq(Row(2)))
    checkAnswer(sql("select roll,person from table1"),
      Seq(Row(1, Row(mutable.WrappedArray.make(Array(1, 2))))))
    checkAnswer(sql("select roll,person.detail[0],person,person.detail[1] from table1"),
      Seq(Row(1, 1, Row(mutable.WrappedArray.make(Array(1, 2))), 2)))
  }

  test("test Projection PushDown for Struct - String type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:string>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,'abc')")
    checkExistence(sql("select person from table1"), true, "abc")
    checkAnswer(sql("select roll,person,person.detail from table1"), Seq(Row(1, Row("abc"), "abc")))
    checkExistence(sql("select person.detail from table1"), true, "abc")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row("abc"))))
    checkAnswer(sql("select roll,person,roll,person from table1"),
      Seq(Row(1, Row("abc"), 1, Row("abc"))))
  }

  test("test Projection PushDown for StructofArray - String type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:array<string>>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,'abc\002bcd')")
    checkAnswer(sql("select person.detail[0] from table1"), Seq(Row("abc")))
    checkAnswer(sql("select person.detail[1] from table1"), Seq(Row("bcd")))
    checkAnswer(sql("select roll,person from table1"),
      Seq(Row(1, Row(mutable.WrappedArray.make(Array("abc", "bcd"))))))
    checkAnswer(sql("select roll,person.detail[0],person,person.detail[1] from table1"),
      Seq(Row(1, "abc", Row(mutable.WrappedArray.make(Array("abc", "bcd"))), "bcd")))
  }

  test("test Projection PushDown for Array - String type when Array is Empty") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FAIL")
    sql("drop table if exists table1")
    sql("create table table1 (detail array<string>) stored by 'carbondata'")
    sql("insert into table1 values('')")
    checkAnswer(sql("select detail[0] from table1"), Seq(Row("")))
    sql("drop table if exists table1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, badRecordAction)
  }

  test("test Projection PushDown for Struct - Array type when Array is Empty") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FAIL")
    sql("drop table if exists table1")
    sql("create table table1 (person struct<detail:array<string>,age:int>) stored by 'carbondata'")
    sql("insert into table1 values ('\0011')")
    checkAnswer(sql("select person.detail[0] from table1"), Seq(Row("")))
    checkAnswer(sql("select person.age from table1"), Seq(Row(1)))
    sql("drop table if exists table1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, badRecordAction)
  }

  test("test Projection PushDown for Struct - Double type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:double>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,10.00)")
    checkExistence(sql("select person from table1"), true, "10.0")
    checkAnswer(sql("select roll,person,person.detail from table1"), Seq(Row(1, Row(10.0), 10.0)))
    checkExistence(sql("select person.detail from table1"), true, "10.0")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(10.0))))
  }

  test("test Projection PushDown for StructofArray - Double type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:array<double>>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,'10.00\00220.00')")
    checkAnswer(sql("select person.detail[0] from table1"), Seq(Row(10.0)))
    checkAnswer(sql("select person.detail[1] from table1"), Seq(Row(20.0)))
    checkAnswer(sql("select roll,person from table1"),
      Seq(Row(1, Row(mutable.WrappedArray.make(Array(10.0, 20.0))))))
  }

  test("test Projection PushDown for Struct - Decimal type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:decimal(3,2)>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,3.4)")
    checkExistence(sql("select person from table1"), true, "3")
    checkExistence(sql("select person.detail from table1"), true, "3")
    checkAnswer(sql("select roll,person.detail from table1"), Seq(Row(1, 3.40)))
  }

  test("test Projection PushDown for StructofArray - Decimal type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:array<decimal(3,2)>>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,'3.4\0024.2')")
    checkAnswer(sql("select person.detail[0] from table1"), Seq(Row(3.40)))
    checkAnswer(sql("select person.detail[1] from table1"), Seq(Row(4.20)))
    checkAnswer(sql("select roll,person.detail[0] from table1"), Seq(Row(1, 3.40)))
  }

  test("test Projection PushDown for Struct - timestamp type") {
    sql("DROP TABLE IF EXISTS table1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(
      "create table table1 (roll int,person Struct<detail:timestamp>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,'2018/01/01')")
    checkExistence(sql("select person from table1"), true, "2018-01-01 00:00:00.0")
    checkAnswer(sql("select person,roll,person.detail from table1"),
      Seq(Row(Row(Timestamp.valueOf("2018-01-01 00:00:00.0")), 1,
        Timestamp.valueOf("2018-01-01 00:00:00.0"))))
    checkAnswer(sql("select roll,person from table1"),
      Seq(Row(1, Row(Timestamp.valueOf("2018-01-01 00:00:00.0")))))
    checkAnswer(sql("select roll,person.detail from table1"),
      Seq(Row(1, Timestamp.valueOf("2018-01-01 00:00:00.0"))))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
  }

  test("test Projection PushDown for StructofArray - timestamp type") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:array<timestamp>>) stored by " +
      "'carbondata'")
    sql("insert into table1 select 1,'2018/01/01\0022017/01/01'")
    checkExistence(sql("select person.detail[0] from table1"), true, "2018-01-01 00:00:00.0")
    checkExistence(sql("select person.detail[1] from table1"), true, "2017-01-01 00:00:00.0")
    checkAnswer(sql("select roll,person from table1"),
      Seq(Row(1, Row(mutable.WrappedArray
        .make(Array(Timestamp.valueOf("2018-01-01 00:00:00.0"),
          Timestamp.valueOf("2017-01-01 00:00:00.0")))))))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }

  test("test Projection PushDown for Struct - long type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:long>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,2018888)")
    checkExistence(sql("select person from table1"), true, "2018888")
    checkAnswer(sql("select person,roll,person.detail from table1"),
      Seq(Row(Row(2018888), 1, 2018888)))
    checkExistence(sql("select person.detail from table1"), true, "2018888")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(2018888))))
  }

  test("test Projection PushDown for StructofArray - long type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:array<long>>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,'2018888\0022018889')")
    checkAnswer(sql("select person.detail[0] from table1"), Seq(Row(2018888)))
    checkAnswer(sql("select person.detail[1] from table1"), Seq(Row(2018889)))
    checkAnswer(sql("select person,roll from table1"),
      Seq(Row(Row(mutable.WrappedArray.make(Array(2018888, 2018889))), 1)))
  }

  test("test Projection PushDown for Struct - short type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:short>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,20)")
    checkExistence(sql("select person from table1"), true, "20")
    checkAnswer(sql("select person,roll,person.detail from table1"), Seq(Row(Row(20), 1, 20)))
    checkExistence(sql("select person.detail from table1"), true, "20")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(20))))
  }

  test("test Projection PushDown for StructofArray - short type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:array<short>>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,'20\00230')")
    checkAnswer(sql("select person.detail[0] from table1"), Seq(Row(20)))
    checkAnswer(sql("select person.detail[1] from table1"), Seq(Row(30)))
    checkAnswer(sql("select person,roll from table1"),
      Seq(Row(Row(mutable.WrappedArray.make(Array(20, 30))), 1)))
  }

  test("test Projection PushDown for Struct - boolean type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:boolean>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,true)")
    checkExistence(sql("select person from table1"), true, "true")
    checkAnswer(sql("select person,roll,person.detail from table1"), Seq(Row(Row(true), 1, true)))
    checkExistence(sql("select person.detail from table1"), true, "true")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(true))))
  }

  test("test Projection PushDown for StructofArray - boolean type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:array<boolean>>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,'true\002false')")
    checkAnswer(sql("select person.detail[0] from table1"), Seq(Row(true)))
    checkAnswer(sql("select person.detail[1] from table1"), Seq(Row(false)))
    checkAnswer(sql("select person,roll from table1"),
      Seq(Row(Row(mutable.WrappedArray.make(Array(true, false))), 1)))
  }

  test("test Projection PushDown for StructofStruct - Integer type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:Struct<age:int>>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,'1')")
    checkExistence(sql("select person from table1"), true, "1")
    checkAnswer(sql("select person,roll,person.detail from table1"),
      Seq(Row(Row(Row(1)), 1, Row(1))))
    checkExistence(sql("select person.detail.age from table1"), true, "1")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(Row(1)))))
  }

  test("test Projection PushDown for StructofStruct - String type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:Struct<age:string>>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,'abc')")
    checkExistence(sql("select person from table1"), true, "abc")
    checkAnswer(sql("select person,person.detail from table1"),
      Seq(Row(Row(Row("abc")), Row("abc"))))
    checkExistence(sql("select person.detail.age from table1"), true, "abc")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(Row("abc")))))
  }

  test("test Projection PushDown for StructofStruct - Double type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:Struct<age:double>>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,10.00)")
    checkExistence(sql("select person from table1"), true, "10.0")
    checkAnswer(sql("select person,person.detail from table1"), Seq(Row(Row(Row(10.0)), Row(10.0))))
    checkExistence(sql("select person.detail.age from table1"), true, "10.0")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(Row(10.0)))))
  }

  test("test Projection PushDown for StructofStruct - Decimal type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:Struct<age:decimal(3,2)>>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,3.2)")
    checkExistence(sql("select person from table1"), true, "3")
    checkExistence(sql("select person.detail.age from table1"), true, "3")
  }

  test("test Projection PushDown for StructofStruct - timestamp type") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:Struct<age:timestamp>>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,'2018/01/01')")
    checkExistence(sql("select person from table1"), true, "2018-01-01 00:00:00.0")
    checkAnswer(sql("select person,person.detail from table1"),
      Seq(Row(Row(Row(Timestamp.valueOf("2018-01-01 00:00:00.0"))),
        Row(Timestamp.valueOf("2018-01-01 00:00:00.0")))))
    checkExistence(sql("select person.detail.age from table1"), true, "2018-01-01 00:00:00.0")
    checkAnswer(sql("select roll,person from table1"),
      Seq(Row(1, Row(Row(Timestamp.valueOf("2018-01-01 00:00:00.0"))))))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }

  test("test Projection PushDown for StructofStruct - long type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:Struct<age:long>>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,2018888)")
    checkExistence(sql("select person from table1"), true, "2018888")
    checkAnswer(sql("select person,person.detail from table1"),
      Seq(Row(Row(Row(2018888)), Row(2018888))))
    checkExistence(sql("select person.detail.age from table1"), true, "2018888")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(Row(2018888)))))

  }

  test("test Projection PushDown for StructofStruct - short type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:Struct<age:short>>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,20)")
    checkExistence(sql("select person from table1"), true, "20")
    checkAnswer(sql("select person,person.detail from table1"), Seq(Row(Row(Row(20)), Row(20))))
    checkExistence(sql("select person.detail.age from table1"), true, "20")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(Row(20)))))
  }

  test("test Projection PushDown for  StructofStruct - boolean type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:Struct<age:boolean>>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,true)")
    checkExistence(sql("select person from table1"), true, "true")
    checkAnswer(sql("select person,person.detail from table1"), Seq(Row(Row(Row(true)), Row(true))))
    checkExistence(sql("select person.detail.age from table1"), true, "true")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(Row(true)))))
  }

  test("test StructofArray pushdown") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (person Struct<detail:string,ph:array<int>>) stored by " +
      "'carbondata' tblproperties('dictionary_include'='person')")
    sql("insert into table1 values ('abc\0012')")
    sql("select person from table1").show(false)
    sql("select person.detail, person.ph[0] from table1").show(false)
  }

  test("test Projection PushDown for Struct - Merge column") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:int,age:string,height:double>) stored " +
      "by " +
      "'carbondata'")
    sql(
      "load data inpath '" + resourcesPath +
      "/Struct.csv' into table table1 options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,person','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'='&')")
    checkAnswer(sql("select person from table1"), Seq(
      Row(Row(11, "abc", 10.0)),
      Row(Row(12, "abcd", 10.01)),
      Row(Row(13, "abce", 10.02)),
      Row(Row(14, "abcr", 10.03)),
      Row(Row(15, "abct", 10.04)),
      Row(Row(16, "abcn", 10.05)),
      Row(Row(17, "abcq", 10.06)),
      Row(Row(18, "abcs", 10.07)),
      Row(Row(19, "abcm", 10.08)),
      Row(Row(20, "abck", 10.09))
    ))
    checkAnswer(sql("select person.detail,person.age,person.height from table1"), Seq(
      Row(11, "abc", 10.0),
      Row(12, "abcd", 10.01),
      Row(13, "abce", 10.02),
      Row(14, "abcr", 10.03),
      Row(15, "abct", 10.04),
      Row(16, "abcn", 10.05),
      Row(17, "abcq", 10.06),
      Row(18, "abcs", 10.07),
      Row(19, "abcm", 10.08),
      Row(20, "abck", 10.09)))
    checkAnswer(sql("select person.age,person.detail,person.height from table1"), Seq(
      Row("abc", 11, 10.0),
      Row("abcd", 12, 10.01),
      Row("abce", 13, 10.02),
      Row("abcr", 14, 10.03),
      Row("abct", 15, 10.04),
      Row("abcn", 16, 10.05),
      Row("abcq", 17, 10.06),
      Row("abcs", 18, 10.07),
      Row("abcm", 19, 10.08),
      Row("abck", 20, 10.09)))
    checkAnswer(sql("select person.height,person.age,person.detail from table1"), Seq(
      Row(10.0, "abc", 11),
      Row(10.01, "abcd", 12),
      Row(10.02, "abce", 13),
      Row(10.03, "abcr", 14),
      Row(10.04, "abct", 15),
      Row(10.05, "abcn", 16),
      Row(10.06, "abcq", 17),
      Row(10.07, "abcs", 18),
      Row(10.08, "abcm", 19),
      Row(10.09, "abck", 20)))
  }

  test("test Projection PushDown for StructofStruct - Merging columns") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:Struct<age:int,name:string," +
      "height:double>>) stored " +
      "by " +
      "'carbondata'")
    sql(
      "load data inpath '" + resourcesPath +
      "/StructofStruct.csv' into table table1 options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,person','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'='&')")
    checkAnswer(sql("select person from table1"), Seq(
      Row(Row(Row(11, "abc", 10.0))),
      Row(Row(Row(12, "abcd", 10.01))),
      Row(Row(Row(13, "abce", 10.02))),
      Row(Row(Row(14, "abcr", 10.03))),
      Row(Row(Row(15, "abct", 10.04))),
      Row(Row(Row(16, "abcn", 10.05))),
      Row(Row(Row(17, "abcq", 10.06))),
      Row(Row(Row(18, "abcs", 10.07))),
      Row(Row(Row(19, "abcm", 10.08))),
      Row(Row(Row(20, "abck", 10.09)))))
    checkAnswer(sql("select person.detail.age,person.detail.name,person.detail.height from table1"),
      Seq(
        Row(11, "abc", 10.0),
        Row(12, "abcd", 10.01),
        Row(13, "abce", 10.02),
        Row(14, "abcr", 10.03),
        Row(15, "abct", 10.04),
        Row(16, "abcn", 10.05),
        Row(17, "abcq", 10.06),
        Row(18, "abcs", 10.07),
        Row(19, "abcm", 10.08),
        Row(20, "abck", 10.09)))
    checkAnswer(sql("select person.detail.name,person.detail.age,person.detail.height from table1"),
      Seq(
        Row("abc", 11, 10.0),
        Row("abcd", 12, 10.01),
        Row("abce", 13, 10.02),
        Row("abcr", 14, 10.03),
        Row("abct", 15, 10.04),
        Row("abcn", 16, 10.05),
        Row("abcq", 17, 10.06),
        Row("abcs", 18, 10.07),
        Row("abcm", 19, 10.08),
        Row("abck", 20, 10.09)))
    checkAnswer(sql("select person.detail.height,person.detail.name,person.detail.age from table1"),
      Seq(
        Row(10.0, "abc", 11),
        Row(10.01, "abcd", 12),
        Row(10.02, "abce", 13),
        Row(10.03, "abcr", 14),
        Row(10.04, "abct", 15),
        Row(10.05, "abcn", 16),
        Row(10.06, "abcq", 17),
        Row(10.07, "abcs", 18),
        Row(10.08, "abcm", 19),
        Row(10.09, "abck", 20)))
    checkAnswer(sql("select person.detail from table1"),
      Seq(
        Row(Row(11, "abc", 10.0)),
        Row(Row(12, "abcd", 10.01)),
        Row(Row(13, "abce", 10.02)),
        Row(Row(14, "abcr", 10.03)),
        Row(Row(15, "abct", 10.04)),
        Row(Row(16, "abcn", 10.05)),
        Row(Row(17, "abcq", 10.06)),
        Row(Row(18, "abcs", 10.07)),
        Row(Row(19, "abcm", 10.08)),
        Row(Row(20, "abck", 10.09))))
    checkAnswer(sql("select person.detail.age from table1"), Seq(
      Row(11), Row(12), Row(13), Row(14), Row(15), Row(16), Row(17), Row(18), Row(19), Row(20)))
  }

  test("test Projection PushDown for more than one Struct column- Integer type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll string,person Struct<detail:int,age:string>,person1 " +
      "Struct<detail:int,age:array<string>>) stored by " +
      "'carbondata'")
    sql("insert into table1 values('abc','1\001abc','2\001cde')")
    sql("select person.detail,person1.age from table1").show(false)
  }

  test("test Projection PushDown for more than one Struct column Cases -1") {
    sql("drop table if exists test")
    sql("create table test (a struct<b:int, c:struct<d:int,e:int>>) stored by 'carbondata'")
    sql("insert into test select '1\0012\0023'")
    checkAnswer(sql("select * from test"), Seq(Row(Row(1, Row(2, 3)))))
    checkAnswer(sql("select a.b,a.c from test"), Seq(Row(1, Row(2, 3))))
    checkAnswer(sql("select a.c, a.b from test"), Seq(Row(Row(2, 3), 1)))
    checkAnswer(sql("select a.c,a,a.b from test"), Seq(Row(Row(2, 3), Row(1, Row(2, 3)), 1)))
    checkAnswer(sql("select a.c from test"), Seq(Row(Row(2, 3))))
    checkAnswer(sql("select a.b from test"), Seq(Row(1)))
    sql("drop table if exists test")
  }

  test("test Projection PushDown for with more than one StructofArray column - Integer type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (person Struct<detail:array<int>>,person1 Struct<detail:array<int>>) " +
      "stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,2)")
    sql("select person.detail[0],person1.detail[0] from table1").show(false)
  }

  test("test Projection PushDown for StructofStruct case1 - Merging columns") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,a struct<b:int,c:string,d:int,e:string,f:struct<g:int," +
      "h:string,i:int>,j:int>) stored " +
      "by " +
      "'carbondata'")
    sql("insert into table1 values(1,'1\001abc\0012\001efg\0013\002mno\0024\0015')")
    sql("insert into table1 values(2,'1\001abc\0012\001efg\0013\002mno\0024\0015')")
    sql("insert into table1 values(3,'1\001abc\0012\001efg\0013\002mno\0024\0015')")
    checkAnswer(sql("select a.b from table1"), Seq(Row(1), Row(1), Row(1)))
    checkAnswer(sql("select a.c from table1"), Seq(Row("abc"), Row("abc"), Row("abc")))
    checkAnswer(sql("select a.d from table1"), Seq(Row(2), Row(2), Row(2)))
    checkAnswer(sql("select a.e from table1"), Seq(Row("efg"), Row("efg"), Row("efg")))
    checkAnswer(sql("select a.f from table1"),
      Seq(Row(Row(3, "mno", 4)), Row(Row(3, "mno", 4)), Row(Row(3, "mno", 4))))
    checkAnswer(sql("select a.f.g  from table1"), Seq(Row(3), Row(3), Row(3)))
    checkAnswer(sql("select a.f.h  from table1"), Seq(Row("mno"), Row("mno"), Row("mno")))
    checkAnswer(sql("select a.f.i  from table1"), Seq(Row(4), Row(4), Row(4)))
    checkAnswer(sql("select a.f.g,a.f.h,a.f.i  from table1"),
      Seq(Row(3, "mno", 4), Row(3, "mno", 4), Row(3, "mno", 4)))
    checkAnswer(sql("select a.b,a.f from table1"),
      Seq(Row(1, Row(3, "mno", 4)), Row(1, Row(3, "mno", 4)), Row(1, Row(3, "mno", 4))))
    checkAnswer(sql("select a.c,a.f from table1"),
      Seq(Row("abc", Row(3, "mno", 4)), Row("abc", Row(3, "mno", 4)), Row("abc", Row(3, "mno", 4))))
    checkAnswer(sql("select a.d,a.f from table1"),
      Seq(Row(2, Row(3, "mno", 4)), Row(2, Row(3, "mno", 4)), Row(2, Row(3, "mno", 4))))
    checkAnswer(sql("select a.j from table1"), Seq(Row(5), Row(5), Row(5)))
    checkAnswer(sql("select * from table1"),
      Seq(Row(1, Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(2, Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(3, Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5))))
    checkAnswer(sql("select *,a from table1"),
      Seq(Row(1,
        Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5),
        Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(2,
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5),
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(3,
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5),
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5))))
  }

  test("test Projection PushDown for StructofStruct for Dictionary Include ") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,a struct<b:int,c:string,d:int,e:string,f:struct<g:int," +
      "h:string,i:int>,j:int>) stored " +
      "by " +
      "'carbondata' tblproperties('dictionary_include'='a')")
    sql("insert into table1 values(1,'1\001abc\0012\001efg\0013\002mno\0024\0015')")
    sql("insert into table1 values(2,'1\001abc\0012\001efg\0013\002mno\0024\0015')")
    sql("insert into table1 values(3,'1\001abc\0012\001efg\0013\002mno\0024\0015')")

    checkAnswer(sql("select a.b from table1"), Seq(Row(1), Row(1), Row(1)))
    checkAnswer(sql("select a.c from table1"), Seq(Row("abc"), Row("abc"), Row("abc")))
    checkAnswer(sql("select a.d from table1"), Seq(Row(2), Row(2), Row(2)))
    checkAnswer(sql("select a.e from table1"), Seq(Row("efg"), Row("efg"), Row("efg")))
    checkAnswer(sql("select a.f from table1"),
      Seq(Row(Row(3, "mno", 4)), Row(Row(3, "mno", 4)), Row(Row(3, "mno", 4))))
    checkAnswer(sql("select a.f.g  from table1"), Seq(Row(3), Row(3), Row(3)))
    checkAnswer(sql("select a.f.h  from table1"), Seq(Row("mno"), Row("mno"), Row("mno")))
    checkAnswer(sql("select a.f.i  from table1"), Seq(Row(4), Row(4), Row(4)))
    checkAnswer(sql("select a.f.g,a.f.h,a.f.i  from table1"),
      Seq(Row(3, "mno", 4), Row(3, "mno", 4), Row(3, "mno", 4)))
    checkAnswer(sql("select a.b,a.f from table1"),
      Seq(Row(1, Row(3, "mno", 4)), Row(1, Row(3, "mno", 4)), Row(1, Row(3, "mno", 4))))
    checkAnswer(sql("select a.c,a.f from table1"),
      Seq(Row("abc", Row(3, "mno", 4)), Row("abc", Row(3, "mno", 4)), Row("abc", Row(3, "mno", 4))))
    checkAnswer(sql("select a.d,a.f from table1"),
      Seq(Row(2, Row(3, "mno", 4)), Row(2, Row(3, "mno", 4)), Row(2, Row(3, "mno", 4))))
    checkAnswer(sql("select a.j from table1"), Seq(Row(5), Row(5), Row(5)))
    checkAnswer(sql("select * from table1"),
      Seq(Row(1, Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(2, Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(3, Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5))))
    checkAnswer(sql("select *,a from table1"),
      Seq(Row(1,
        Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5),
        Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(2,
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5),
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(3,
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5),
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5))))
  }

  test("ArrayofArray PushDown") {
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(a array<array<int>>) stored by 'carbondata'")
    sql("insert into test values(1) ")
    sql("select a[0][0] from test").show(false)
  }

  test("Struct and ArrayofArray PushDown") {
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(a array<array<int>>,b struct<c:array<int>>) stored by 'carbondata'")
    sql("insert into test values(1,1) ")
    sql("select b.c[0],a[0][0] from test").show(false)
  }

  test("test structofarray with count(distinct)") {
    sql("DROP TABLE IF EXISTS test")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(
      "create table test(cus_id string, struct_of_array struct<id:int,date:timestamp," +
      "sno:array<int>,sal:array<double>,state:array<string>,date1:array<timestamp>>) stored by " +
      "'carbondata'")
    sql("insert into test values('cus_01','1\0012017/01/01\0011\0022\0012.0\0023.0\001ab\002ac\0012018/01/01')")
    //    sql("select *from test").show(false)
    sql(
      "select struct_of_array.state[0],count(distinct struct_of_array.id) as count_int,count" +
      "(distinct struct_of_array.state[0]) as count_string from test group by struct_of_array" +
      ".state[0]")
      .show(false)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }

  test("test arrayofstruct with count(distinct)") {
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(cus_id string,array_of_struct array<struct<id:int,country:string," +
        "state:string,city:string>>) stored by 'carbondata'")
    sql("insert into test values('cus_01','123\002abc\002mno\002xyz\0011234\002abc1\002mno1\002xyz1')")
    checkAnswer(sql("select array_of_struct.state[0],count(distinct array_of_struct.id[0]) as count_country," +
      "count(distinct array_of_struct.state[0]) as count_city from test group by array_of_struct" +
      ".state[0]"), Seq(Row("mno", 1, 1)))
  }

  test("test struct complex type with filter") {
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(id int,a struct<b:int,c:int>) stored by 'carbondata'")
    sql("insert into test values(1,'2\0013')")
    sql("insert into test values(3,'5\0013')")
    sql("insert into test values(2,'4\0015')")
    checkAnswer(sql("select a.b from test where id=3"),Seq(Row(5)))
    checkAnswer(sql("select a.b from test where a.c!=3"),Seq(Row(4)))
    checkAnswer(sql("select a.b from test where a.c=3"),Seq(Row(5),Row(2)))
    checkAnswer(sql("select a.b from test where id=1 or !a.c=3"),Seq(Row(4),Row(2)))
    checkAnswer(sql("select a.b from test where id=3 or a.c=3"),Seq(Row(5),Row(2)))
  }

  /* test struct of date*/
  test("test struct complex type with date") {
    var backupdateFormat = CarbonProperties.getInstance().getProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(a struct<b:date>) stored by 'carbondata'")
    sql("insert into test select '1992-02-19' ")
    checkAnswer(sql("select * from test "), Row(Row(java.sql.Date.valueOf("1992-02-19"))))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        backupdateFormat)
  }

  test("test Projection with two struct") {
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(id int,a struct<b:int,c:int>, d struct<e:int,f:int>) stored by 'carbondata'")
    sql("insert into test values(1,'2\0013','3\0012')")
    checkAnswer(sql("select * from test"),Seq(Row(1,Row(2,3),Row(3,2))))
    checkAnswer(sql("select a.b,id,a.c from test"),Seq(Row(2,1,3)))
    checkAnswer(sql("select d.e,d.f from test"),Seq(Row(3,2)))
    checkAnswer(sql("select a.b,d.e,d.f,id,a.c from test"),Seq(Row(2,3,2,1,3)))
    checkAnswer(sql("select a.b,d.e,id,a.c,d.f,a.c from test"), Seq(Row(2, 3, 1, 3, 2, 3)))
    checkAnswer(sql("select a.b,d.e,d.f from test"), Seq(Row(2, 3, 2)))
    checkAnswer(sql("select a.b,a.c,id,d.e,d.f from test"), Seq(Row(2, 3, 1, 3, 2)))
    checkAnswer(sql("select d.e,d.f,id,a.b,a.c from test"), Seq(Row(3, 2, 1, 2, 3)))
    checkAnswer(sql("select d.e,1,d.f from test"), Seq(Row(3, 1, 2)))
    checkAnswer(sql("select d.e,1,d.f,a.b,id,a.c from test"), Seq(Row(3, 1, 2, 2, 1, 3)))
    checkAnswer(sql("select d.e+1,d.f,a.b,d.e,a.c,id from test"), Seq(Row(4,2,2,3,3,1)))
    checkAnswer(sql("select d.f,a.c,a.b,id,a.c,a.b from test"), Seq(Row(2,3,2,1,3,2)))
    checkAnswer(sql("select sum(d.e) from test"), Seq(Row(3)))
    checkAnswer(sql("select d.f,a.c,a.b,id,a.c,a.b,id,1,id,3,d.f from test"), Seq(Row(2,3,2,1,3,2,1,1,1,3,2)))
  }

  test("test project with struct and array") {
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(id int,a struct<b:int,c:int>, d struct<e:int,f:int>,person Struct<detail:array<int>>) stored by 'carbondata'")
    sql("insert into test values(1,'2\0013','3\0012','5\0026\0027\0028')")
    checkAnswer(sql("select * from test"),Seq(Row(1,Row(2,3),Row(3,2),Row(mutable.WrappedArray.make(Array(5,6,7,8))))))
    checkAnswer(sql("select a.b,id,a.c,person.detail[0] from test"),Seq(Row(2,1,3,5)))
    checkAnswer(sql("select a.b,id,a.c,person.detail[0],d.e,d.f,person.detail[1],id from test"),Seq(Row(2,1,3,5,3,2,6,1)))
    checkAnswer(sql("select a.b,id,a.c,person.detail[0],d.e,d.f,person.detail[1],id,1,a.b from test"),Seq(Row(2,1,3,5,3,2,6,1,1,2)))
  }

  test("test block Update for complex datatype") {
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(id int,a struct<b:int,c:int>,d array<int>) stored by 'carbondata'")
    sql("insert into test values(1,'2\0013',4)")
    val structException = intercept[UnsupportedOperationException](
    sql("update test set(a.b)=(4) where id=1").show(false))
    assertResult("Unsupported operation on Complex data type")(structException.getMessage)
    val arrayException = intercept[UnsupportedOperationException](
    sql("update test set(a)=(4) where id=1").show(false))
    assertResult("Unsupported operation on Complex data type")(arrayException.getMessage)
  }

  test("test block partition column") {
    sql("DROP TABLE IF EXISTS test")
    val arrayException = intercept[AnalysisException](
    sql("""
          | CREATE TABLE IF NOT EXISTS test
          | (
          | id Int,
          | vin string,
          | logdate Timestamp,
          | phonenumber Long,
          | country array<string>,
          | salary Int
          | )
          | PARTITIONED BY (area array<string>)
          | STORED BY 'carbondata'
        """.stripMargin))
    assertResult("Cannot use array<string> for partition column;")(arrayException.getMessage)
    sql("DROP TABLE IF EXISTS test")
    val structException = intercept[AnalysisException](
      sql("""
            | CREATE TABLE IF NOT EXISTS test
            | (
            | id Int,
            | vin string,
            | logdate Timestamp,
            | phonenumber Long,
            | country array<string>,
            | salary Int
            | )
            | PARTITIONED BY (area struct<b:int>)
            | STORED BY 'carbondata'
          """.stripMargin)
    )
    assertResult("Cannot use struct<b:int> for partition column;")(structException.getMessage)
  }

  test("test block preaggregate") {
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(id int,a struct<b:int>) stored by 'carbondata'")
    sql("insert into test values (1,2)")
    sql("insert into test values (1,2)")
    sql("insert into test values (1,2)")
    val structException = intercept[UnsupportedOperationException](
      sql("create datamap preagg_sum on table test using 'preaggregate' as select id,sum(a.b) from test group by id"))
    assertResult("Preaggregate is unsupported for ComplexData type column: a.b")(structException.getMessage)
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(id int,a array<int>) stored by 'carbondata'")
    sql("insert into test values (1,2)")
    val arrayException = intercept[UnsupportedOperationException](
      sql("create datamap preagg_sum on table test using 'preaggregate' as select id,sum(a[0]) from test group by id"))
    assertResult("Preaggregate is unsupported for ComplexData type column: a[0]")(arrayException.getMessage)
  }

  test("test block dictionary exclude for child column") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,a struct<b:int,c:string,d:int,e:string,f:struct<g:int," +
      "h:string,i:int>,j:int>) stored " +
      "by " +
      "'carbondata' tblproperties('dictionary_exclude'='a')")
    sql("insert into table1 values(1,'1\001abc\0012\001efg\0013\002mno\0024\0015')")
    checkAnswer(sql("select a.b from table1"), Seq(Row(1)))
    sql("DROP TABLE IF EXISTS table1")
    val structException = intercept[MalformedCarbonCommandException](
    sql(
      "create table table1 (roll int,a struct<b:int,c:string,d:int,e:string,f:struct<g:int," +
      "h:string,i:int>,j:int>) stored " +
      "by " +
      "'carbondata' tblproperties('dictionary_exclude'='a.b')"))
    assertResult(
      "DICTIONARY_EXCLUDE column: a.b does not exist in table or unsupported for complex child " +
      "column. Please check the create table statement.")(
      structException.getMessage)
    sql("DROP TABLE IF EXISTS table1")
    val arrayException = intercept[MalformedCarbonCommandException](
      sql(
        "create table table1 (roll int,a array<int>) stored " +
        "by " +
        "'carbondata' tblproperties('dictionary_exclude'='a[0]')"))
    assertResult(
      "DICTIONARY_EXCLUDE column: a[0] does not exist in table or unsupported for complex child " +
      "column. Please check the create table statement.")(
      arrayException.getMessage)
  }

  test("test block dictionary include for child column") {
    sql("DROP TABLE IF EXISTS table1")
    val structException = intercept[MalformedCarbonCommandException](
      sql(
        "create table table1 (roll int,a struct<b:int,c:string,d:int,e:string,f:struct<g:int," +
        "h:string,i:int>,j:int>) stored " +
        "by " +
        "'carbondata' tblproperties('dictionary_include'='a.b')"))
    assertResult(
      "DICTIONARY_INCLUDE column: a.b does not exist in table or unsupported for complex child " +
      "column. Please check the create table statement.")(
      structException.getMessage)
    sql("DROP TABLE IF EXISTS table1")
    val arrayException = intercept[MalformedCarbonCommandException](
      sql(
        "create table table1 (roll int,a array<int>) stored " +
        "by " +
        "'carbondata' tblproperties('dictionary_include'='a[0]')"))
    assertResult(
      "DICTIONARY_INCLUDE column: a[0] does not exist in table or unsupported for complex child " +
      "column. Please check the create table statement.")(
      arrayException.getMessage)
  }

  test("test complex datatype double for encoding") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (person struct<height:double>) stored by 'carbondata'")
    sql("insert into table1 values('1000000000')")
    checkExistence(sql("select * from table1"),true,"1.0E9")
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (person struct<height:double>) stored by 'carbondata'")
    sql("insert into table1 values('12345678912')")
    checkExistence(sql("select * from table1"),true,"1.2345678912E10")
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (person struct<b:array<double>>) stored by 'carbondata'")
    sql("insert into table1 values('10000000\0022000000000\0022900000000')")
    checkExistence(sql("select * from table1"),true,"2.9E9")
  }

  test("test compaction - auto merge") {
    sql("DROP TABLE IF EXISTS table1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    sql(
      "create table table1 (roll int,person Struct<detail:int,age:string,height:double>) stored " +
      "by 'carbondata'")
    sql(
      "load data inpath '" + resourcesPath +
      "/Struct.csv' into table table1 options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,person','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'='&')")
    sql(
      "load data inpath '" + resourcesPath +
      "/Struct.csv' into table table1 options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,person','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'='&')")
    sql(
      "load data inpath '" + resourcesPath +
      "/Struct.csv' into table table1 options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,person','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'='&')")
    sql(
      "load data inpath '" + resourcesPath +
      "/Struct.csv' into table table1 options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,person','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'='&')")
    checkExistence(sql("show segments for table table1"),true, "Compacted")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
  }

  test("decimal with two level struct type") {
    sql("DROP TABLE IF EXISTS test")
    sql(
      "create table test(id int,a struct<c:struct<d:decimal(20,10)>>) stored by 'carbondata' " +
      "tblproperties('dictionary_include'='a')")
    checkExistence(sql("desc test"),true,"struct<c:struct<d:decimal(20,10)>>")
    checkExistence(sql("describe formatted test"),true,"struct<c:struct<d:decimal(20,10)>>")
    sql("insert into test values(1,'3999.999')")
    checkExistence(sql("select * from test"),true,"3999.9990000000")
  }

  test("test dictionary include for second struct and array column") {
    sql("DROP TABLE IF EXISTS test")
    sql(
      "create table test(id int,a struct<b:int,c:int>, d struct<e:int,f:int>, d1 struct<e1:int," +
      "f1:int>) stored by 'carbondata' tblproperties('dictionary_include'='d1')")
    sql("insert into test values(1,'2\0013','4\0015','6\0017')")
    checkAnswer(sql("select * from test"),Seq(Row(1,Row(2,3),Row(4,5),Row(6,7))))
    sql("DROP TABLE IF EXISTS test")
    sql(
      "create table test(a array<int>, b array<int>) stored by 'carbondata' tblproperties" +
      "('dictionary_include'='b')")
    sql("insert into test values(1,2) ")
    checkAnswer(sql("select b[0] from test"),Seq(Row(2)))
    sql("DROP TABLE IF EXISTS test")
    sql(
      "create table test(intval array<array<int>>,str array<array<string>>, bool " +
      "array<array<boolean>>, sint array<array<short>>, big array<array<bigint>>)  stored by " +
      "'carbondata' tblproperties('dictionary_include'='bool,sint,big')")
    sql("insert into test values(1,'ab',true,22,33)")
    checkExistence(sql("select * from test"), true, "33")
  }

  test("date with struct and array") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT)
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(a struct<b:date>) stored by 'carbondata'")
    val exception1 = intercept[Exception] {
      sql("insert into test select 'a' ")
    }
    assert(exception1.getMessage
      .contains(
        "Data load failed due to bad record: The value with column name a.b and column data type " +
        "DATE is not a valid DATE type.Please enable bad record logger to know the detail reason."))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT)
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(a array<date>) stored by 'carbondata'")
    val exception2 = intercept[Exception] {
      sql("insert into test select 'a' ")
    }
    assert(exception2.getMessage
      .contains(
        "Data load failed due to bad record: The value with column name a.val and column data type " +
        "DATE is not a valid DATE type.Please enable bad record logger to know the detail reason."))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        "MM-dd-yyyy")
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(a struct<d1:date,d2:date>) stored by 'carbondata'")
    sql("insert into test values ('02-18-2012\00112-9-2016')")
    checkAnswer(sql("select * from test "), Row(Row(java.sql.Date.valueOf("2012-02-18"),java.sql.Date.valueOf("2016-12-09"))))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
  }
  test("test null values in primitive data type and select all data types including complex data type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (id int, name string, structField struct<intval:int, stringval:string>) stored by 'carbondata'")
    sql("insert into table1 values(null,'aaa','23\001bb')")
    checkAnswer(sql("select * from table1"),Seq(Row(null,"aaa", Row(23,"bb"))))
    checkAnswer(sql("select id,name,structField.intval,structField.stringval from table1"),Seq(Row(null,"aaa",23,"bb")))
    checkAnswer(sql("select id,name,structField.intval,structField.stringval,name from table1"),Seq(Row(null,"aaa",23,"bb","aaa")))
    checkAnswer(sql("select id,name,structField.intval,name,structField.stringval from table1"),Seq(Row(null,"aaa",23,"aaa","bb")))
  }

}
