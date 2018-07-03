package org.apache.carbondata.integration.spark.testsuite.complexType

import java.sql.Timestamp

import scala.collection.mutable

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test class of testing projection with complex data type
 *
 */

class TestComplexDataType extends QueryTest with BeforeAndAfterAll {

  test("test Projection PushDown for Struct - Integer type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll string,person Struct<detail:int>) stored by " +
      "'carbondata'")
    sql("insert into table1 values('abc',1)")
    sql("select roll,person,roll,person.detail from table1").show(false)
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
    sql("insert into table1 values('abc','1$2$3')")
    sql("select roll,person,roll,person from table1").show(false)
    checkAnswer(sql("select roll,person from table1"),
      Seq(Row("abc", mutable.WrappedArray.make(Array(1, 2, 3)))))
  }

  test("test Projection PushDown for StructofArray - Integer type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:array<int>>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,'1:2')")
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
    sql("insert into table1 values(1,'abc:bcd')")
    //    sql("select person from table1").show(false)
    sql("select person.detail[0] from table1").show(false)
    checkAnswer(sql("select person.detail[0] from table1"), Seq(Row("abc")))
    checkAnswer(sql("select person.detail[1] from table1"), Seq(Row("bcd")))
    checkAnswer(sql("select roll,person from table1"),
      Seq(Row(1, Row(mutable.WrappedArray.make(Array("abc", "bcd"))))))
    checkAnswer(sql("select roll,person.detail[0],person,person.detail[1] from table1"),
      Seq(Row(1, "abc", Row(mutable.WrappedArray.make(Array("abc", "bcd"))), "bcd")))
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
    sql("insert into table1 values(1,'10.00:20.00')")
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
    sql("insert into table1 values(1,'3.4:4.2')")
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
    sql("select person,roll,person.detail from table1").show(false)
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
    sql("insert into table1 select 1,'2018/01/01:2017/01/01'")
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
    sql("insert into table1 values(1,'2018888:2018889')")
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
    sql("select person,person.detail from table1").show(false)
    checkAnswer(sql("select person,roll,person.detail from table1"), Seq(Row(Row(20), 1, 20)))
    checkExistence(sql("select person.detail from table1"), true, "20")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(20))))
  }

  test("test Projection PushDown for StructofArray - short type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:array<short>>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,'20:30')")
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
    sql("select person,person.detail from table1").show(false)
    checkAnswer(sql("select person,roll,person.detail from table1"), Seq(Row(Row(true), 1, true)))
    checkExistence(sql("select person.detail from table1"), true, "true")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(true))))
  }

  test("test Projection PushDown for StructofArray - boolean type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:array<boolean>>) stored by " +
      "'carbondata'")
    sql("insert into table1 values(1,'true:false')")
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
    sql("insert into table1 values ('abc$2')")
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
    sql("insert into table1 values('abc','1$abc','2$cde')")
    sql("select person.detail,person1.age from table1").show(false)
  }

  test("test Projection PushDown for more than one Struct column Cases -1") {
    sql("drop table if exists test")
    sql("create table test (a struct<b:int, c:struct<d:int,e:int>>) stored by 'carbondata'")
    sql("insert into test select '1$2:3'")
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
    sql("insert into table1 values(1,'1$abc$2$efg$3:mno:4$5')")
    sql("insert into table1 values(2,'1$abc$2$efg$3:mno:4$5')")
    sql("insert into table1 values(3,'1$abc$2$efg$3:mno:4$5')")
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
    sql("insert into table1 values(1,'1$abc$2$efg$3:mno:4$5')")
    sql("insert into table1 values(2,'1$abc$2$efg$3:mno:4$5')")
    sql("insert into table1 values(3,'1$abc$2$efg$3:mno:4$5')")

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

  test("ArrayofArray PushDown")
  {
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(a array<array<int>>) stored by 'carbondata'")
    sql("insert into test values(1) ")
    sql("select a[0][0] from test").show(false)
  }

  test("Struct and ArrayofArray PushDown")
  {
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
    sql("insert into test values('cus_01','1$2017/01/01$1:2$2.0:3.0$ab:ac$2018/01/01')")
    sql("select *from test").show(false)
    sql(
      "select struct_of_array.state[0],count(distinct struct_of_array.id) as count_int,count" +
      "(distinct struct_of_array.state[0]) as count_string from test group by struct_of_array" +
      ".state[0]")
      .show(false)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }

}
