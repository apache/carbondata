package org.apache.carbondata.spark.testsuite.localdictionary

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

class LocalDictionarySupportCreateTableTest extends QueryTest with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS LOCAL1")
  }

  test("test local dictionary default configuration") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    val desc_result = sql("describe formatted local1")

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test local dictionary custom configurations for local dict columns _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_include'='name')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test(
    "test local dictionary custom configurations for local dict columns _002")
  {
    sql("drop table if exists local1")

    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_include'='name,name')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations for local dict columns _003") {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_include'='')
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column:  does not exist in table. " +
        "Please check " +
        "create table statement"))
  }

  test("test local dictionary custom configurations for local dict columns _004") {
    sql("drop table if exists local1")
    val exception1 = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_include'='abc')
        """.
          stripMargin)
    }
    assert(exception1.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: abc does not exist in table. " +
        "Please check " +
        "create table " +
        "statement"))
  }

  test("test local dictionary custom configurations for local dict columns _005") {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_include'='id')
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: id is not a String/complex " +
        "datatype column. " +
        "LOCAL_DICTIONARY_COLUMN should " +
        "be no dictionary string/complex datatype column"))
  }

  test("test local dictionary custom configurations for local dict columns _006") {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('dictionary_include'='name','local_dictionary_include'='name')
        """.
          stripMargin)
    }
  }

  test("test local dictionary custom configurations for local dict threshold _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_threshold'='20000')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
    }
  }

  test("test local dictionary custom configurations for local dict threshold _002")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_threshold'='-100')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test local dictionary custom configurations for local dict threshold _003")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_threshold'='21474874811')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test local dictionary custom configurations for local dict threshold _004")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_threshold'='')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test local dictionary custom configurations for local dict threshold _005")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_threshold'='hello')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_001")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_threshold'='20000','local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_002")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_threshold'='-100','local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_003")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_threshold'='','local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_004")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_threshold'='vdslv','local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_005") {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_threshold'='20000','local_dictionary_include'='name,
          | name')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_006")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_threshold'='20000','local_dictionary_include'=' ')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_007")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_threshold'='20000','local_dictionary_include'='hello')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_008")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_threshold'='20000','local_dictionary_include'='name',
          | 'dictionary_include'='name')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_009")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_threshold'='','local_dictionary_include'='name,name')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_010")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_threshold'='-100','local_dictionary_include'='Hello')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured " +
       "_011")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_threshold'='23213497321591234324',
          | 'local_dictionary_include'='name','dictionary_include'='name')
        """.stripMargin)
    }
  }

  test("test local dictionary default configuration when enabled") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='true')
      """.stripMargin)

    val desc_result = sql("describe formatted local1")

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test local dictionary custom configurations when enabled for local dict columns _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_include'='name','local_dictionary_enable'='true')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test local dictionary custom configurations when enabled for local dict columns _002")
  {
    sql("drop table if exists local1")

    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name,name')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations when enabled for local dict columns _003") {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='')
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column:  does not exist in table. " +
        "Please check " +
        "create table statement"))

  }

  test("test local dictionary custom configurations when enabled for local dict columns _004") {
    sql("drop table if exists local1")
    val exception1 = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='abc')
        """.
          stripMargin)
    }
    assert(exception1.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: abc does not exist in table. " +
        "Please check " +
        "create table " +
        "statement"))
  }

  test("test local dictionary custom configurations when enabled for local dict columns _005") {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='id')
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: id is not a String/complex " +
        "datatype column. " +
        "LOCAL_DICTIONARY_COLUMN should " +
        "be no dictionary string/complex datatype column"))
  }

  test("test local dictionary custom configurations when enabled for local dict columns _006") {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','dictionary_include'='name',
          | 'local_dictionary_include'='name')
        """.
          stripMargin)
    }
  }

  test(
    "test local dictionary custom configurations when local_dictionary_exclude is configured _001")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_enable'='true')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test(
    "test local dictionary custom configurations when local_dictionary_exclude is configured _002")
  {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_exclude'='name,name')
        """.stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE contains Duplicate Columns: name. " +
        "Please check create table statement."))
  }

  test(
    "test local dictionary custom configurations when local_dictionary_exclude is configured _003")
  {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_exclude'='')
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column:  does not exist in table. " +
        "Please check " +
        "create table statement"))

  }

  test(
    "test local dictionary custom configurations when local_dictionary_exclude is configured _004")
  {
    sql("drop table if exists local1")
    val exception1 = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_exclude'='abc')
        """.
          stripMargin)
    }
    assert(exception1.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: abc does not exist in table. " +
        "Please check " +
        "create table " +
        "statement"))
  }

  test(
    "test local dictionary custom configurations when local_dictionary_exclude is configured _005")
  {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_exclude'='id')
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: id is not a String/complex " +
        "datatype column. " +
        "LOCAL_DICTIONARY_COLUMN should " +
        "be no dictionary string/complex datatype column"))
  }

  test(
    "test local dictionary custom configurations when local_dictionary_exclude is configured _006")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','dictionary_include'='name',
          | 'local_dictionary_exclude'='name')
        """.
          stripMargin)
    }
  }

  test(
    "test local dictionary custom configurations when local_dictionary_include and " +
    "local_dictionary_exclude " +
    "is configured _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city',
        | 'local_dictionary_enable'='true')
      """.
        stripMargin)

    val descFormatted1 = sql("describe formatted local1").collect

    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
    }
  }

  test(
    "test local dictionary custom configurations when local_dictionary_include and " +
    "local_dictionary_exclude " +
    "is configured _002") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int,add string)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city',
        | 'sort_columns'='add',
        | 'local_dictionary_enable'='true')
      """.
        stripMargin)

    val descFormatted1 = sql("describe formatted local1").collect

    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
    }
  }

  test(
    "test local dictionary custom configurations when local_dictionary_include and " +
    "local_dictionary_exclude " +
    "is configured _003") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city',
        | 'local_dictionary_enable'='false')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }

    checkExistence(sql("describe formatted local1"), false, "Local Dictionary Include")
    checkExistence(sql("describe formatted local1"), false, "Local Dictionary Exclude")
  }

  test(
    "test local dictionary custom configurations when local_dictionary_include and " +
    "local_dictionary_exclude " +
    "is configured _004") {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city',
          | 'local_dictionary_enable'='true','dictionary_include'='name,city')
        """.
          stripMargin)
    }
  }

  test(
    "test local dictionary custom configurations when local_dictionary_include and " +
    "local_dictionary_exclude " +
    "is configured _005") {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name,city',
          | 'local_dictionary_exclude'=' NaMe')
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "Column ambiguity as duplicate column(s):name is present in LOCAL_DICTIONARY_INCLUDE and " +
        "LOCAL_DICTIONARY_EXCLUDE. Duplicate columns are not allowed."))
  }

  test(
    "test local dictionary custom configurations when local_dictionary_include and " +
    "local_dictionary_exclude " +
    "is configured _006") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int,st struct<s_id:int,
        | s_name:string,s_city:array<string>>)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city,st',
        | 'local_dictionary_enable'='true')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city,st"))
    }
  }

  test(
    "test local dictionary custom configurations when local_dictionary_include and " +
    "local_dictionary_exclude " +
    "is configured _007") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int,st array<struct<s_id:int,
        | s_name:string>>)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city,st',
        | 'local_dictionary_enable'='true')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city,st"))
    }
  }

  test("test local dictionary custom configurations when enabled for local dict threshold _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
    }
  }

  test("test local dictionary custom configurations when enabled for local dict threshold _002")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='-100')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test local dictionary custom configurations when enabled for local dict threshold _003")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='21474874811')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test local dictionary custom configurations when enabled for local dict threshold _004")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test local dictionary custom configurations when enabled for local dict threshold _005")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='hello')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _001")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000',
        | 'local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _002")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='-100',
        | 'local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _003")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='',
        | 'local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _004")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='vdslv',
        | 'local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _005")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000',
          | 'local_dictionary_include'='name,name')
        """.stripMargin)
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _006")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000',
          | 'local_dictionary_include'=' ')
        """.stripMargin)
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _007")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000',
          | 'local_dictionary_include'='hello')
        """.stripMargin)
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _008")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000',
          | 'local_dictionary_include'='name','dictionary_include'='name')
        """.stripMargin)
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _009")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='',
          | 'local_dictionary_include'='name,name')
        """.stripMargin)
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _010")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='-100',
          | 'local_dictionary_include'='Hello')
        """.stripMargin)
    }
  }

  test(
    "test local dictionary custom configurations when enabled with both columns and threshold " +
    "configured _011")
  {
    sql("drop table if exists local1")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true',
          | 'local_dictionary_threshold'='23213497321591234324','local_dictionary_include'='name',
          | 'dictionary_include'='name')
        """.stripMargin)
    }
  }

  test("test local dictionary default configuration when disabled") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)

    val desc_result = sql("describe formatted local1")

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict columns _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_include'='name','local_dictionary_enable'='false')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict columns _002")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_include'='name,name')
      """.stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict columns _003") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_include'='')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict columns _004") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_include'='abc')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict columns _005") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_include'='id')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict columns _006") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','dictionary_include'='name',
        | 'local_dictionary_include'='name')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict threshold _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='20000')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict threshold _002")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='-100')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict threshold _003") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false',
        | 'local_dictionary_threshold'='21474874811')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict threshold _004")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict threshold _005")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='hello')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _001")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='20000',
        | 'local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _002")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='-100',
        | 'local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _003")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='',
        | 'local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _004")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='vdslv',
        | 'local_dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _005")
  {
    sql("drop table if exists local1")

    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='20000',
        | 'local_dictionary_include'='name,name')
      """.stripMargin)
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _006")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='20000',
        | 'local_dictionary_include'=' ')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _007")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='20000',
        | 'local_dictionary_include'='hello')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _008")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='20000',
        | 'local_dictionary_include'='name','dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _009")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='',
        | 'local_dictionary_include'='name,name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _010")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false','local_dictionary_threshold'='-100',
        | 'local_dictionary_include'='Hello')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled with both columns and threshold " +
    "configured _011")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='false',
        | 'local_dictionary_threshold'='23213497321591234324','local_dictionary_include'='name',
        | 'dictionary_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect

    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configuration with other table properties _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('dictionary_include'='city','sort_scope'='global_sort',
        | 'sort_columns'='city,name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect

    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("SORT_SCOPE")) match {
      case Some(row) => assert(row.get(1).toString.contains("global_sort"))
    }
  }

  test("test local dictionary custom configuration with other table properties _002") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('dictionary_include'='city','sort_scope'='batch_sort',
        | 'sort_columns'='city,name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect

    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("SORT_SCOPE")) match {
      case Some(row) => assert(row.get(1).toString.contains("batch_sort"))
    }
  }
  test("test local dictionary custom configuration with other table properties _003") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('dictionary_include'='city','sort_scope'='no_sort',
        | 'sort_columns'='city,name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect

    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("SORT_SCOPE")) match {
      case Some(row) => assert(row.get(1).toString.contains("no_sort"))
    }
  }
  test("test local dictionary custom configuration with other table properties _004") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('dictionary_include'='city','sort_scope'='local_sort',
        | 'sort_columns'='city,name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect

    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("SORT_SCOPE")) match {
      case Some(row) => assert(row.get(1).toString.contains("local_sort"))
    }
  }

  test("test CTAS statements for local dictionary default configuration when enabled") {
    sql("drop table if exists local")
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    sql(
      """
        | create table local1 stored by 'carbondata' tblproperties
        | ('local_dictionary_enable'='true') as
        | select * from local
      """.stripMargin)

    val desc_result = sql("describe formatted local1")

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test(
    "test CTAS statements for local dictionary custom configurations when enabled for local dict " +
    "columns _001")
  {
    sql("drop table if exists local")
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_include'='name','local_dictionary_enable'='true')
        | as select * from local
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test(
    "test CTAS statements for local dictionary custom configurations when enabled for local dict " +
    "columns _002")
  {
    sql("drop table if exists local")
    sql("drop table if exists local1")

    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name,name')
          | as select * from local
        """.stripMargin)
    }
  }

  test(
    "test CTAS statements for local dictionary custom configurations when enabled for local dict " +
    "columns _003")
  {
    sql("drop table if exists local")
    sql("drop table if exists local1")

    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='')
          | as select * from local
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column:  does not exist in table. " +
        "Please check " +
        "create table statement"))

  }

  test(
    "test CTAS statements for local dictionary custom configurations when enabled for local dict " +
    "columns _004")
  {
    sql("drop table if exists local")
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    val exception1 = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='abc')
          | as select * from local
        """.
          stripMargin)
    }
    assert(exception1.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: abc does not exist in table. " +
        "Please check " +
        "create table " +
        "statement"))
  }

  test(
    "test CTAS statements for local dictionary custom configurations when enabled for local dict " +
    "columns _005")
  {
    sql("drop table if exists local")
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='id')
          | as select * from local
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: id is not a String/complex " +
        "datatype column. " +
        "LOCAL_DICTIONARY_COLUMN should " +
        "be no dictionary string/complex datatype column"))
  }

  test(
    "test CTAS statements for local dictionary custom configurations when enabled for local dict " +
    "columns _006")
  {
    sql("drop table if exists local")
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','dictionary_include'='name',
          | 'local_dictionary_include'='name') as select * from local
        """.
          stripMargin)
    }
  }

  test(
    "test CTAS statements for local dictionary custom configurations when " +
    "local_dictionary_exclude is configured _001")
  {
    sql("drop table if exists local")
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_enable'='true')
        | as select * from local
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test(
    "test CTAS statements for local dictionary custom configurations when " +
    "local_dictionary_exclude is configured _002")
  {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_exclude'='name,name')
          | as select * from local
        """.stripMargin)
    }
  }

  test(
    "test CTAS statements for local dictionary custom configurations when " +
    "local_dictionary_exclude is configured _003")
  {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_exclude'='')
          | as select * from local
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column:  does not exist in table. " +
        "Please check " +
        "create table statement"))
  }

  test(
    "test CTAS statements for local dictionary custom configurations when " +
    "local_dictionary_exclude is configured _004")
  {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    val exception1 = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_exclude'='abc')
          | as select * from local
        """.
          stripMargin)
    }
    assert(exception1.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: abc does not exist in table. " +
        "Please check " +
        "create table " +
        "statement"))
  }

  test(
    "test CTAS statements for local dictionary custom configurations when " +
    "local_dictionary_exclude is configured _005")
  {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_exclude'='id')
          | as select * from local
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: id is not a String/complex " +
        "datatype column. " +
        "LOCAL_DICTIONARY_COLUMN should " +
        "be no dictionary string/complex datatype column"))
  }

  test(
    "test CTAS statements for local dictionary custom configurations when " +
    "local_dictionary_exclude is configured _006")
  {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','dictionary_include'='name',
          | 'local_dictionary_exclude'='name') as select * from local
        """.
          stripMargin)
    }
  }

  test(
    "test CTAS statements for local dictionary custom configurations when " +
    "local_dictionary_include and local_dictionary_exclude " +
    "is configured _001") {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city',
        | 'local_dictionary_enable'='true') as select * from local
      """.
        stripMargin)

    val descFormatted1 = sql("describe formatted local1").collect

    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
    }
  }

  test(
    "test CTAS statements for local dictionary custom configurations when " +
    "local_dictionary_include and local_dictionary_exclude " +
    "is configured _002") {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city',
        | 'local_dictionary_enable'='false') as select * from local
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }

    checkExistence(sql("describe formatted local1"), false, "Local Dictionary Include")
    checkExistence(sql("describe formatted local1"), false, "Local Dictionary Exclude")
  }

  test(
    "test CTAS statements for local dictionary custom configurations when " +
    "local_dictionary_include and local_dictionary_exclude " +
    "is configured _003") {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city',
          | 'local_dictionary_enable'='true','dictionary_include'='name,city') as select * from
          | local
        """.
          stripMargin)
    }
  }

  test(
    "test CTAS statements for local dictionary custom configurations when " +
    "local_dictionary_include and local_dictionary_exclude " +
    "is configured _004") {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name,city',
          | 'local_dictionary_exclude'='name') as select * from local
        """.
          stripMargin)
    }
  }

  test(
    "test CTAS statements for local dictionary custom configurations when " +
    "local_dictionary_include and local_dictionary_exclude " +
    "is configured _005") {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int,st struct<s_id:int,
        | s_name:string,s_city:array<string>>)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='true')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_exclude'='name','local_dictionary_include'='city,st',
        | 'local_dictionary_enable'='false') as select * from local
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test CTAS statements for local dictionary custom configurations when enabled for local dict " +
    "threshold _001")
  {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000')
        | as select * from local
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
    }
  }

  test(
    "test CTAS statements for local dictionary custom configurations when enabled for local dict " +
    "threshold _002")
  {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='-100')
        | as select * from local
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test(
    "test CTAS statements for local dictionary custom configurations when enabled for local dict " +
    "threshold _003")
  {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_threshold'='23589714365172595')
        | as select * from local
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test(
    "test CTAS statements for local dictionary custom configurations when first table is hive " +
    "table")
  {
    sql("drop table if exists local1")
    sql("drop table if exists local")
    sql(
      """
        | CREATE TABLE local(id int, name string, city string, age int)
        |  tblproperties('local_dictionary_enable'='false')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE local1 STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='20000',
        | 'local_dictionary_include'='city')
        | as select * from local
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
    }
  }

  test("test no inverted index for local dictionary custom configurations") {
    sql("drop table if exists local1")

    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_threshold'='20000','local_dictionary_include'='city',
        | 'no_inverted_index'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
    }
  }

  test(
    "test local dictionary custom configurations when complex dataType columns are given in " +
    "local_dictionary_include _001") {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string,city array<int>, st array<struct<i:int,s:int>>)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name,st')
        """.stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "None of the child columns specified in the complex dataType column(s) in " +
        "local_dictionary_include are not of string dataType."))
  }

  test(
    "test local dictionary custom configurations when complex dataType columns are given in " +
    "local_dictionary_include _002") {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string,city array<int>, st string)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name,st,
          | city')
        """.stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "None of the child columns specified in the complex dataType column(s) in " +
        "local_dictionary_include are not of string dataType."))
  }

  test(
    "test local dictionary custom configurations when complex dataType columns are given in " +
    "local_dictionary_include _003")
  {
    sql("drop table if exists local1")
      sql(
        """
          | CREATE TABLE local1(id int, name string,city array<int>, st struct<i:int,s:string>)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name,st')
        """.stripMargin)
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name,st"))
    }
  }

  test(
    "test local dictionary custom configurations when complex dataType columns are given in " +
    "local_dictionary_include _004")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string,city array<string>, st struct<i:int,s:int>)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name,city')
      """.stripMargin)
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name,city"))
    }
  }

  test(
    "test local dictionary custom configurations when complex dataType columns are given in " +
    "local_dictionary_include _005")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string,city array<int>, st struct<i:int,s:array<string>>)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name,st')
      """.stripMargin)
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name,st"))
    }
  }

  test(
    "test local dictionary custom configurations when complex dataType columns are given in " +
    "local_dictionary_include _006") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string,city array<int>, st struct<i:int,
        | s:struct<si:string>>)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name,st')
      """.stripMargin)
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name,st"))
    }
  }

  test(
    "test local dictionary custom configurations when complex dataType columns are given in " +
    "local_dictionary_include _007")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string,city array<int>, st array<struct<si:string>>)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name,st')
      """.stripMargin)
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name,st"))
    }
  }

  test(
    "test local dictionary custom configurations when complex dataType columns are given in " +
    "local_dictionary_include _008") {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string,city array<int>, st struct<i:int,s:string>)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name,st,
          | city')
        """.stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "None of the child columns specified in the complex dataType column(s) in " +
        "local_dictionary_include are not of string dataType."))
  }

  test("test alter table add column") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_threshold'='20000','local_dictionary_include'='city','no_inverted_index'='name')
      """.stripMargin)
    sql("alter table local1 add columns (alt string) tblproperties('local_dictionary_include'='alt')")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city,alt"))
    }
  }

  test("test alter table add column default configs for local dictionary") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_threshold'='20000','no_inverted_index'='name')
      """.stripMargin)
    sql("alter table local1 add columns (alt string)")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name,city,alt"))
    }
  }

  test("test alter table add column where same column is in dictionary include and local dictionary include") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_threshold'='20000','local_dictionary_include'='city','no_inverted_index'='name')
      """.stripMargin)
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        "alter table local1 add columns (alt string) tblproperties('local_dictionary_include'='alt','dictionary_include'='alt')")
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: alt specified in Dictionary " +
        "include. Local Dictionary will not be generated for Dictionary include columns. Please " +
        "check create table statement."))
  }

  test("test alter table add column where duplicate columns present in local dictionary include") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_threshold'='20000','local_dictionary_include'='city','no_inverted_index'='name')
      """.stripMargin)
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        "alter table local1 add columns (alt string) tblproperties('local_dictionary_include'='alt,alt')")
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE contains Duplicate Columns: alt. " +
        "Please check create table statement."))
  }

  test("test alter table add column where duplicate columns present in local dictionary include/exclude")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_threshold'='20000','local_dictionary_include'='city',
        | 'no_inverted_index'='name')
      """.stripMargin)
    val exception1 = intercept[MalformedCarbonCommandException] {
      sql(
        "alter table local1 add columns (alt string) tblproperties" +
        "('local_dictionary_include'='abc')")
    }
    assert(exception1.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: abc does not exist in table. " +
        "Please check create table statement."))
    val exception2 = intercept[MalformedCarbonCommandException] {
      sql(
        "alter table local1 add columns (alt string) tblproperties" +
        "('local_dictionary_exclude'='abc')")
    }
    assert(exception2.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: abc does not exist in table. " +
        "Please check create table statement."))
  }

  test("test alter table add column for datatype validation")
  {
    sql("drop table if exists local1")
    sql(
      """ | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_include'='city', 'no_inverted_index'='name')
      """.stripMargin)
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        "alter table local1 add columns (alt string,abc int) tblproperties" +
        "('local_dictionary_include'='abc')")
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: abc is not a String/complex " +
        "datatype column. LOCAL_DICTIONARY_COLUMN should be no dictionary string/complex datatype" +
        " column.Please check create table statement."))
  }

  test("test alter table add column where duplicate columns are present in local dictionary include and exclude")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_include'='city', 'no_inverted_index'='name')
      """.stripMargin)
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        "alter table local1 add columns (alt string,abc string) tblproperties" +
        "('local_dictionary_include'='abc','local_dictionary_exclude'='alt,abc')")
    }
    assert(exception.getMessage
      .contains(
        "Column ambiguity as duplicate column(s):abc is present in LOCAL_DICTIONARY_INCLUDE " +
        "and LOCAL_DICTIONARY_EXCLUDE. Duplicate columns are not allowed."))
  }

  test("test alter table add column unsupported table property")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_include'='city', 'no_inverted_index'='name')
      """.stripMargin)
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        "alter table local1 add columns (alt string,abc string) tblproperties" +
        "('local_dictionary_enable'='abc')")
    }
    assert(exception.getMessage
      .contains(
        "Unsupported Table property in add column: local_dictionary_enable"))
    val exception1 = intercept[MalformedCarbonCommandException] {
      sql(
        "alter table local1 add columns (alt string,abc string) tblproperties" +
        "('local_dictionary_threshold'='10000')")
    }
    assert(exception1.getMessage
      .contains(
        "Unsupported Table property in add column: local_dictionary_threshold"))
  }

  test("test alter table add column when main table is disabled for local dictionary")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false',
        | 'local_dictionary_include'='city', 'no_inverted_index'='name')
      """.stripMargin)
    sql(
      "alter table local1 add columns (alt string,abc string) tblproperties" +
      "('local_dictionary_include'='abc')")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }

    checkExistence(sql("DESC FORMATTED local1"), false,
      "Local Dictionary Include")
  }

  test("test local dictionary threshold for boundary values") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_threshold'='300000')
      """.stripMargin)
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_threshold'='500')
      """.stripMargin)
    val descLoc1 = sql("describe formatted local1").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test alter table add column for local dictionary include and exclude configs")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='true',
        | 'local_dictionary_include'='city', 'no_inverted_index'='name')
      """.stripMargin)
    sql(
      "alter table local1 add columns (alt string,abc string) tblproperties" +
      "('local_dictionary_include'='abc','local_dictionary_exclude'='alt')")
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city,abc"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("alt"))
    }
  }

  test("test preaggregate table local dictionary enabled table")
  {
    sql("drop table if exists local1")
    sql("CREATE TABLE local1 (id Int, date date, country string, phonetype string, " +
        "serialname String,salary int ) STORED BY 'org.apache.carbondata.format' " +
        "tblproperties('dictionary_include'='country','local_dictionary_enable'='true','local_dictionary_include' = 'phonetype','local_dictionary_exclude' ='serialname')")
    sql("create datamap PreAggCount on table local1 using 'preaggregate' as " +
        "select country,count(salary) as count from local1 group by country")
    val descLoc = sql("describe formatted local1_PreAggCount").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("phonetype"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("serialname"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
  }

  test("test local dictionary foer varchar datatype columns") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dictionary_include'='city',
        | 'LONG_STRING_COLUMNS'='city')
      """.stripMargin)
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("city"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test local dictionary describe formatted only with default configs")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'carbondata'
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name,city"))
    }
  }

  override protected def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS LOCAL1")
  }
}
