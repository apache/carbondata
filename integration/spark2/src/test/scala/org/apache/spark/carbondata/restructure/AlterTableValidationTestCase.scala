package org.apache.spark.carbondata.restructure


import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class AlterTableValidationTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

    sql("drop table if exists restructure")
    // clean data folder
    CarbonProperties.getInstance()
    sql("CREATE TABLE restructure (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE restructure OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""");
  }

  test("test add dictionary column") {
    sql("alter table restructure add columns(dict int) TBLPROPERTIES ('DICTIONARY_INCLUDE'='dict', 'DEFAULT.VALUE.dict'= '9999')")
    checkAnswer(sql("select distinct(dict) from restructure"), Row(9999))
  }
  test("test add no dictionary column") {
    sql("alter table restructure add columns(nodict string) TBLPROPERTIES ('DICTIONARY_EXCLUDE'='nodict', 'DEFAULT.VALUE.NoDict'= 'abcd')")
    checkAnswer(sql("select distinct(nodict) from restructure"), Row("abcd"))
  }
  test("test add timestamp direct dictionary column") {
    sql("alter table restructure add columns(tmpstmp timestamp) TBLPROPERTIES ('DEFAULT.VALUE.tmpstmp'= '17-01-2007')")
    checkAnswer(sql("select distinct(tmpstmp) from restructure"), Row(new java.sql.Timestamp(107,0,17,0,0,0,0)))
    checkExistence(sql("desc restructure"), true, "tmpstmptimestamp")
  }
  test("test add msr column") {
    sql("alter table restructure add columns(msrField decimal(5,2))TBLPROPERTIES ('DEFAULT.VALUE.msrfield'= '12345.11')")
    checkExistence(sql("desc restructure"), true, "msrfielddecimal(5,2)")
  }

  test("test add all datatype supported dictionary column") {
    sql("alter table restructure add columns(strfld string, datefld date, tptfld timestamp, shortFld smallInt, " +
        "intFld int, longFld bigint, dblFld double,dcml decimal(5,4))TBLPROPERTIES" +
        "('DICTIONARY_INCLUDE'='datefld,shortFld,intFld,longFld,dblFld,dcml', 'DEFAULT.VALUE.dblFld'= '12345')")
    checkAnswer(sql("select distinct(dblFld) from restructure"), Row(java.lang.Double.parseDouble("12345")))
    checkExistence(sql("desc restructure"), true, "strfldstring")
    checkExistence(sql("desc restructure"), true, "dateflddate")
    checkExistence(sql("desc restructure"), true, "tptfldtimestamp")
    checkExistence(sql("desc restructure"), true, "shortfldsmallint")
    checkExistence(sql("desc restructure"), true, "intfldint")
    checkExistence(sql("desc restructure"), true, "longfldbigint")
    checkExistence(sql("desc restructure"), true, "dblflddouble")
    checkExistence(sql("desc restructure"), true, "dcmldecimal(5,4)")
  }

  test("test adding existing measure as dimension") {
    sql("alter table restructure add columns(dcmlfld decimal(5,4))")
    try {
      sql("alter table restructure add columns(dcmlfld string)")
      assert(false)
    } catch {
      case e: Exception =>
        println(e.getMessage)
        assert(true)
    }
  }

  test("test adding existing dimension as measure") {
    sql("alter table restructure add columns(dimfld string)")
    try {
      sql("alter table restructure add columns(dimfld decimal(5,4))")
      assert(false)
    } catch {
      case e: Exception =>
        println(e.getMessage)
        assert(true)
    }
  }

  test("test adding existing column again") {
    sql("alter table restructure add columns(dimfld1 string, msrCol double)")
    try {
      sql("alter table restructure add columns(dimfld1 int)TBLPROPERTIES('DICTIONARY_INCLUDE'='dimfld1')")
      assert(false)
    } catch {
      case e: Exception =>
        println(e.getMessage)
        try {
          sql("alter table restructure add columns(msrCol decimal(5,3))")
          assert(false)
        } catch {
          case e: Exception =>
            println(e.getMessage)
            assert(true)
        }
    }
  }

  test("test adding no dictionary column with numeric type") {
    try {
      sql("alter table restructure add columns(dimfld2 double) TBLPROPERTIES('DICTIONARY_EXCLUDE'='dimfld2')")
      assert(false)
    } catch {
      case e: Exception =>
        println(e.getMessage)
        assert(true)
    }
  }
  override def afterAll {
    sql("DROP TABLE IF EXISTS restructure")
  }
}
