package com.huawei.datasight.carbon.testsuite.aggquery

import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.Row

/**
 * Test Class for aggregate query on Integer datatypes
 * @author N00902756
 *
 */
class IntegerDataTypeTestCase extends QueryTest with BeforeAndAfter {
  
  import org.apache.spark.sql.common.util.CarbonHiveContext.implicits._
  
  before
  {
	  sql("CREATE CUBE integertypecube DIMENSIONS (empno Integer, workgroupcategory Integer, deptno Integer, projectcode Integer) MEASURES (attendance Integer) OPTIONS (PARTITIONER [PARTITION_COUNT=1])")
	  sql("LOAD DATA fact from './TestData/data.csv' INTO CUBE integertypecube PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')")
  }
  test("select empno from integertypecube") {
    checkAnswer(
      sql("select empno from integertypecube"),
      Seq(Row(11),Row(12),Row(13),Row(14),Row(15),Row(16),Row(17),Row(18),Row(19),Row(20)))
  }
  after
  {
	  sql("drop cube integertypecube")
  }
}