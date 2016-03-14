package com.huawei.datasight.carbon.testsuite.sortexpr

import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.Row

/**
 * Test Class for sort expression query on Numeric datatypes
 * @author N00902756
 *
 */
class NumericDataTypeTestCase extends QueryTest with BeforeAndAfter {
  
  import org.apache.spark.sql.common.util.CarbonHiveContext.implicits._
  
  before
  {
	  sql("CREATE CUBE doubletype DIMENSIONS (utilization Numeric,salary Numeric) OPTIONS (PARTITIONER [PARTITION_COUNT=1])")
	  sql("LOAD DATA fact from './TestData/data.csv' INTO CUBE doubletype PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')")
  }
  test("select utilization from doubletype") {
    checkAnswer(
      sql("select utilization from doubletype"),
      Seq(Row(96.2),Row(95.1),Row(99.0),Row(92.2),Row(91.5),
          Row(93.0),Row(97.45),Row(98.23),Row(91.678),Row(94.22)))
  }
  after
  {
	  sql("drop cube doubletype")
  }
}