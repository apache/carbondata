package com.huawei.datasight.carbon.testsuite.aggquery

import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.Row
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory
import com.huawei.datasight.molap.load.MolapLoaderUtil

/**
 * Test Class for aggregate query on String datatypes
 * @author N00902756
 *
 */
class StringDataTypeTestCase extends QueryTest with BeforeAndAfter {
  
  import org.apache.spark.sql.common.util.CarbonHiveContext.implicits._
  
  before
  {
	  sql("CREATE CUBE stringtypecube DIMENSIONS (empname String, designation String, workgroupcategoryname String, deptname String) OPTIONS (PARTITIONER [PARTITION_COUNT=1])");
	  sql("LOAD DATA fact from './TestData/data.csv' INTO CUBE stringtypecube PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')");
  }
  test("select empname from stringtypecube") {
    checkAnswer(
      sql("select empname from stringtypecube"),
      Seq(Row("arvind"),Row("krithin"),Row("madhan"),Row("anandh"),Row("ayushi"),
          Row("pramod"),Row("gawrav"),Row("sibi"),Row("shivani"),Row("bill")))
  }
  after
  {
	  sql("drop cube stringtypecube")
  }
}