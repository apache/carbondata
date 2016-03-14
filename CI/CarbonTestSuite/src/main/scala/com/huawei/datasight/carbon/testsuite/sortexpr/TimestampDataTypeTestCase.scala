package com.huawei.datasight.carbon.testsuite.sortexpr

import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.Row
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory
import com.huawei.datasight.molap.load.MolapLoaderUtil
import java.sql.Timestamp

/**
 * Test Class for sort expression query on timestamp datatypes
 * @author N00902756
 *
 */
class TimestampDataTypeTestCase extends QueryTest with BeforeAndAfter {
  
  import org.apache.spark.sql.common.util.CarbonHiveContext.implicits._
  
  before
  {
	  sql("CREATE CUBE timestamptypecube DIMENSIONS (doj Timestamp, projectjoindate Timestamp, projectenddate Timestamp) OPTIONS (PARTITIONER [PARTITION_COUNT=1])");
	  sql("LOAD DATA fact from './TestData/data.csv' INTO CUBE timestamptypecube PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')");
  }
  test("select doj from timestamptypecube") {
    checkAnswer(
      sql("select doj from timestamptypecube"),
      Seq(Row(Timestamp.valueOf("2007-01-17 00:00:00.0")),
          Row(Timestamp.valueOf("2008-05-29 00:00:00.0")),
          Row(Timestamp.valueOf("2009-07-07 00:00:00.0")),
          Row(Timestamp.valueOf("2010-12-29 00:00:00.0")),
          Row(Timestamp.valueOf("2011-11-09 00:00:00.0")),
          Row(Timestamp.valueOf("2012-10-14 00:00:00.0")),
          Row(Timestamp.valueOf("2013-09-22 00:00:00.0")),
          Row(Timestamp.valueOf("2014-08-15 00:00:00.0")),
          Row(Timestamp.valueOf("2015-05-12 00:00:00.0")),
          Row(Timestamp.valueOf("2015-12-01 00:00:00.0"))))
  }
  after
  {
	  sql("drop cube timestamptypecube")
  }
}