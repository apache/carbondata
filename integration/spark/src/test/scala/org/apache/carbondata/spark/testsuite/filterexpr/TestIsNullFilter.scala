package org.apache.carbondata.spark.testsuite.filterexpr

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestIsNullFilter extends QueryTest with BeforeAndAfterAll {
  override def beforeAll: Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table if exists main")
    sql("create table main(id int, name string, time timestamp) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/newsample.csv' into table main OPTIONS('bad_records_action'='force')")
  }

  test("select * from main where time is null") {
    checkAnswer(
      sql("select count(*) from main where time is null"),
      Seq(Row(1)))
  }

  override def afterAll: Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table if exists main")
  }

}
