package org.carbondata.spark.testsuite.filterexpr

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.scalatest.BeforeAndAfterAll

class NullMeasureValueTestCaseFilter extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql(
      "CREATE TABLE IF NOT EXISTS t3 (ID Int, date Timestamp, country String, name String, " +
        "phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata.format'"
    )
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/mm/dd")
    sql("LOAD DATA LOCAL INPATH './src/test/resources/datawithnullmeasure.csv' into table t3");
  }

  test("select ID from t3 where salary is not null") {
    checkAnswer(
      sql("select ID from t3 where salary is not null"),
      Seq(Row(1.0),Row(4.0)))
  }

  test("select ID from t3 where salary is null") {
    checkAnswer(
      sql("select ID from t3 where salary is null"),
      Seq(Row(2.0),Row(3.0)))
  }

  override def afterAll {
    sql("drop cube t3")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }
}
