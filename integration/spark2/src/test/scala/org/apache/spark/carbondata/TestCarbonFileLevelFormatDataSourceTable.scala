package org.apache.spark.carbondata

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.Spark2QueryTest
import org.scalatest.BeforeAndAfterAll

class TestCarbonFileLevelFormatDataSourceTable extends Spark2QueryTest with BeforeAndAfterAll {

  test("test CarbonFile format") {
    sql("DROP TABLE IF EXISTS source")
    sql(
      """
        |CREATE TABLE source (id string, value int)
        |USING org.apache.spark.sql.execution.datasources.CarbonFileLevelFormat
        |OPTIONS ('sort_columns'='', 'tableName'='source')
      """.stripMargin)
    sql("INSERT INTO source SELECT 'amy', 1")
    sql("SELECT * FROM source").show()
    checkAnswer(sql("SELECT * FROM source"), Row("amy", 1))
    sql("DROP TABLE IF EXISTS source")
  }

}
