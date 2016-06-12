package org.carbondata.spark.testsuite.datacompaction

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.carbondata.lcm.status.SegmentStatusManager
import org.scalatest.BeforeAndAfterAll

import scala.collection.JavaConverters._

/**
  * FT for data compaction scenario.
  */
class DataCompactionCardinalityBoundryTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "true")
    sql("drop table if exists  cardinalityTest")

    sql(
      "CREATE TABLE IF NOT EXISTS cardinalityTest (country String, ID String, date Timestamp, name " +
        "String, " +
        "phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata" +
        ".format'"
    )


    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    var csvFilePath1 = currentDirectory + "/src/test/resources/compaction/compaction1.csv"

    // loading the rows greater than 256. so that the column cardinality crosses byte boundary.
    var csvFilePath2 = currentDirectory + "/src/test/resources/compaction/compactioncard2.csv"

    var csvFilePath3 = currentDirectory + "/src/test/resources/compaction/compaction3.csv"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/mm/dd")
    sql("LOAD DATA fact from '" + csvFilePath1 + "' INTO CUBE cardinalityTest PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    )
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "true")
    sql("LOAD DATA fact from '" + csvFilePath2 + "' INTO CUBE cardinalityTest  PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    )
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "true")
    System.out
      .println("load merge status is " + CarbonProperties.getInstance()
        .getProperty("carbon.enable.load.merge")
      )
    // compaction will happen here.
    sql("LOAD DATA fact from '" + csvFilePath3 + "' INTO CUBE cardinalityTest  PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    )
    // compaction will happen here.
    sql("alter table cardinalityTest compact 'major'"
    )

  }

  test("check if compaction is completed or not and  verify select query.") {
    var status = true
    var noOfRetries = 0
    while (status && noOfRetries < 10) {

      val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(new
          AbsoluteTableIdentifier(
            CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
            new CarbonTableIdentifier("default", "cardinalityTest", "1")
          )
      )
      val segments = segmentStatusManager.getValidSegments().listOfValidSegments.asScala.toList

      if (!segments.contains("0.1")) {
        // wait for 2 seconds for compaction to complete.
        Thread.sleep(500)
        noOfRetries += 1
      }
      else {
        status = false
      }
    }
    // now check the answers it should be same.
    checkAnswer(
      sql("select country,count(*) from cardinalityTest group by country"),
      Seq(Row("america",1),
        Row("canada",1),
        Row("chile",1),
        Row("china",2),
        Row("england",1),
        Row("burma",152),
        Row("butan",101),
        Row("mexico",1),
        Row("newzealand",1),
        Row("westindies",1),
        Row("india",1),
        Row("iran",1),
        Row("iraq",1),
        Row("ireland",1)
      )
    )
  }

  override def afterAll {
    /* sql("drop cube cardinalityTest") */
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
      )
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "false")
  }

}
