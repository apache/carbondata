package org.carbondata.spark.testsuite.datacompaction

import java.io.File

import scala.collection.JavaConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.carbondata.lcm.status.SegmentStatusManager

/**
  * FT for data compaction scenario.
  */
class DataCompactionTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "true")
    sql("drop table if exists  normalcompaction")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "mm/dd/yyyy")
    sql(
      "CREATE TABLE IF NOT EXISTS normalcompaction (country String, ID Int, date Timestamp, name " +
        "String, " +
        "phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata" +
        ".format'"
    )


    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    var csvFilePath1 = currentDirectory + "/src/test/resources/compaction/compaction1.csv"

    var csvFilePath2 = currentDirectory + "/src/test/resources/compaction/compaction2.csv"
    var csvFilePath3 = currentDirectory + "/src/test/resources/compaction/compaction3.csv"

    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE normalcompaction OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "true")
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE normalcompaction  OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "true")
    System.out
      .println("load merge status is " + CarbonProperties.getInstance()
        .getProperty("carbon.enable.load.merge")
      )
    // compaction will happen here.
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath3 + "' INTO TABLE normalcompaction  OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    // compaction will happen here.
    sql("alter table normalcompaction compact 'major'"
    )

  }

  test("check if compaction is completed or not.") {
    var status = true
    var noOfRetries = 0
    while (status && noOfRetries < 10) {

      val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(new
          AbsoluteTableIdentifier(
            CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
            new CarbonTableIdentifier("default", "normalcompaction", "1")
          )
      )
      val segments = segmentStatusManager.getValidSegments().listOfValidSegments.asScala.toList

      if (!segments.contains("0.1")) {
        // wait for 2 seconds for compaction to complete.
        Thread.sleep(2000)
        noOfRetries += 1
      }
      else {
        status = false
      }
    }
  }


  test("select country from normalcompaction") {
    // check answers after compaction.
    checkAnswer(
      sql("select country from normalcompaction"),
      Seq(Row("america"),
        Row("canada"),
        Row("chile"),
        Row("china"),
        Row("england"),
        Row("burma"),
        Row("butan"),
        Row("mexico"),
        Row("newzealand"),
        Row("westindies"),
        Row("china"),
        Row("india"),
        Row("iran"),
        Row("iraq"),
        Row("ireland")
      )
    )
  }

  test("delete merged folder and execute query") {
    // delete merged segments
    sql("clean files for table normalcompaction")

    val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(new
        AbsoluteTableIdentifier(
          CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
          new CarbonTableIdentifier("default", "normalcompaction", "uniqueid")
        )
    )
    // merged segment should not be there
    val segments   = segmentStatusManager.getValidSegments.listOfValidSegments.asScala.toList
    assert(!segments.contains("0"))
    assert(!segments.contains("1"))
    assert(!segments.contains("2"))
    assert(segments.contains("0.1"))

    // now check the answers it should be same.
    checkAnswer(
      sql("select country from normalcompaction"),
      Seq(Row("america"),
        Row("canada"),
        Row("chile"),
        Row("china"),
        Row("england"),
        Row("burma"),
        Row("butan"),
        Row("mexico"),
        Row("newzealand"),
        Row("westindies"),
        Row("china"),
        Row("india"),
        Row("iran"),
        Row("iraq"),
        Row("ireland")
      )
    )
  }

  override def afterAll {
    /* sql("drop cube normalcompaction") */
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "false")
  }

}
