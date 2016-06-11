package org.carbondata.spark.testsuite.datacompaction

import java.io.File

import scala.collection.JavaConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.carbondata.lcm.status.SegmentStatusManager
import org.scalatest.BeforeAndAfterAll

/**
  * FT for data compaction scenario.
  */
class DataCompactionNoDictionaryTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists  noDictionaryCompaction")

    sql(
      "CREATE TABLE IF NOT EXISTS noDictionaryCompaction (country String, ID Int, date Timestamp, name " +
        "String, " +
        "phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata" +
        ".format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='country')"
    )

    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath

    var csvFilePath1 = currentDirectory + "/src/test/resources/compaction/compaction1.csv"
    var csvFilePath2 = currentDirectory + "/src/test/resources/compaction/compaction2.csv"
    var csvFilePath3 = currentDirectory + "/src/test/resources/compaction/compaction3.csv"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/mm/dd")
    sql("LOAD DATA fact from '" + csvFilePath1 + "' INTO CUBE noDictionaryCompaction PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    )
    sql("LOAD DATA fact from '" + csvFilePath2 + "' INTO CUBE noDictionaryCompaction  PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    )
    sql("LOAD DATA fact from '" + csvFilePath3 + "' INTO CUBE noDictionaryCompaction  PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    )
    // compaction will happen here.
    sql("alter table noDictionaryCompaction compact 'major'"
    )

    // wait for compaction to finish.
    Thread.sleep(1000)
  }

  // check for 15 seconds if the compacted segments has come or not .
  // if not created after 15 seconds then testcase will fail.

  test("check if compaction is completed or not.") {
    var status = true
    var noOfRetries = 0
    while (status && noOfRetries < 10) {

      val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(new
          AbsoluteTableIdentifier(
            CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
            new CarbonTableIdentifier("default", "noDictionaryCompaction", "1")
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


  test("select country from noDictionaryCompaction") {
    // check answers after compaction.
    checkAnswer(
      sql("select country from noDictionaryCompaction"),
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
   sql("clean files for table noDictionaryCompaction")

    val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(new
        AbsoluteTableIdentifier(
          CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
          new CarbonTableIdentifier("default", "noDictionaryCompaction", "1")
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
      sql("select country from noDictionaryCompaction"),
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
    sql("drop cube noDictionaryCompaction")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
      )
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "false")
  }

}
