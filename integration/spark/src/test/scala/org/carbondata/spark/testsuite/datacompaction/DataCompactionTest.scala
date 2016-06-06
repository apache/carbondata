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
class DataCompactionTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "true")
    sql("drop table if exists  normalcompaction")

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

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/mm/dd")
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "true")
    sql("LOAD DATA fact from '" + csvFilePath1 + "' INTO CUBE normalcompaction PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    )
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "true")
    sql("LOAD DATA fact from '" + csvFilePath2 + "' INTO CUBE normalcompaction  PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    )
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "true")
    System.out
      .println("load merge status is " + CarbonProperties.getInstance()
        .getProperty("carbon.enable.load.merge")
      )
    // compaction will happen here.
    sql("LOAD DATA fact from '" + csvFilePath3 + "' INTO CUBE normalcompaction  PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    )

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
          new CarbonTableIdentifier("default", "normalcompaction")
        )
    )
    // merged segment should not be there
    val segments   = segmentStatusManager.getValidSegments.listOfValidSegments.asScala.toList
    assert(!segments.contains("1"))

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
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
      )
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "false")
  }

}
