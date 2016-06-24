package org.carbondata.spark.testsuite.datacompaction

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.carbondata.core.carbon.path.{CarbonStorePath, CarbonTablePath}
import org.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.locks.{CarbonLockFactory, ICarbonLock, LockUsage}
import org.carbondata.core.util.CarbonProperties
import org.carbondata.lcm.status.SegmentStatusManager
import org.scalatest.BeforeAndAfterAll

import scala.collection.JavaConverters._

/**
  * FT for data compaction Locking scenario.
  */
class DataCompactionLockTest extends QueryTest with BeforeAndAfterAll {

  val absoluteTableIdentifier: AbsoluteTableIdentifier = new
      AbsoluteTableIdentifier(
        CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
        new CarbonTableIdentifier("default", "compactionLockTestTable", "1")
      )
  val carbonTablePath: CarbonTablePath = CarbonStorePath
    .getCarbonTablePath(absoluteTableIdentifier.getStorePath,
      absoluteTableIdentifier.getCarbonTableIdentifier
    )
  val dataPath: String = carbonTablePath.getMetadataDirectoryPath

  val carbonLock: ICarbonLock =
    CarbonLockFactory.getCarbonLockObj(dataPath, LockUsage.TABLE_STATUS_LOCK)


  override def beforeAll {
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "true")
    sql("drop table if exists  compactionLockTestTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "mm/dd/yyyy")
    sql(
      "CREATE TABLE IF NOT EXISTS compactionLockTestTable (country String, ID Int, date " +
        "Timestamp, name " +
        "String, " +
        "phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata" +
        ".format'"
    )

    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    var csvFilePath1 = currentDirectory + "/src/test/resources/compaction/compaction1.csv"

    var csvFilePath2 = currentDirectory + "/src/test/resources/compaction/compaction2.csv"
    var csvFilePath3 = currentDirectory + "/src/test/resources/compaction/compaction3.csv"

    sql("LOAD DATA fact from '" + csvFilePath1 + "' INTO CUBE compactionLockTestTable " +
      "PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    )
    sql("LOAD DATA fact from '" + csvFilePath2 + "' INTO CUBE compactionLockTestTable  " +
      "PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    )
    sql("LOAD DATA fact from '" + csvFilePath3 + "' INTO CUBE compactionLockTestTable  " +
      "PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    )
    // take the lock so that next compaction will be failed.
    carbonLock.lockWithRetries()

    // compaction should happen here.
    sql("alter table compactionLockTestTable compact 'major'"
    )
  }

  /**
    * Compaction should fail as lock is being held purposefully
    */
  test("check if compaction is failed or not.") {
    var status = true
    var noOfRetries = 0
    while (status && noOfRetries < 10) {

      val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(
        absoluteTableIdentifier
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
    assert(status)
  }


  override def afterAll {
    /* sql("drop cube compactionLockTestTable") */
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    carbonLock.unlock()
  }

}
