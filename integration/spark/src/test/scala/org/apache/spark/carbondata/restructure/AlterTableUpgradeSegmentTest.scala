package org.apache.spark.carbondata.restructure

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.processing.util.CarbonLoaderUtil

class AlterTableUpgradeSegmentTest extends QueryTest with BeforeAndAfterAll {
  override protected def beforeAll(): Unit = {
    sql("drop table if exists altertest")
    sql("create table altertest(a string) STORED AS carbondata")
    sql("insert into altertest select 'k'")
    sql("insert into altertest select 'tttttt'")
  }

  private def removeDataAndIndexSizeFromTableStatus(table: CarbonTable): Unit = {
    val loadMetaDataDetails = SegmentStatusManager.readTableStatusFile(CarbonTablePath
      .getTableStatusFilePath(table.getTablePath))
    loadMetaDataDetails.foreach { loadMetaDataDetail =>
      loadMetaDataDetail.setIndexSize("0")
      loadMetaDataDetail.setDataSize("0")
    }
    SegmentStatusManager.writeLoadDetailsIntoFile(CarbonTablePath
      .getTableStatusFilePath(table.getTablePath), loadMetaDataDetails)
  }

  test("test alter table upgrade segment test") {
    val carbonTable = CarbonEnv.getCarbonTable(TableIdentifier("altertest"))(sqlContext.sparkSession)
    removeDataAndIndexSizeFromTableStatus(carbonTable)
    val loadMetaDataDetails = SegmentStatusManager.readTableStatusFile(CarbonTablePath
      .getTableStatusFilePath(carbonTable.getTablePath))
    loadMetaDataDetails.foreach(detail => assert(detail.getIndexSize.toInt + detail.getDataSize
      .toInt == 0))
    sql("alter table altertest compact 'upgrade_segment'")
    val loadMetaDataDetailsNew = SegmentStatusManager.readTableStatusFile(CarbonTablePath
      .getTableStatusFilePath(carbonTable.getTablePath))
    loadMetaDataDetailsNew.foreach{detail =>
      assert(detail.getIndexSize.toInt != 0)
      assert(detail.getDataSize.toInt != 0)}
  }

  override protected def afterAll(): Unit = {
    sql("drop table if exists altertest")
  }
}
