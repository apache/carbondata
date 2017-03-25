package org.apache.carbondata.spark.testsuite.dataload

import java.io.{File, FilenameFilter}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.reader.CarbonIndexFileReader
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestDataLoadWithFileName extends QueryTest with BeforeAndAfterAll {
  var originVersion = ""

  override def beforeAll() {
    originVersion =
      CarbonProperties.getInstance.getProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION)
  }

  test("Check the file_name in carbonindex with v1 format") {
    CarbonProperties.getInstance.addProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION, "1")
    sql("DROP TABLE IF EXISTS test_table_v1")
    sql(
      """
        | CREATE TABLE test_table_v1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    val testData = s"$resourcesPath/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table test_table_v1")
    val indexReader = new CarbonIndexFileReader()
    val carbonIndexPaths = new File(s"$storeLocation/default/test_table_v1/Fact/Part0/Segment_0/")
      .listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = {
          name.endsWith(CarbonTablePath.getCarbonIndexExtension)
        }
      })
    for (carbonIndexPath <- carbonIndexPaths) {
      indexReader.openThriftReader(carbonIndexPath.getCanonicalPath)
      assert(indexReader.readIndexHeader().getVersion === 1)
      while (indexReader.hasNext) {
        val readBlockIndexInfo = indexReader.readBlockIndexInfo()
        assert(readBlockIndexInfo.getFile_name.startsWith(CarbonTablePath.getCarbonDataPrefix))
        assert(readBlockIndexInfo.getFile_name.endsWith(CarbonTablePath.getCarbonDataExtension))
      }
    }
  }

  test("Check the file_name in carbonindex with v2 format") {
    CarbonProperties.getInstance.addProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION, "2")
    sql("DROP TABLE IF EXISTS test_table_v2")
    sql(
      """
        | CREATE TABLE test_table_v2(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    val testData = s"$resourcesPath/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table test_table_v2")
    val indexReader = new CarbonIndexFileReader()
    val carbonIndexPaths = new File(s"$storeLocation/default/test_table_v2/Fact/Part0/Segment_0/")
      .listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = {
          name.endsWith(CarbonTablePath.getCarbonIndexExtension)
        }
      })
    for (carbonIndexPath <- carbonIndexPaths) {
      indexReader.openThriftReader(carbonIndexPath.getCanonicalPath)
      assert(indexReader.readIndexHeader().getVersion === 2)
      while (indexReader.hasNext) {
        val readBlockIndexInfo = indexReader.readBlockIndexInfo()
        assert(readBlockIndexInfo.getFile_name.startsWith(CarbonTablePath.getCarbonDataPrefix))
        assert(readBlockIndexInfo.getFile_name.endsWith(CarbonTablePath.getCarbonDataExtension))
      }
    }
  }

  test("Check the file_name in carbonindex with v3 format") {
    CarbonProperties.getInstance.addProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION, "3")
    sql("DROP TABLE IF EXISTS test_table_v3")
    sql(
      """
        | CREATE TABLE test_table_v3(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    val testData = s"$resourcesPath/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table test_table_v3")
    val indexReader = new CarbonIndexFileReader()
    val carbonIndexPaths = new File(s"$storeLocation/default/test_table_v3/Fact/Part0/Segment_0/")
      .listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = {
          name.endsWith(CarbonTablePath.getCarbonIndexExtension)
        }
      })
    for (carbonIndexPath <- carbonIndexPaths) {
      indexReader.openThriftReader(carbonIndexPath.getCanonicalPath)
      assert(indexReader.readIndexHeader().getVersion === 3)
      while (indexReader.hasNext) {
        val readBlockIndexInfo = indexReader.readBlockIndexInfo()
        assert(readBlockIndexInfo.getFile_name.startsWith(CarbonTablePath.getCarbonDataPrefix))
        assert(readBlockIndexInfo.getFile_name.endsWith(CarbonTablePath.getCarbonDataExtension))
      }
    }
  }

  override protected def afterAll() {
    sql("DROP TABLE IF EXISTS test_table_v1")
    sql("DROP TABLE IF EXISTS test_table_v2")
    sql("DROP TABLE IF EXISTS test_table_v3")
    CarbonProperties.getInstance.addProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION,
      originVersion)
  }
}
