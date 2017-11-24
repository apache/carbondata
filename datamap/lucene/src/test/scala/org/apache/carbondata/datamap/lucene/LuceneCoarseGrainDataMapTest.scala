
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.datamap.lucene.LuceneCoarseGrainDataMapFactory
import org.apache.spark.sql.test.util.QueryTest
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LuceneCoarseGrainDataMapTest extends QueryTest with BeforeAndAfterAll {

  val file2 = resourcesPath + "/compaction/fil2.csv"

  override protected def beforeAll(): Unit = {
    //n should be about 5000000 of reset if size is default 1024
    val n = 15000
    LuceneFineGrainDataMapTest.createFile(file2, n * 4, n)
    sql("DROP TABLE IF EXISTS normal_test")
    sql(
      """
        | CREATE TABLE normal_test(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE normal_test OPTIONS('header'='false')")
  }

  test("test lucene coarse grain data map") {
    sql("DROP TABLE IF EXISTS datamap_test")
    sql(
      """
        | CREATE TABLE datamap_test(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    val table = CarbonMetadata.getInstance().getCarbonTable("default_datamap_test")

    val LuceneCGName = "LuceneCoarseGrainDatamap";

    val luceneCGTableDataMap =
      DataMapStoreManager.getInstance().getDataMap(table.getAbsoluteTableIdentifier,
        LuceneCGName,
        classOf[LuceneCoarseGrainDataMapFactory].getName);


    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test OPTIONS('header'='false')")


    checkAnswer(sql("select * from datamap_test where name='n502670'"),
      sql("select * from normal_test where name='n502670'"))
  }

  override protected def afterAll(): Unit = {
    LuceneFineGrainDataMapTest.deleteFile(file2)
    sql("DROP TABLE IF EXISTS normal_test")
    sql("DROP TABLE IF EXISTS datamap_test")
  }
}

