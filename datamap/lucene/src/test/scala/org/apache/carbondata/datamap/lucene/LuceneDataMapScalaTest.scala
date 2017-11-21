package org.apache.carbondata.datamap.lucene

import java.io.File

import junit.framework.TestCase
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.junit.{AfterClass, BeforeClass, Test}
import org.scalatest.junit.AssertionsForJUnit

class LuceneDataMapScalaTest extends TestCase  with AssertionsForJUnit{


  val rootPath = System.getProperty("java.io.tmpdir") + File.separator + "LuceneDataMapSuite";
  val storePath = rootPath + File.separator + "store"
  val warehouse = rootPath + File.separator + "warehouse"
  val metastoredb = rootPath + File.separator + "metastoredb"

  def deleteDirectory(file: File): Unit = {

    if (!file.exists())
      return

    if (file.isDirectory) {
      val childs = file.listFiles()
      childs.foreach(deleteDirectory)
    } else {
      file.delete();
    }
  }

  var carbonSession: SparkSession = _

  @BeforeClass override def setUp(): Unit = {

    deleteDirectory(new File(rootPath))

    import org.apache.spark.sql.CarbonSession._
    carbonSession = SparkSession
      .builder()
      .master("local")
      .appName("LuceneDataMapScalaTest")
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.driver.host", "localhost")
      .config("hive.metastore.warehouse.dir", metastoredb)
      .getOrCreateCarbonSession(storePath)
  }

  @Test def testLuceneDatamap(): Unit = {

    //carbonSession.sql("drop table if exists test_table")
    //carbonSession
    //  .sql("create table if not exists test_table(id long, name string, city string, age int) stored by 'carbondata'")


    //carbonSession.sql("create datamap lucenedatamap on table test_table using" +
    //" 'org.apache.carbondata.datamap.lucene.LuceneDataMapFactory' " +
    //" dmproperties() as select * from test_table")

    val options = Map("tableName" -> "test_table")
    val rdd = carbonSession.sparkContext.makeRDD(
      Seq((1, "david", "shenzhen", 31),
        (2, "eason", "shenzhen", 27),
        (3, "jarry", "wuhan", 35),
        (4, "simon", "guangzhou", 39)))
    val df = carbonSession.createDataFrame(rdd).toDF("id","name","city","age")

    val dbName = "default"
    val tableName = "test_table"
    val tableIndentifier = AbsoluteTableIdentifier.from(storePath, dbName, tableName)

    carbonSession.sql("drop table if exists test_table")

    val dataMapStoreManager = DataMapStoreManager.getInstance()
    dataMapStoreManager.createAndRegisterDataMap(tableIndentifier,classOf[LuceneDataMapFactory].getName,LuceneDataMap.NAME);

    df.write.format("carbondata").options(options).mode(SaveMode.Overwrite).save


//    val lstTableDataMap = dataMapStoreManager.getDataMap(tableIndentifier, LuceneDataMap.NAME, classOf[LuceneDataMapFactory].getName)

//    assert(lstTableDataMap != null, "list table data map is empty")

//    val Writer = lstTableDataMap.getDataMapFactory.createWriter("0")
//    val luceneWriter = classOf[LuceneDataMapWriter].cast(Writer)
//    val lucenePath = luceneWriter.getIndexPath("0");
//
//    assert(new File(lucenePath).exists(), s"lucene ${lucenePath} data not exists");
//
//    /**
//      *  query lucene data
//      */
//    val indexDir = FSDirectory.open(Paths.get(lucenePath))
//    val indexSearcher = new IndexSearcher(DirectoryReader.open(indexDir))
//    val queryParser = new MultiFieldQueryParser(Array("id","name","city","age"),new StandardAnalyzer())
//    val query = queryParser.parse("*:*")
//    val search = indexSearcher.search(query,1000);
//
//    /**
//      * assert those results
//      */
//    assertResult(4)(search.scoreDocs.length)
//
//    search.scoreDocs.foreach(scoreDoc =>{
//      val doc = indexSearcher.doc(scoreDoc.doc)
//      assertResult(3)(doc.getFields.size())
//    })
//
      val result = carbonSession.sql("select * from test_table where city like 'wuha%'").collect()
      assertResult(1)(result.length)
      assertResult(Row.fromSeq(Seq(3,"jarry","wuhan",35)))(result.apply(0))

  }

  @AfterClass override def tearDown(): Unit = {
    //carbonSession.stop()
    //carbonSession.close()
    //deleteDirectory(new File(rootPath))
  }
}

