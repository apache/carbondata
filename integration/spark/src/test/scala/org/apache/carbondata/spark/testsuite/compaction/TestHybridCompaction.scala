package org.apache.carbondata.spark.testsuite.compaction

import java.io.{BufferedWriter, File, FileWriter}

import scala.collection.mutable.ListBuffer

import au.com.bytecode.opencsv.CSVWriter
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.test.util.QueryTest
import org.junit.Assert
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties


class TestHybridCompaction extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {

  val rootPath = new File(this.getClass.getResource("/").getPath + "../../../..").getCanonicalPath

  val csvPath1 =
    s"$rootPath/integration/spark/src/test/resources/compaction/hybridCompaction1.csv"

  val csvPath2 =
    s"$rootPath/integration/spark/src/test/resources/compaction/hybridCompaction2.csv"

  val tableName = "t1"


  override def beforeAll: Unit = {
    setCarbonProperties("aggregate.columnar.keyblock=true, carbon.allowed.compaction.days=0, carbon.bad.records.action=FORCE, carbon.blocklet.size=120000, carbon.blockletgroup.size.in.mb=64, carbon.column.compressor=snappy, carbon.compaction.level.threshold=4,3, carbon.compaction.prefetch.enable=false, carbon.concurrent.compaction=true, carbon.custom.block.distribution=false, carbon.cutOffTimestamp=2000-12-13 02:10.00, carbon.data.file.version=V3, carbon.date.format=MM/dd/yyyy, carbon.detail.batch.size=100, carbon.dimension.split.value.in.columnar=1, carbon.direct.dictionary=false, carbon.enable.auto.load.merge=false, carbon.enable.bad.record.handling.for.insert=false, carbon.enable.distributed.datamap=false, carbon.enable.page.level.reader.in.compaction=false, carbon.enable.tablestatus.backup=false, carbon.enable.vector.reader=true, carbon.horizontal.compaction.enable=true, carbon.is.columnar.storage=true, carbon.is.fullyfilled.bits=true, carbon.load.directWriteToStorePath.enabled=false, carbon.load.global.sort.partitions=0, carbon.local.dictionary.enable=true, carbon.local.dictionary.size.threshold.inmb=4, carbon.lock.retry.timeout.sec=1, carbon.lock.type=LOCALLOCK, carbon.lucene.index.stop.words=false, carbon.major.compaction.size=1024, carbon.max.driver.lru.cache.size=1024, carbon.max.executor.lru.cache.size=1024, carbon.merge.index.in.segment=true, carbon.minmax.allowed.byte.count=200, carbon.number.of.cores.while.loading=1, carbon.numberof.preserve.segments=0, carbon.prefetch.buffersize=1000, carbon.query.directQueryOnDataMap.enabled=true, carbon.read.partition.hive.direct=true, carbon.skip.empty.line=false, carbon.sort.intermediate.files.limit=20, carbon.sort.size=100000, carbon.sort.storage.inmemory.size.inmb=512, carbon.sort.temp.compressor=SNAPPY, carbon.streaming.segment.max.size=1073741824, carbon.task.distribution=block, carbon.timegranularity=SECOND, carbon.timestamp.format=yyyy-MM-dd HH:mm:ss, carbon.update.check.unique.value=true, carbon.update.persist.enable=true, carbon.update.segment.parallelism=1, carbon.update.sync.folder=/tmp/carbondata, carbon.use.local.dir=true, carbon.writtenby.app.name=SparkTestQueryExecutor, DATA_LOAD_BATCH_SIZE=1000, enable.offheap.sort=true, enable.query.statistics=false, enable.unsafe.columnpage=false, enable.unsafe.in.query.processing=false, enable.unsafe.sort=false, false=false, is.compressed.keyblock=false, is.driver.instance=true, is.int.based.indexer=true, max.query.execution.time=1, send.signal.load=false, zookeeper.enable.lock=false")
    defaultConfig()
    generateCSVFiles()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "MM/dd/yyyy")
  }


  override def afterAll: Unit = {
    deleteCSVFiles()
  }


  override def beforeEach(): Unit = {
    dropTable()
    createTable()
  }


  override def afterEach(): Unit = {
    dropTable()
  }


  def generateCSVFiles(): Unit = {
    val rows1 = new ListBuffer[Array[String]]
    rows1 += Array("seq", "first", "last", "age", "city", "state", "date")
    rows1 += Array("1", "Augusta", "Nichols", "20", "Varasdo", "WA", "07/05/2003")
    rows1 += Array("2", "Luis", "Barnes", "39", "Oroaklim", "MT", "04/05/2048")
    rows1 += Array("3", "Leah", "Guzman", "54", "Culeosa", "KS", "02/23/1983")
    rows1 += Array("4", "Ian", "Ford", "61", "Rufado", "AL", "03/02/1995")
    rows1 += Array("5", "Fanny", "Horton", "37", "Rorlihbem", "CT", "05/12/1987")
    createCSV(rows1, csvPath1)

    val rows2 = new ListBuffer[Array[String]]
    rows2 += Array("seq", "first", "last", "age", "city", "state", "date")
    rows2 += Array("11", "Claudia", "Sullivan", "42", "Dilwuani", "ND", "09/01/2003")
    rows2 += Array("12", "Kate", "Adkins", "54", "Fokafrid", "WA", "10/13/2013")
    rows2 += Array("13", "Eliza", "Lynch", "23", "Bonpige", "ME", "05/02/2015")
    rows2 += Array("14", "Sarah", "Fleming", "60", "Duvugove", "IA", "04/15/2036")
    rows2 += Array("15", "Maude", "Bass", "44", "Ukozedka", "CT", "11/08/1988")
    createCSV(rows2, csvPath2)
  }


  def createCSV(rows: ListBuffer[Array[String]], csvPath: String): Unit = {
    val out = new BufferedWriter(new FileWriter(csvPath))
    val writer: CSVWriter = new CSVWriter(out)

    for (row <- rows) {
      writer.writeNext(row)
    }

    out.close()
    writer.close()
  }


  def deleteCSVFiles(): Unit = {
    try {
      FileUtils.forceDelete(new File(csvPath1))
      FileUtils.forceDelete(new File(csvPath2))
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        Assert.fail(e.getMessage)
    }
  }


  def createTable(): Unit = {
    sql(
      s"""
         | CREATE TABLE $tableName(seq int, first string, last string,
         |   age int, city string, state string, date date)
         | STORED AS carbondata
         | TBLPROPERTIES(
         |   'sort_scope'='local_sort',
         |   'sort_columns'='state, age',
         |   'dateformat'='MM/dd/yyyy')
      """.stripMargin)
  }


  def loadUnsortedData(n : Int = 1): Unit = {
    for(_ <- 1 to n) {
      sql(
        s"""
           | LOAD DATA INPATH '$csvPath1' INTO TABLE $tableName
           | OPTIONS (
           |   'sort_scope'='no_sort')""".stripMargin)
    }
  }


  def loadSortedData(n : Int = 1): Unit = {
    for(_ <- 1 to n) {
      sql(
        s"""
           | LOAD DATA INPATH '$csvPath2' INTO TABLE $tableName
           | OPTIONS (
           |   'sort_scope'='local_sort')""".stripMargin)
    }
  }


  def dropTable(): Unit = {
    sql(s"DROP TABLE IF EXISTS $tableName")
  }


  test("SORTED LOADS") {
    loadSortedData(2)
    sql(s"ALTER TABLE $tableName COMPACT 'major'")

    val out = sql(s"SELECT state, age FROM $tableName").collect()
    out.map(_.get(0).toString) should
    equal(Array("CT", "CT", "IA", "IA", "ME", "ME", "ND", "ND", "WA", "WA"))
  }


  test("UNSORTED LOADS") {
    loadUnsortedData(2)
    sql(s"ALTER TABLE $tableName COMPACT 'major'")

    val out = sql(s"SELECT state, age FROM $tableName").collect()
    out.map(_.get(0).toString) should
    equal(Array("AL", "AL", "CT", "CT", "KS", "KS", "MT", "MT", "WA", "WA"))
  }


  test("MIXED LOADS") {
    loadSortedData()
    loadUnsortedData()
    sql(s"ALTER TABLE $tableName COMPACT 'major'")
    val out = sql(s"SELECT state, age FROM $tableName").collect()
    out.map(_.get(0).toString) should
    equal(Array("AL", "CT", "CT", "IA", "KS", "ME", "MT", "ND", "WA", "WA"))
    out.map(_.get(1).toString) should
    equal(Array("61", "37", "44", "60", "54", "23", "39", "42", "20", "54"))
  }


  test("INSERT") {
    loadSortedData()
    loadUnsortedData()
    sql(
      s"""
         | INSERT INTO $tableName
         | VALUES('20', 'Naman', 'Rastogi', '23', 'Bengaluru', 'ZZ', '12/28/2018')
      """.stripMargin)
    sql(s"ALTER TABLE $tableName COMPACT 'major'")

    val out = sql(s"SELECT state FROM $tableName").collect()
    out.map(_.get(0).toString) should equal(
      Array("AL", "CT", "CT", "IA", "KS", "ME", "MT", "ND", "WA", "WA", "ZZ"))
  }


  test("UPDATE") {
    loadSortedData()
    loadUnsortedData()
    sql(s"UPDATE  $tableName SET (state)=('CT') WHERE seq='13'").collect()
    sql(s"ALTER TABLE $tableName COMPACT 'major'")

    val out = sql(s"SELECT state FROM $tableName WHERE seq='13'").collect()
    out.map(_.get(0).toString) should equal(Array("CT"))
  }

  test("DELETE") {
    loadSortedData()
    loadUnsortedData()
    sql(s"DELETE FROM $tableName WHERE seq='13'").collect()
    sql(s"ALTER TABLE $tableName COMPACT 'major'")

    val out = sql(s"SELECT state FROM $tableName").collect()
    out.map(_.get(0).toString) should equal(
      Array("AL", "CT", "CT", "IA", "KS", "MT", "ND", "WA", "WA"))
  }


  test("RESTRUCTURE TABLE REMOVE COLUMN NOT IN SORT_COLUMNS") {
    loadSortedData()
    loadUnsortedData()
    sql(s"ALTER TABLE $tableName DROP COLUMNS(city)")
    sql(s"ALTER TABLE $tableName COMPACT 'major'")

    val out = sql(s"SELECT age FROM $tableName").collect()
    out.map(_.get(0).toString) should equal(
      Array("61", "37", "44", "60", "54", "23", "39", "42", "20", "54"))
  }


  test("RESTRUCTURE TABLE REMOVE COLUMN IN SORT_COLUMNS") {
    loadSortedData()
    loadUnsortedData()
    sql(s"ALTER TABLE $tableName DROP COLUMNS(state)")
    sql(s"ALTER TABLE $tableName COMPACT 'major'")

    val out = sql(s"SELECT age FROM $tableName").collect()
    out.map(_.get(0).toString) should equal(
      Array("20", "23", "37", "39", "42", "44", "54", "54", "60", "61"))
  }
}
