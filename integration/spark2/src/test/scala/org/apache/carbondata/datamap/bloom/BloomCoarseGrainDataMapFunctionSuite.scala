package org.apache.carbondata.datamap.bloom

import java.io.File

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.datamap.bloom.BloomCoarseGrainDataMapTestUtil.deleteFile
import org.apache.carbondata.datamap.bloom.BloomCoarseGrainDataMapTestUtil.createFile
import org.apache.carbondata.datamap.bloom.BloomCoarseGrainDataMapTestUtil.checkBasicQuery

class BloomCoarseGrainDataMapFunctionSuite  extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach {
  val bigFile = s"$resourcesPath/bloom_datamap_function_test_big.csv"
  val normalTable = "carbon_normal"
  val bloomDMSampleTable = "carbon_bloom"
  val dataMapName = "bloom_dm"

  override protected def beforeAll(): Unit = {
    deleteFile(bigFile)
    new File(CarbonProperties.getInstance().getSystemFolderLocation).delete()
    createFile(bigFile, line = 2000)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }

  override def afterEach(): Unit = {
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
  }

  test("test bloom datamap: index column is integer, dictionary, sort_column") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata' TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomDMSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata' TBLPROPERTIES('table_blocksize'='128', 'dictionary_include'='id', 'sort_columns'='id')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='city,id', 'BLOOM_SIZE'='640000')
      """.stripMargin)

    var map = DataMapStatusManager.readDataMapStatusMap()
    assert(map.get(dataMapName).isEnabled)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $normalTable
         | OPTIONS('header'='false')
         """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $bloomDMSampleTable
         | OPTIONS('header'='false')
         """.stripMargin)

    map = DataMapStatusManager.readDataMapStatusMap()
    assert(map.get(dataMapName).isEnabled)

    sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable").show(false)
    checkExistence(sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable"), true, dataMapName)
    sql(s"select * from $bloomDMSampleTable where id = 1").show(false)
    sql(s"select * from $bloomDMSampleTable where city = 'city_1'").show(false)
    checkBasicQuery(dataMapName, bloomDMSampleTable, normalTable)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }

  test("test bloom datamap: index column is integer, dictionary, not sort_column") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata' TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomDMSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata' TBLPROPERTIES('table_blocksize'='128', 'dictionary_include'='id', 'sort_columns'='name')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='city,id', 'BLOOM_SIZE'='640000')
      """.stripMargin)

    var map = DataMapStatusManager.readDataMapStatusMap()
    assert(map.get(dataMapName).isEnabled)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $normalTable
         | OPTIONS('header'='false')
         """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $bloomDMSampleTable
         | OPTIONS('header'='false')
         """.stripMargin)

    map = DataMapStatusManager.readDataMapStatusMap()
    assert(map.get(dataMapName).isEnabled)

    sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable").show(false)
    checkExistence(sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable"), true, dataMapName)
    sql(s"select * from $bloomDMSampleTable where id = 1").show(false)
    sql(s"select * from $bloomDMSampleTable where city = 'city_1'").show(false)
    checkBasicQuery(dataMapName, bloomDMSampleTable, normalTable)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }

  test("test bloom datamap: index column is integer, sort_column") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata' TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomDMSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata' TBLPROPERTIES('table_blocksize'='128', 'sort_columns'='id')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='city,id', 'BLOOM_SIZE'='640000')
      """.stripMargin)

    var map = DataMapStatusManager.readDataMapStatusMap()
    assert(map.get(dataMapName).isEnabled)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $normalTable
         | OPTIONS('header'='false')
         """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $bloomDMSampleTable
         | OPTIONS('header'='false')
         """.stripMargin)

    map = DataMapStatusManager.readDataMapStatusMap()
    assert(map.get(dataMapName).isEnabled)

    sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable").show(false)
    checkExistence(sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable"), true, dataMapName)
    sql(s"select * from $bloomDMSampleTable where id = 1").show(false)
    sql(s"select * from $bloomDMSampleTable where city = 'city_1'").show(false)
    checkBasicQuery(dataMapName, bloomDMSampleTable, normalTable)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }

  test("test bloom datamap: index column is float, not dictionary") {
    val floatCsvPath = s"$resourcesPath/datasamplefordate.csv"
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
    sql(
      s"""
         | CREATE TABLE $normalTable(empno string, doj date, salary float)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$floatCsvPath' INTO TABLE $normalTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomDMSampleTable(empno string, doj date, salary float)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable USING 'bloomfilter' DMPROPERTIES (
         | 'INDEX_COLUMNS'='salary')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$floatCsvPath' INTO TABLE $bloomDMSampleTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE salary='1040.56'").show(false)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE salary='1040'").show(false)
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE salary='1040.56'"),
      sql(s"SELECT * FROM $normalTable WHERE salary='1040.56'"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE salary='1040'"),
      sql(s"SELECT * FROM $normalTable WHERE salary='1040'"))
  }

  test("test bloom datamap: index column is float, dictionary") {
    val floatCsvPath = s"$resourcesPath/datasamplefordate.csv"
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
    sql(
      s"""
         | CREATE TABLE $normalTable(empno string, doj date, salary float)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$floatCsvPath' INTO TABLE $normalTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomDMSampleTable(empno string, doj date, salary float)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('SORT_COLUMNS'='empno', 'dictionary_include'='salary')
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable USING 'bloomfilter' DMPROPERTIES (
         | 'INDEX_COLUMNS'='salary')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$floatCsvPath' INTO TABLE $bloomDMSampleTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE salary='1040.56'").show(false)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE salary='1040'").show(false)
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE salary='1040.56'"),
      sql(s"SELECT * FROM $normalTable WHERE salary='1040.56'"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE salary='1040'"),
      sql(s"SELECT * FROM $normalTable WHERE salary='1040'"))
  }

  // since float cannot be sort_columns, we skip the test case

  test("test bloom datamap: index column is date") {
    val dateCsvPath = s"$resourcesPath/datasamplefordate.csv"
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
    sql(
      s"""
         | CREATE TABLE $normalTable(empno string, doj date, salary float)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$dateCsvPath' INTO TABLE $normalTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomDMSampleTable(empno string, doj date, salary float)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable USING 'bloomfilter' DMPROPERTIES (
         | 'INDEX_COLUMNS'='doj')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$dateCsvPath' INTO TABLE $bloomDMSampleTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE doj='2016-03-14'").show(false)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE doj='2016-03-15'").show(false)
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE doj='2016-03-14'"),
      sql(s"SELECT * FROM $normalTable WHERE doj='2016-03-14'"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE doj='2016-03-15'"),
      sql(s"SELECT * FROM $normalTable WHERE doj='2016-03-15'"))
  }

  test("test bloom datamap: index column is date, dictionary, sort column") {
    val dateCsvPath = s"$resourcesPath/datasamplefordate.csv"
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
    sql(
      s"""
         | CREATE TABLE $normalTable(empno string, doj date, salary float)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$dateCsvPath' INTO TABLE $normalTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomDMSampleTable(empno string, doj date, salary float)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('SORT_COLUMNS'='empno', 'dictionary_include'='doj', 'sort_columns'='doj')
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable USING 'bloomfilter' DMPROPERTIES (
         | 'INDEX_COLUMNS'='doj')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$dateCsvPath' INTO TABLE $bloomDMSampleTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE doj='2016-03-14'").show(false)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE doj='2016-03-15'").show(false)
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE doj='2016-03-14'"),
      sql(s"SELECT * FROM $normalTable WHERE doj='2016-03-14'"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE doj='2016-03-15'"),
      sql(s"SELECT * FROM $normalTable WHERE doj='2016-03-15'"))
  }

  // since date cannot be dictionary_exclude, we skip the test case

  // timestamp is naturally not dictionary
  test("test bloom datamap: index column is timestamp") {
    val timeStampData = s"$resourcesPath/timeStampFormatData1.csv"
    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS $normalTable (
         | ID Int, date date, starttime Timestamp, country String, name String, phonetype String, serialname String, salary Int)
         | STORED BY 'carbondata'
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$timeStampData' into table $normalTable
         | OPTIONS('dateformat' = 'yyyy/MM/dd','timestampformat'='yyyy-MM-dd HH:mm:ss')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS $bloomDMSampleTable (
         | ID Int, date date, starttime Timestamp, country String, name String, phonetype String, serialname String, salary Int)
         | STORED BY 'carbondata'
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable USING 'bloomfilter' DMPROPERTIES (
         | 'INDEX_COLUMNS'='starttime')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$timeStampData' into table $bloomDMSampleTable
         | OPTIONS('dateformat' = 'yyyy/MM/dd','timestampformat'='yyyy-MM-dd HH:mm:ss')
       """.stripMargin)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE starttime='2016-07-25 01:03:30.0'").show(false)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE starttime='2016-07-25 01:03:31.0'").show(false)
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE starttime='2016-07-25 01:03:30.0'"),
      sql(s"SELECT * FROM $normalTable WHERE starttime='2016-07-25 01:03:30.0'"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE starttime='2016-07-25 01:03:31.0'"),
      sql(s"SELECT * FROM $normalTable WHERE starttime='2016-07-25 01:03:31.0'"))
  }

  test("test bloom datamap: index column is timestamp, dictionary, sort_column") {
    val timeStampData = s"$resourcesPath/timeStampFormatData1.csv"
    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS $normalTable (
         | ID Int, date date, starttime Timestamp, country String, name String, phonetype String, serialname String, salary Int)
         | STORED BY 'carbondata'
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$timeStampData' into table $normalTable
         | OPTIONS('dateformat' = 'yyyy/MM/dd','timestampformat'='yyyy-MM-dd HH:mm:ss')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS $bloomDMSampleTable (
         | ID Int, date date, starttime Timestamp, country String, name String, phonetype String, serialname String, salary Int)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('dictionary_column'='starttime', 'sort_columns'='starttime')
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable USING 'bloomfilter' DMPROPERTIES (
         | 'INDEX_COLUMNS'='starttime')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$timeStampData' into table $bloomDMSampleTable
         | OPTIONS('dateformat' = 'yyyy/MM/dd','timestampformat'='yyyy-MM-dd HH:mm:ss')
       """.stripMargin)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE starttime=null").show(false)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE starttime='2016-07-25 01:03:30.0'").show(false)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE starttime='2016-07-25 01:03:31.0'").show(false)
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE starttime='2016-07-25 01:03:30.0'"),
      sql(s"SELECT * FROM $normalTable WHERE starttime='2016-07-25 01:03:30.0'"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE starttime='2016-07-25 01:03:31.0'"),
      sql(s"SELECT * FROM $normalTable WHERE starttime='2016-07-25 01:03:31.0'"))
  }

  override def afterAll(): Unit = {
    deleteFile(bigFile)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }
}
