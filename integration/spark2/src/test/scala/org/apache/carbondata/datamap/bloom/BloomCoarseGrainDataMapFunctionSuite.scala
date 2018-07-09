package org.apache.carbondata.datamap.bloom

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.test.Spark2TestQueryExecutor
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
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

  // it seems the CI env will be timeout on this test, just ignore it here
  ignore("test bloom datamap: loading and querying with empty values on index column") {
    sql(s"CREATE TABLE $normalTable(c1 string, c2 int, c3 string) STORED BY 'carbondata'")
    sql(s"CREATE TABLE $bloomDMSampleTable(c1 string, c2 int, c3 string) STORED BY 'carbondata'")
    sql(
      s"""
         | CREATE DATAMAP $dataMapName on table $bloomDMSampleTable
         | using 'bloomfilter'
         | DMPROPERTIES('index_columns'='c1, c2')
       """.stripMargin)

    // load data with empty value
    sql(s"INSERT INTO $normalTable SELECT '', 1, 'xxx'")
    sql(s"INSERT INTO $bloomDMSampleTable SELECT '', 1, 'xxx'")
    sql(s"INSERT INTO $normalTable SELECT '', null, 'xxx'")
    sql(s"INSERT INTO $bloomDMSampleTable SELECT '', null, 'xxx'")

    // query on null fields
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable"),
      sql(s"SELECT * FROM $normalTable"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE c1 = null"),
      sql(s"SELECT * FROM $normalTable WHERE c1 = null"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE c1 = ''"),
      sql(s"SELECT * FROM $normalTable WHERE c1 = ''"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE isNull(c1)"),
      sql(s"SELECT * FROM $normalTable WHERE isNull(c1)"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE isNull(c2)"),
      sql(s"SELECT * FROM $normalTable WHERE isNull(c2)"))
  }

  test("test bloom datamap: querying with longstring index column") {
    sql(s"CREATE TABLE $normalTable(c1 string, c2 int, c3 string) STORED BY 'carbondata' TBLPROPERTIES('long_string_columns'='c3')")
    sql(s"CREATE TABLE $bloomDMSampleTable(c1 string, c2 int, c3 string) STORED BY 'carbondata' TBLPROPERTIES('long_string_columns'='c3')")
    // create datamap on longstring columns
    sql(
      s"""
         | CREATE DATAMAP $dataMapName on table $bloomDMSampleTable
         | using 'bloomfilter'
         | DMPROPERTIES('index_columns'='c3')
       """.stripMargin)

    sql(s"INSERT INTO $normalTable SELECT 'c1v1', 1, 'xxx'")
    sql(s"INSERT INTO $bloomDMSampleTable SELECT 'c1v1', 1, 'xxx'")
    sql(s"INSERT INTO $normalTable SELECT 'c1v1', 1, 'yyy'")
    sql(s"INSERT INTO $bloomDMSampleTable SELECT 'c1v1', 1, 'yyy'")

    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE c3 = 'xxx'"),
      sql(s"SELECT * FROM $normalTable WHERE c3 = 'xxx'"))
  }

  test("test rebuild bloom datamap: index column is integer, dictionary, sort_column") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomDMSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('table_blocksize'='128', 'dictionary_include'='id, name, s1', 'sort_columns'='id')
         |  """.stripMargin)

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

    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='city,id,age,name', 'BLOOM_SIZE'='640000')
      """.stripMargin)

    sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable").show(false)
    checkExistence(sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable"), true, dataMapName)
    checkBasicQuery(dataMapName, bloomDMSampleTable, normalTable)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }

  test("test rebuild bloom datamap: index column is integer, dictionary, not sort_column") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomDMSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('table_blocksize'='128', 'dictionary_include'='id, name, s1', 'sort_columns'='name')
         |  """.stripMargin)

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

    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='city,id,age,name', 'BLOOM_SIZE'='640000')
      """.stripMargin)

    sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable").show(false)
    checkExistence(sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable"), true, dataMapName)
    checkBasicQuery(dataMapName, bloomDMSampleTable, normalTable)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }

  test("test rebuild bloom datamap: index column is integer, sort_column") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomDMSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('table_blocksize'='128', 'dictionary_include'='name, s1', 'sort_columns'='id')
         |  """.stripMargin)

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

    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='city,id,age,name', 'BLOOM_SIZE'='640000')
      """.stripMargin)

    sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable").show(false)
    checkExistence(sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable"), true, dataMapName)
    checkBasicQuery(dataMapName, bloomDMSampleTable, normalTable)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }

  test("test rebuild bloom datamap: index column is float, not dictionary") {
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
         | CREATE TABLE $bloomDMSampleTable(empno string, doj date, salary float)
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
         | LOAD DATA INPATH '$floatCsvPath' INTO TABLE $bloomDMSampleTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable USING 'bloomfilter' DMPROPERTIES (
         | 'INDEX_COLUMNS'='salary')
       """.stripMargin)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE salary='1040.56'").show(false)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE salary='1040'").show(false)
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE salary='1040.56'"),
      sql(s"SELECT * FROM $normalTable WHERE salary='1040.56'"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE salary='1040'"),
      sql(s"SELECT * FROM $normalTable WHERE salary='1040'"))
  }

  test("test rebuild bloom datamap: index column is float, dictionary") {
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
         | CREATE TABLE $bloomDMSampleTable(empno string, doj date, salary float)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('SORT_COLUMNS'='empno', 'DICTIONARY_INCLUDE'='salary')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$floatCsvPath' INTO TABLE $normalTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$floatCsvPath' INTO TABLE $bloomDMSampleTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable USING 'bloomfilter' DMPROPERTIES (
         | 'INDEX_COLUMNS'='salary')
       """.stripMargin)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE salary='1040.56'").show(false)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE salary='1040'").show(false)
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE salary='1040.56'"),
      sql(s"SELECT * FROM $normalTable WHERE salary='1040.56'"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE salary='1040'"),
      sql(s"SELECT * FROM $normalTable WHERE salary='1040'"))
  }

  test("test rebuild bloom datamap: index column is date") {
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
         | CREATE TABLE $bloomDMSampleTable(empno string, doj date, salary float)
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
         | LOAD DATA INPATH '$dateCsvPath' INTO TABLE $bloomDMSampleTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable USING 'bloomfilter' DMPROPERTIES (
         | 'INDEX_COLUMNS'='doj')
       """.stripMargin)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE doj='2016-03-14'").show(false)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE doj='2016-03-15'").show(false)
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE doj='2016-03-14'"),
      sql(s"SELECT * FROM $normalTable WHERE doj='2016-03-14'"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE doj='2016-03-15'"),
      sql(s"SELECT * FROM $normalTable WHERE doj='2016-03-15'"))
  }

  test("test rebuild bloom datamap: index column is date, dictionary, sort_colum") {
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
         | CREATE TABLE $bloomDMSampleTable(empno string, doj date, salary float)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('SORT_COLUMNS'='empno,doj', 'DICTIONARY_INCLUDE'='doj')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$dateCsvPath' INTO TABLE $normalTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$dateCsvPath' INTO TABLE $bloomDMSampleTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable USING 'bloomfilter' DMPROPERTIES (
         | 'INDEX_COLUMNS'='doj')
       """.stripMargin)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE doj='2016-03-14'").show(false)
    sql(s"SELECT * FROM $bloomDMSampleTable WHERE doj='2016-03-15'").show(false)
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE doj='2016-03-14'"),
      sql(s"SELECT * FROM $normalTable WHERE doj='2016-03-14'"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE doj='2016-03-15'"),
      sql(s"SELECT * FROM $normalTable WHERE doj='2016-03-15'"))
  }

  ignore("test rebuild bloom datamap: loading and querying with empty values on index column") {
    sql(s"CREATE TABLE $normalTable(c1 string, c2 int, c3 string) STORED BY 'carbondata'")
    sql(s"CREATE TABLE $bloomDMSampleTable(c1 string, c2 int, c3 string) STORED BY 'carbondata'")

    // load data with empty value
    sql(s"INSERT INTO $normalTable SELECT '', 1, 'xxx'")
    sql(s"INSERT INTO $bloomDMSampleTable SELECT '', 1, 'xxx'")
    sql(s"INSERT INTO $normalTable SELECT '', null, 'xxx'")
    sql(s"INSERT INTO $bloomDMSampleTable SELECT '', null, 'xxx'")

    sql(
      s"""
         | CREATE DATAMAP $dataMapName on table $bloomDMSampleTable
         | using 'bloomfilter'
         | DMPROPERTIES('index_columns'='c1, c2')
       """.stripMargin)

    // query on null fields
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable"),
      sql(s"SELECT * FROM $normalTable"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE c1 = null"),
      sql(s"SELECT * FROM $normalTable WHERE c1 = null"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE c1 = ''"),
      sql(s"SELECT * FROM $normalTable WHERE c1 = ''"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE isNull(c1)"),
      sql(s"SELECT * FROM $normalTable WHERE isNull(c1)"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE isNull(c2)"),
      sql(s"SELECT * FROM $normalTable WHERE isNull(c2)"))
  }

  test("test bloom datamap: deleting & clearning segment will clear datamap files") {
    sql(s"CREATE TABLE $bloomDMSampleTable(c1 string, c2 int, c3 string) STORED BY 'carbondata'")
    sql(
      s"""
         | CREATE DATAMAP $dataMapName on table $bloomDMSampleTable
         | using 'bloomfilter'
         | DMPROPERTIES('index_columns'='c1, c2')
       """.stripMargin)
    sql(s"INSERT INTO $bloomDMSampleTable SELECT 'c1v1', 1, 'c3v1'")
    sql(s"INSERT INTO $bloomDMSampleTable SELECT 'c1v2', 2, 'c3v2'")

    // two segments both has datamap files
    val carbonTable = CarbonEnv.getCarbonTable(Option("default"), bloomDMSampleTable)(Spark2TestQueryExecutor.spark)
    import scala.collection.JavaConverters._
    (0 to 1).foreach { segId =>
      val datamapPath = CarbonTablePath.getDataMapStorePath(carbonTable.getTablePath, segId.toString, dataMapName)
      assert(FileUtils.listFiles(FileUtils.getFile(datamapPath), Array("bloomindex"), true).asScala.nonEmpty)
    }
    // delete and clean the first segment, the corresponding datamap files should be cleaned too
    sql(s"DELETE FROM TABLE $bloomDMSampleTable WHERE SEGMENT.ID IN (0)")
    sql(s"CLEAN FILES FOR TABLE $bloomDMSampleTable")
    var datamapPath = CarbonTablePath.getDataMapStorePath(carbonTable.getTablePath, "0", dataMapName)
    assert(!FileUtils.getFile(datamapPath).exists(), "index file of this segment has been deleted, should not exist")
    datamapPath = CarbonTablePath.getDataMapStorePath(carbonTable.getTablePath, "1", dataMapName)
    assert(FileUtils.listFiles(FileUtils.getFile(datamapPath), Array("bloomindex"), true).asScala.nonEmpty)
  }

  override def afterAll(): Unit = {
    deleteFile(bigFile)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }
}
