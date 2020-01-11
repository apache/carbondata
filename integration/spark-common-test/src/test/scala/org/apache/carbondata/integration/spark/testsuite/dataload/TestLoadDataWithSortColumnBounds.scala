/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.integration.spark.testsuite.dataload

import java.io.{File, FileOutputStream, OutputStreamWriter, Serializable}

import scala.util.Random

import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.scalatest.{BeforeAndAfterAll, Ignore}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

case class SortColumnBoundRow (id: Int, date: String, country: String, name: String,
    phoneType: String, serialName: String, salary: Int) extends Serializable

object TestLoadDataWithSortColumnBounds {
  def generateOneRow(id : Int): SortColumnBoundRow = {
    SortColumnBoundRow(id,
      "2015/7/23",
      s"country$id",
      s"name$id",
      s"phone${new Random().nextInt(10000)}",
      s"ASD${new Random().nextInt(10000)}",
      10000 + id)
  }
}

class TestLoadDataWithSortColumnBounds extends QueryTest with BeforeAndAfterAll {
  private val tableName: String = "test_table_with_sort_column_bounds"
  private val filePath: String = s"$resourcesPath/source_for_sort_column_bounds.csv"
  private var df: DataFrame = _

  private val dateFormatStr: String = "yyyy/MM/dd"
  private val totalLineNum = 2000

  private val originDateStatus: String = CarbonProperties.getInstance().getProperty(
    CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
    CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)


  override def beforeAll(): Unit = {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, dateFormatStr)
    sql(s"DROP TABLE IF EXISTS $tableName")
    // sort column bounds work only with local_sort
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, "local_sort")
    prepareDataFile()
    prepareDataFrame()
  }

  override def afterAll(): Unit = {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, originDateStatus)
    sql(s"DROP TABLE IF EXISTS $tableName")
    new File(filePath).delete()
    df = null
    // sort column bounds work only with local_sort
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
        CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
  }

  /**
   * generate loading files based on source.csv but can have more lines
   */
  private def prepareDataFile(): Unit = {
    val file = new File(filePath)

    val sb: StringBuilder = new StringBuilder
    def generateLine(id : Int): String = {
      sb.clear()
      val row = TestLoadDataWithSortColumnBounds.generateOneRow(id)
      sb.append(row.id).append(',')
        .append(row.date).append(',')
        .append(row.country).append(',')
        .append(row.name).append(',')
        .append(row.phoneType).append(',')
        .append(row.serialName).append(',')
        .append(row.salary)
        .append(System.lineSeparator())
        .toString()
    }

    val outputStream = new FileOutputStream(file)
    val writer = new OutputStreamWriter(outputStream)
    for (i <- 1 to totalLineNum) {
      writer.write(generateLine(i))
    }

    writer.flush()
    writer.close()
    outputStream.flush()
    outputStream.close()
  }

  /**
   * prepare data frame
   */
  private def prepareDataFrame(): Unit = {
    import sqlContext.implicits._
    df = sqlContext.sparkSession.sparkContext.parallelize(1 to totalLineNum)
      .map(id => {
        val row = TestLoadDataWithSortColumnBounds.generateOneRow(id)
        (row.id, row.date, row.country, row.name, row.phoneType, row.serialName, row.salary)
      })
      .toDF("ID", "date", "country", "name", "phoneType", "serialName", "salary")
  }

  test("load data with sort column bounds: safe mode") {
    val originStatus = CarbonProperties.getInstance().getProperty(
      CarbonCommonConstants.ENABLE_UNSAFE_SORT, CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "false")
    sql(s"DROP TABLE IF EXISTS $tableName")

    sql(s"CREATE TABLE $tableName (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata " +
        "tblproperties('sort_columns'='ID,name')")
    // load with 4 bounds
    sql(s"LOAD DATA INPATH '$filePath' INTO TABLE $tableName " +
        s" OPTIONS('fileheader'='ID,date,country,name,phonetype,serialname,salary'," +
        s" 'sort_column_bounds'='400,aab1;800,aab1;1200,aab1;1600,aab1')")

    checkAnswer(sql(s"select count(*) from $tableName"), Row(totalLineNum))
    checkAnswer(sql(s"select count(*) from $tableName where ID > 1001"), Row(totalLineNum - 1001))

    sql(s"DROP TABLE IF EXISTS $tableName")
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.ENABLE_UNSAFE_SORT, originStatus)
  }

  test("load data with sort column bounds: unsafe mode") {
    val originStatus = CarbonProperties.getInstance().getProperty(
      CarbonCommonConstants.ENABLE_UNSAFE_SORT, CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "true")
    sql(s"DROP TABLE IF EXISTS $tableName")

    sql(s"CREATE TABLE $tableName (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata " +
        "tblproperties('sort_columns'='ID,name')")
    // load with 4 bounds
    sql(s"LOAD DATA INPATH '$filePath' INTO TABLE $tableName " +
        s" OPTIONS('fileheader'='ID,date,country,name,phonetype,serialname,salary'," +
        s" 'sort_column_bounds'='400,aab1;800,aab1;1200,aab1;1600,aab1')")

    checkAnswer(sql(s"select count(*) from $tableName"), Row(totalLineNum))
    checkAnswer(sql(s"select count(*) from $tableName where ID > 1001"), Row(totalLineNum - 1001))

    sql(s"DROP TABLE IF EXISTS $tableName")
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.ENABLE_UNSAFE_SORT, originStatus)
  }

  test("load data with sort column bounds: empty column value in bounds is treated as null") {
    sql(s"DROP TABLE IF EXISTS $tableName")

    sql(s"CREATE TABLE $tableName (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata " +
        "tblproperties('sort_columns'='ID,name')")
    // bounds have empty value
    sql(s"LOAD DATA INPATH '$filePath' INTO TABLE $tableName " +
        s" OPTIONS('fileheader'='ID,date,country,name,phonetype,serialname,salary'," +
        s" 'sort_column_bounds'='200,aab1;,aab1')")

    checkAnswer(sql(s"select count(*) from $tableName"), Row(totalLineNum))
    checkAnswer(sql(s"select count(*) from $tableName where ID > 1001"), Row(totalLineNum - 1001))

    sql(s"DROP TABLE IF EXISTS $tableName")
  }

  test("load data with sort column bounds: sort column bounds will be ignored if it is empty.") {
    sql(s"DROP TABLE IF EXISTS $tableName")

    sql(s"CREATE TABLE $tableName (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata " +
        "tblproperties('sort_columns'='ID,name')")
    sql(s"LOAD DATA INPATH '$filePath' INTO TABLE $tableName " +
        s" OPTIONS('fileheader'='ID,date,country,name,phonetype,serialname,salary'," +
        s" 'sort_column_bounds'='')")

    checkAnswer(sql(s"select count(*) from $tableName"), Row(totalLineNum))
    checkAnswer(sql(s"select count(*) from $tableName where ID > 1001"), Row(totalLineNum - 1001))

    sql(s"DROP TABLE IF EXISTS $tableName")
  }

  test("load data with sort column bounds: number of column value in bounds should match that of sort column") {
    sql(s"DROP TABLE IF EXISTS $tableName")

    sql(s"CREATE TABLE $tableName (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata " +
        "tblproperties('sort_columns'='ID,name')")
    val e = intercept[Exception] {
      // number of column value does not match that of sort columns
      sql(s"LOAD DATA INPATH '$filePath' INTO TABLE $tableName " +
          s" OPTIONS('fileheader'='ID,date,country,name,phonetype,serialname,salary'," +
          s" 'sort_column_bounds'='400,aab1;800')")
    }

    assert(e.getMessage.contains(
      "The number of field in bounds should be equal to that in sort columns." +
      " Expected 2, actual 1." +
      " The illegal bound is '800'"))

    val e2 = intercept[Exception] {
      // number of column value does not match that of sort columns
      sql(s"LOAD DATA INPATH '$filePath' INTO TABLE $tableName " +
          s" OPTIONS('fileheader'='ID,date,country,name,phonetype,serialname,salary'," +
          s" 'sort_column_bounds'='400,aab1;800,aab1,def')")
    }

    assert(e2.getMessage.contains(
      "The number of field in bounds should be equal to that in sort columns." +
      " Expected 2, actual 3." +
      " The illegal bound is '800,aab1,def'"))

    sql(s"DROP TABLE IF EXISTS $tableName")
  }

  test("load data with sort column bounds: sort column bounds will be ignored if not using local_sort") {
    sql(s"DROP TABLE IF EXISTS $tableName")

    sql(s"CREATE TABLE $tableName (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata " +
        "tblproperties('sort_columns'='ID,name','sort_scope'='global_sort')")
    // since the sort_scope is 'global_sort', we will ignore the sort column bounds,
    // so the error in sort_column bounds will not be thrown
    sql(s"LOAD DATA INPATH '$filePath' INTO TABLE $tableName " +
        s" OPTIONS('fileheader'='ID,date,country,name,phonetype,serialname,salary'," +
        s" 'sort_column_bounds'='400,aab,extra_field')")

    checkAnswer(sql(s"select count(*) from $tableName"), Row(totalLineNum))
    checkAnswer(sql(s"select count(*) from $tableName where ID > 1001"), Row(totalLineNum - 1001))

    sql(s"DROP TABLE IF EXISTS $tableName")
  }

  // now default sort scope is no_sort, hence all dimesions are not sorted by default
  ignore("load data with sort column bounds: no sort columns explicitly specified" +
       " means all dimension columns will be sort columns, so bounds should be set correctly") {
    sql(s"DROP TABLE IF EXISTS $tableName")

    sql(s"CREATE TABLE $tableName (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata")
    // the sort_columns will have 5 columns if we don't specify it explicitly
    val e = intercept[Exception] {
      sql(s"LOAD DATA INPATH '$filePath' INTO TABLE $tableName " +
          s" OPTIONS('fileheader'='ID,date,country,name,phonetype,serialname,salary'," +
          s" 'sort_column_bounds'='400,aab')")
    }
    assert(e.getMessage.contains(
      "The number of field in bounds should be equal to that in sort columns." +
      " Expected 5, actual 2." +
      " The illegal bound is '400,aab'"))

    sql(s"DROP TABLE IF EXISTS $tableName")
  }

  test("load data with sort column bounds: sort column is global dictionary encoded") {
    sql(s"DROP TABLE IF EXISTS $tableName")

    sql(s"CREATE TABLE $tableName (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata " +
        "tblproperties('sort_columns'='ID,name')")
    // ID is sort column and dictionary column. Since the actual order and literal order of
    // this column are not necessarily the same, this will not cause error but will cause data skewed.
    sql(s"LOAD DATA INPATH '$filePath' INTO TABLE $tableName " +
        s" OPTIONS('fileheader'='ID,date,country,name,phonetype,serialname,salary'," +
        s" 'sort_column_bounds'='400,name400;800,name800;1200,name1200;1600,name1600')")

    checkAnswer(sql(s"select count(*) from $tableName"), Row(totalLineNum))
    checkAnswer(sql(s"select count(*) from $tableName where ID > 1001"), Row(totalLineNum - 1001))

    sql(s"DROP TABLE IF EXISTS $tableName")
  }

  test("load data with sort column bounds: sort column is global dictionary encoded" +
       " but bounds are not in dictionary") {
    sql(s"DROP TABLE IF EXISTS $tableName")

    sql(s"CREATE TABLE $tableName (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata " +
        "tblproperties('sort_columns'='name,ID')")
    // 'name' is sort column and dictionary column, but value for 'name' in bounds does not exists
    // in dictionary. It will not cause error but will cause data skewed.
    sql(s"LOAD DATA INPATH '$filePath' INTO TABLE $tableName " +
        s" OPTIONS('fileheader'='ID,date,country,name,phonetype,serialname,salary'," +
        s" 'sort_column_bounds'='nmm400,400;nme800,800;nme1200,1200;nme1600,1600')")

    checkAnswer(sql(s"select count(*) from $tableName"), Row(totalLineNum))
    checkAnswer(sql(s"select count(*) from $tableName where ID > 1001"), Row(totalLineNum - 1001))

    sql(s"DROP TABLE IF EXISTS $tableName")
  }

  test("load data frame with sort column bounds") {
    sql(s"DROP TABLE IF EXISTS $tableName")

    df.write
      .format("carbondata")
      .option("tableName", tableName)
      .option("tempCSV", "false")
      .option("sort_columns", "ID,name")
      .option("sort_column_bounds", "600,aab1;1200,aab1")
      .mode(SaveMode.Overwrite)
      .save()

    sql(s"select count(*) from $tableName").show()
    sql(s"select count(*) from $tableName where ID > 1001").show()

    checkAnswer(sql(s"select count(*) from $tableName"), Row(totalLineNum))
    checkAnswer(sql(s"select count(*) from $tableName where ID > 1001"), Row(totalLineNum - 1001))

    sql(s"DROP TABLE IF EXISTS $tableName")
  }

  test("load data frame with sort column bounds: number of column value in bounds should match that of sort column") {
    sql(s"DROP TABLE IF EXISTS $tableName")

    val e = intercept[Exception] {
      df.write
        .format("carbondata")
        .option("tableName", tableName)
        .option("tempCSV", "false")
        .option("sort_columns", "ID,name")
        .option("sort_column_bounds", "600,aab1;1200,aab1,def")
        .mode(SaveMode.Overwrite)
        .save()
    }
    assert(e.getMessage.contains(
      "The number of field in bounds should be equal to that in sort columns." +
      " Expected 2, actual 3." +
      " The illegal bound is '1200,aab1,def'"))

    sql(s"DROP TABLE IF EXISTS $tableName")
  }
}