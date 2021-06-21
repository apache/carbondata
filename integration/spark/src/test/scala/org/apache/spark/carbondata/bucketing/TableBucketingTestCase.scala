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

package org.apache.spark.carbondata.bucketing

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.exception.ProcessMetaDataException

class TableBucketingTestCase extends QueryTest with BeforeAndAfterAll {

  var threshold: String = _

  override def beforeAll {

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    threshold = SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.defaultValueString
    sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "-1")
    sql("DROP TABLE IF EXISTS t4")
    sql("DROP TABLE IF EXISTS t5")
    sql("DROP TABLE IF EXISTS t6")
    sql("DROP TABLE IF EXISTS t6_")
    sql("DROP TABLE IF EXISTS t7")
    sql("DROP TABLE IF EXISTS t8")
    sql("DROP TABLE IF EXISTS t9")
    sql("DROP TABLE IF EXISTS t10")
    sql("DROP TABLE IF EXISTS t11")
    sql("DROP TABLE IF EXISTS t12")
    sql("DROP TABLE IF EXISTS t13")
    sql("DROP TABLE IF EXISTS t14")
    sql("DROP TABLE IF EXISTS t15")
    sql("DROP TABLE IF EXISTS t16")
    sql("DROP TABLE IF EXISTS t17")
    sql("DROP TABLE IF EXISTS t18")
    sql("DROP TABLE IF EXISTS t19")
    sql("DROP TABLE IF EXISTS t20")
    sql("DROP TABLE IF EXISTS t21")
    sql("DROP TABLE IF EXISTS t22")
    sql("DROP TABLE IF EXISTS t23")
    sql("DROP TABLE IF EXISTS t24")
    sql("DROP TABLE IF EXISTS t25")
    sql("DROP TABLE IF EXISTS t26")
    sql("DROP TABLE IF EXISTS t27")
    sql("DROP TABLE IF EXISTS t28")
    sql("DROP TABLE IF EXISTS t40")
    sql("DROP TABLE IF EXISTS t41")
    sql("DROP TABLE IF EXISTS t42")
    sql("DROP TABLE IF EXISTS t43")
    sql("DROP TABLE IF EXISTS t44")
    sql("DROP TABLE IF EXISTS t45")
    sql("DROP TABLE IF EXISTS t46")
    sql("DROP TABLE IF EXISTS t47")
    sql("DROP TABLE IF EXISTS t48")
    sql("DROP TABLE IF EXISTS t49")
    sql("DROP TABLE IF EXISTS t50")
    sql("DROP TABLE IF EXISTS bucketed_parquet_table")
    sql("DROP TABLE IF EXISTS parquet_table")
  }

  test("test create table with buckets using table properties and loaded data will" +
    " store into different files") {
    sql("CREATE TABLE t4 (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
        "('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t4")
    val table = CarbonEnv.getCarbonTable(Option("default"), "t4")(sqlContext.sparkSession)
    val segmentDir = FileFactory.getCarbonFile(table.getTablePath + "/Fact/Part0/Segment_0")
    val dataFiles = segmentDir.listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = file.getName.endsWith(".carbondata")
    })
    assert(dataFiles.length == 4)
    checkAnswer(sql("select count(*) from t4"), Row(100))
    checkAnswer(sql("select count(*) from t4 where name='aaa99'"), Row(1))
    if (table != null && table.getBucketingInfo() != null) {
      assert(true)
    } else {
      assert(false, "Bucketing info does not exist")
    }
  }

  test("test load data with DATE data type as bucket column") {
    sql("DROP TABLE IF EXISTS table_bucket")
    sql("""
           CREATE TABLE IF NOT EXISTS table_bucket
           (ID Int, date DATE, starttime Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED AS carbondata TBLPROPERTIES ('BUCKET_NUMBER'='2', 'BUCKET_COLUMNS'='date')
        """)
    sql(
      s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData1.csv' into table table_bucket
           OPTIONS('dateformat' = 'yyyy/MM/dd','timestampformat'='yyyy-MM-dd HH:mm:ss')
           """)
    sql(
      s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData2.csv' into table table_bucket
           OPTIONS('dateformat' = 'yyyy-MM-dd','timestampformat'='yyyy/MM/dd HH:mm:ss')
           """)

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    checkAnswer(
      sql("SELECT date FROM table_bucket WHERE ID = 1"),
      Seq(Row(new Date(sdf.parse("2015-07-23").getTime)))
    )
    checkAnswer(
      sql("SELECT date FROM table_bucket WHERE ID = 18"),
      Seq(Row(new Date(sdf.parse("2015-07-25").getTime)))
    )
    sql("DROP TABLE IF EXISTS table_bucket")
  }

  test("test IUD of bucket table") {
    sql("CREATE TABLE t40 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='10', 'BUCKET_COLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t40")
    sql("CREATE TABLE t41 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='10', 'BUCKET_COLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t41")

    // insert
    sql(s"insert into t40 select 101,'2015/10/16','china','aaa101','phone2569','ASD16163',15100")
    checkAnswer(sql(
      """select count(*) from t40
      """.stripMargin), Row(101))
    // update
    sql(s"update t40 set (name) = ('aaa100') where name='aaa101'")
    checkAnswer(sql(
      """select count(*) from t40
      """.stripMargin), Row(101))
    checkAnswer(sql(
      """select count(*) from t40 where name='aaa100'
      """.stripMargin), Row(2))
    // delete
    sql(s"delete from t40 where name='aaa100'")
    checkAnswer(sql(
      """select count(*) from t40
      """.stripMargin), Row(99))
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t40 t1, t41 t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(99))
    // insert again
    sql(s"insert into t40 select 1011,'2015/10/16','china','aaa1011','phone2569','ASD16163',15100")
    sql(s"insert into t40 select 1012,'2015/10/16','china','aaa1012','phone2569','ASD16163',15100")
    sql(s"insert into t40 select 1013,'2015/10/16','china','aaa1013','phone2569','ASD16163',15100")
    sql(s"insert into t40 select 1014,'2015/10/16','china','aaa1014','phone2569','ASD16163',15100")
    checkAnswer(sql(
      """select count(*) from t40
      """.stripMargin), Row(103))

    // join after IUD
    val plan = sql(
      """
        |select t1.*, t2.*
        |from t40 t1, t41 t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist on bucket tables")
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t40 t1, t41 t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(99))

    // insert into t41
    sql(s"insert into t41 select 1014,'2015/10/16','china','aaa1014','phone2569','ASD16163',15100")

    // join after 2 tables both IUD
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t40 t1, t41 t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(100))
    val plan2 = sql(
      """
        |select t1.*, t2.*
        |from t40 t1, t41 t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    var shuffleExists2 = false
    plan2.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists2 = true
    }
    assert(!shuffleExists2, "shuffle should not exist on bucket tables")
  }

  test("test create carbon table with buckets like hive sql") {
    sql("CREATE TABLE t13 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata CLUSTERED BY (name) INTO 4 BUCKETS")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t13")
    val table = CarbonEnv.getCarbonTable(Option("default"), "t13")(sqlContext.sparkSession)
    val segmentDir = FileFactory.getCarbonFile(table.getTablePath + "/Fact/Part0/Segment_0")
    val dataFiles = segmentDir.listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = file.getName.endsWith(".carbondata")
    })
    assert(dataFiles.length == 4)
    checkAnswer(sql("select count(*) from t13"), Row(100))
    checkAnswer(sql("select count(*) from t13 where name='aaa99'"), Row(1))
    if (table != null && table.getBucketingInfo() != null) {
      assert(true)
    } else {
      assert(false, "Bucketing info does not exist")
    }
  }

  test("test create table with buckets unsafe") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "true")
    sql("CREATE TABLE t10 (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
        "('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t10")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "false")
    val table: CarbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "t10")
    if (table != null && table.getBucketingInfo() != null) {
      assert(true)
    } else {
      assert(false, "Bucketing info does not exist")
    }
  }

  test("test create table with empty bucket number must fail") {
    val ex = intercept[MalformedCarbonCommandException] {
      sql(
        "CREATE TABLE t11 (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
        "('BUCKET_NUMBER'='', 'BUCKET_COLUMNS'='name')"
      )
    }
    assert(ex.getMessage.contains("INVALID NUMBER OF BUCKETS SPECIFIED"))
  }

  test("test create table with bucket number having non numeric value must fail") {
    val ex = intercept[MalformedCarbonCommandException] {
      sql(
        "CREATE TABLE t11 (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
        "('BUCKET_NUMBER'='one', 'BUCKET_COLUMNS'='name')"
      )
    }
    assert(ex.getMessage.contains("INVALID NUMBER OF BUCKETS SPECIFIED"))
  }

  test("must be unable to create if number of buckets is in negative number") {
    try {
      sql(
        """
           CREATE TABLE t9
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           USING carbondata
           OPTIONS("bucket_number"="-1", "bucket_columns"="name")
        """)
      assert(false)
    }
    catch {
      case malformedCarbonCommandException: MalformedCarbonCommandException => assert(true)
    }
  }

  test("must unable to create table if number of buckets is 0") {
    try {
      sql(
        """
          |CREATE TABLE t11
          |(ID Int,
          | date Timestamp,
          | country String,
          | name String,
          | phonetype String,
          | serialname String,
          | salary Int)
          | STORED AS carbondata
          | TBLPROPERTIES('bucket_number'='0', 'bucket_columns'='name')
        """.stripMargin
      )
      assert(false)
    } catch {
      case malformedCarbonCommandException: MalformedCarbonCommandException => assert(true)
    }
  }

  test("Bucket table only sort inside buckets, can not set sort scope but can set sort columns.") {
    val ex = intercept[ProcessMetaDataException] {
      sql("CREATE TABLE t44 (ID Int, date Timestamp, country String, name String, " +
          "phonetype String,serialname String, salary Int) STORED AS carbondata " +
          "TBLPROPERTIES('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='name', " +
          "'sort_columns'='name', 'sort_scope'='global_sort')")
    }
    assert(ex.getMessage.contains("Bucket table only sort inside buckets," +
      " can not set sort scope but can set sort columns."))
  }

  test("test create table with both no bucket join of carbon tables") {
    sql("CREATE TABLE t5 (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t5")
    val plan = sql(
      """
        |select t1.*, t2.*
        |from t5 t1, t5 t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t5 t1, t5 t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(100))
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(shuffleExists, "shuffle should exist on non bucket tables")
  }

  test("test join of carbon bucket table and non bucket parquet table") {
    sql("CREATE TABLE t8 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t8")

    sql("DROP TABLE IF EXISTS parquet_table")
    sql("select * from t8").write
      .format("parquet")
      .saveAsTable("parquet_table")

    val plan = sql(
      """
        |select t1.*, t2.*
        |from t8 t1, parquet_table t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t8 t1, parquet_table t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(100))
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(shuffleExists, "shuffle should exist on non bucket tables")
    sql("DROP TABLE parquet_table")
  }

  test("test no shuffle when using bucket tables") {
    sql("CREATE TABLE t12 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata CLUSTERED BY (name) INTO 4 BUCKETS")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t12")

    sql("DROP TABLE IF EXISTS bucketed_parquet_table")
    sql("select * from t12").write
      .format("parquet")
      .bucketBy(4, "name")
      .saveAsTable("bucketed_parquet_table")

    checkAnswer(sql("select count(*) from t12"), Row(100))
    checkAnswer(sql("select count(*) from bucketed_parquet_table"), Row(100))

    val plan = sql(
      """
        |select t1.*, t2.*
        |from t12 t1, bucketed_parquet_table t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist on bucket tables")
    sql("DROP TABLE bucketed_parquet_table")
  }

  test("test join of carbon bucket tables") {
    sql("CREATE TABLE t6 (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
        "('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t6")
    sql("CREATE TABLE t6_ (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t6_")
    val plan = sql(
      """
        |select t1.*, t2.*
        |from t6 t1, t6_ t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t6 t1, t6_ t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(100))
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist on bucket tables")
  }

  test("test join of carbon bucket table and parquet bucket table") {
    sql("CREATE TABLE t7 (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
        "('BUCKET_NUMBER'='9', 'BUCKET_COLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t7")

    sql("DROP TABLE IF EXISTS bucketed_parquet_table")
    sql("select * from t7").write
      .format("parquet")
      .bucketBy(9, "name")
      .saveAsTable("bucketed_parquet_table")
    // carbon join parquet, both bucket tables.
    val plan = sql(
      """
        |select t1.*, t2.*
        |from t7 t1, bucketed_parquet_table t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    // parquet join parquet, both bucket tables.
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t7 t1, bucketed_parquet_table t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(100))
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist on bucket tables")
    sql("DROP TABLE bucketed_parquet_table")
  }

  test("test join of carbon bucket tables using hive sql") {
    sql("CREATE TABLE t14 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata CLUSTERED BY (name) INTO 4 BUCKETS")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t14")
    sql("CREATE TABLE t15 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata CLUSTERED BY (name) INTO 4 BUCKETS")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t15")
    val plan = sql(
      """
        |select t1.*, t2.*
        |from t14 t1, t15 t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t14 t1, t15 t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(100))
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist on bucket tables")
  }

  test("test join of diff data types as bucket column for carbon tables") {
    sql("CREATE TABLE t16 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='ID')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t16")
    sql("CREATE TABLE t17 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t17")
    val plan = sql(
      """
        |select t1.*, t2.*
        |from t16 t1, t17 t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t16 t1, t17 t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(100))
  }

  test("timestamp as bucket column, test join of carbon bucket tables") {
    sql("CREATE TABLE t18 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='date')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t18")
    sql("CREATE TABLE t19 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='date')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t19")
    val plan = sql(
      """
        |select t1.*, t2.*
        |from t18 t1, t19 t2
        |where t1.date = t2.date
      """.stripMargin).queryExecution.executedPlan
    // here the time column in source.csv has some duplicate values
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t18 t1, t19 t2
        |where t1.date = t2.date) temp
      """.stripMargin), Row(120))
        var shuffleExists = false
        plan.collect {
          case s: Exchange if (s.getClass.getName.equals
          ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
            s.getClass.getName.equals
            ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
          => shuffleExists = true
        }
        assert(!shuffleExists, "shuffle should not exist on bucket tables")
  }

  test("timestamp as bucket column, test join of carbon bucket table and parquet table") {
    sql("CREATE TABLE t20 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='9', 'BUCKET_COLUMNS'='date')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t20")
    // parquet 1
    sql("DROP TABLE IF EXISTS bucketed_parquet_table_t20")
    sql("select * from t20").write
      .format("parquet")
      .bucketBy(9, "date")
      .saveAsTable("bucketed_parquet_table_t20")
    // parquet 2
    sql("DROP TABLE IF EXISTS bucketed_parquet_table_t20_")
    sql("select * from t20").write
      .format("parquet")
      .bucketBy(9, "date")
      .saveAsTable("bucketed_parquet_table_t20_")

    // parquet join with parquet
    val plan2 = sql(
      """
        |select t1.*, t2.*
        |from bucketed_parquet_table_t20_ t1, bucketed_parquet_table_t20 t2
        |where t1.date = t2.date
      """.stripMargin).queryExecution.executedPlan
    var shuffleExists2 = false
    plan2.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists2 = true
    }
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from bucketed_parquet_table_t20_ t1, bucketed_parquet_table_t20 t2
        |where t1.date = t2.date) temp
      """.stripMargin), Row(120))

    // carbon join with parquet
    val plan = sql(
      """
        |select t1.*, t2.*
        |from t20 t1, bucketed_parquet_table_t20 t2
        |where t1.date = t2.date
      """.stripMargin).queryExecution.executedPlan
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t20 t1, bucketed_parquet_table_t20 t2
        |where t1.date = t2.date) temp
      """.stripMargin), Row(120))
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }

    assert(shuffleExists == shuffleExists2, "for no string bucket column, shuffle should " +
      "keep the same behavior as parquet")
    sql("DROP TABLE IF EXISTS bucketed_parquet_table_t20")
    sql("DROP TABLE IF EXISTS bucketed_parquet_table_t20_")

  }

  test("long as bucket column, test join of carbon bucket table and parquet table") {
    sql("CREATE TABLE t21 (ID long, date Timestamp, country String, name String, " +
        "phonetype String,serialname String, salary Int) STORED AS carbondata " +
        "TBLPROPERTIES('BUCKET_NUMBER'='9', 'BUCKET_COLUMNS'='ID')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t21")
    // parquet 1
    sql("DROP TABLE IF EXISTS bucketed_parquet_table_t21")
    sql("select * from t21").write
      .format("parquet")
      .bucketBy(9, "ID")
      .saveAsTable("bucketed_parquet_table_t21")

    // carbon join with parquet
    val plan = sql(
      """
        |select t1.*, t2.*
        |from t21 t1, bucketed_parquet_table_t21 t2
        |where t1.ID = t2.ID
      """.stripMargin).queryExecution.executedPlan
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist in bucket table join")
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t21 t1, bucketed_parquet_table_t21 t2
        |where t1.ID = t2.ID) temp
      """.stripMargin), Row(100))
    sql("DROP TABLE IF EXISTS bucketed_parquet_table_t21")
  }

  test("int as bucket column, test join of carbon bucket table and parquet table") {
    sql("CREATE TABLE t22 (ID int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='9', 'BUCKET_COLUMNS'='ID')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t22")
    // parquet 1
    sql("DROP TABLE IF EXISTS bucketed_parquet_table_t22")
    sql("select * from t22").write
      .format("parquet")
      .bucketBy(9, "ID")
      .saveAsTable("bucketed_parquet_table_t22")

    // carbon join with parquet
    val plan = sql(
      """
        |select t1.*, t2.*
        |from t22 t1, bucketed_parquet_table_t22 t2
        |where t1.ID = t2.ID
      """.stripMargin).queryExecution.executedPlan
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist in bucket table join")
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t22 t1, bucketed_parquet_table_t22 t2
        |where t1.ID = t2.ID) temp
      """.stripMargin), Row(100))
    sql("DROP TABLE IF EXISTS bucketed_parquet_table_t22")
  }

  test("test bucket hash method config") {
    sql("CREATE TABLE t23 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='name', 'bucket_hash_method'='NATIVE' )")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t23")
    sql("CREATE TABLE t24 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='name', 'bucket_hash_method'='NATIVE')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t24")
    val plan = sql(
      """
        |select t1.*, t2.*
        |from t23 t1, t24 t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t23 t1, t24 t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(100))
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist on bucket tables")
  }

  test("only shuffle 1 side whose bucket num larger when join of carbon bucket" +
    " tables with diff bucket num") {
    sql("CREATE TABLE t25 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='3', 'BUCKET_COLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t25")
    sql("CREATE TABLE t26 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='7', 'BUCKET_COLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t26")

    val plan = sql(
      """
        |select t1.*, t2.*
        |from t25 t1, t26 t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t25 t1, t26 t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(100))

    var shuffleLeftExists = false
    var shuffleRightExists = false
    plan.asInstanceOf[WholeStageCodegenExec].child.asInstanceOf[SortMergeJoinExec].left.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleLeftExists = true
    }

    plan.asInstanceOf[WholeStageCodegenExec].child.asInstanceOf[SortMergeJoinExec].right.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleRightExists = true
    }
    assert(shuffleLeftExists && !shuffleRightExists, "only shuffle 1 side whose bucket num larger")
  }

  test("test compaction of bucket tables") {
    sql("CREATE TABLE t27 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='10', 'BUCKET_COLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t27")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t27")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t27")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t27")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t27")
    sql(s"alter table t27 compact 'minor'")

    val table = CarbonEnv.getCarbonTable(Option("default"), "t27")(sqlContext.sparkSession)
    // data should store into diff files bases on bucket id in compaction
    val segmentDir = FileFactory.getCarbonFile(table.getTablePath + "/Fact/Part0/Segment_0.1")
    val dataFiles = segmentDir.listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = file.getName.endsWith(".carbondata")
    })
    assert(dataFiles.length == 10)

    sql("CREATE TABLE t28 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='10', 'BUCKET_COLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t28")
    val plan = sql(
      """
        |select t1.*, t2.*
        |from t27 t1, t28 t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t27 t1, t28 t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(500))
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist on bucket tables")
  }

  test("test alter column of bucket table") {
    sql("CREATE TABLE t42 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='10', 'BUCKET_COLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t42")

    sql("CREATE TABLE t43 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='10', 'BUCKET_COLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t43")

    // bucket columns not allowed to change include rename change data type and drop.
    val ex = intercept[MalformedCarbonCommandException] {
      sql(s"alter table t42 change name name222 string")
    }
    assert(ex.getMessage.contains("Column Rename Operation failed." +
      " Renaming the bucket column name is not allowed"))
    val ex2 = intercept[ProcessMetaDataException] {
      sql(s"alter table t42 drop columns(name)")
    }
    assert(ex2.getMessage.contains("Bucket columns cannot be dropped: List(name)"))

    // alter table column
    sql(s"alter table t42 change salary slong long")
    checkAnswer(sql(
      """select count(*) from t42 where slong=15000
      """.stripMargin), Row(1))
    // join after alter table
    val plan = sql(
      """
        |select t1.*, t2.*
        |from t42 t1, t43 t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t42 t1, t43 t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(100))
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist on bucket tables")

    // test desc formatted
    val descPar = sql("desc formatted t42").collect
    descPar.find(_.get(0).toString.contains("Bucket Columns")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
      case None => fail("Bucket Columns: not found in describe formatted")
    }
    descPar.find(_.get(0).toString.contains("Number of Buckets")) match {
      case Some(row) => assert(row.get(1).toString.contains("10"))
      case None => fail("Number of Buckets: not found in describe formatted")
    }
  }

  test("test insert into bucket table old insert flow") {
    sql("CREATE TABLE t45 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t45")
    sql("CREATE TABLE t46 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='name')")
    // use old flow
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "true")
    sql(s"INSERT INTO t46 SELECT * FROM t45")

    val table = CarbonEnv.getCarbonTable(Option("default"), "t46")(sqlContext.sparkSession)
    val segmentDir = FileFactory.getCarbonFile(table.getTablePath + "/Fact/Part0/Segment_0")
    val dataFiles = segmentDir.listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = file.getName.endsWith(".carbondata")
    })
    assert(dataFiles.length == 4)
    checkAnswer(sql(
      """select count(*) from t46
      """.stripMargin), Row(100))
    sql(
      """select * from t46
      """.stripMargin).collect()

    val plan = sql(
      """
        |select t1.*, t2.*
        |from t45 t1, t46 t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist on bucket tables")
    checkAnswer(sql(
      """select count(*) from t46 where name='aaa1'
      """.stripMargin), Row(1))
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t45 t1, t46 t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(100))
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "false")
  }

  test("test insert into bucket table new insert flow") {
    sql("CREATE TABLE t47 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t47")
    sql("CREATE TABLE t48 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='name')")

    // use new flow
    sql(s"INSERT INTO t48 SELECT * FROM t47")

    val table = CarbonEnv.getCarbonTable(Option("default"), "t48")(sqlContext.sparkSession)
    val segmentDir = FileFactory.getCarbonFile(table.getTablePath + "/Fact/Part0/Segment_0")
    val dataFiles = segmentDir.listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = file.getName.endsWith(".carbondata")
    })
    assert(dataFiles.length == 4)
    checkAnswer(sql(
      """select count(*) from t48
      """.stripMargin), Row(100))
    sql(
      """select * from t48
      """.stripMargin).collect()

    val plan = sql(
      """
        |select t1.*, t2.*
        |from t47 t1, t48 t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist on bucket tables")
    checkAnswer(sql(
      """select count(*) from t48 where name='aaa1'
      """.stripMargin), Row(1))
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t47 t1, t48 t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(100))
  }

  test("test multi bucket columns") {
    sql("CREATE TABLE t49 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='name,date')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t49")
    sql("CREATE TABLE t50 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='name,date')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t50")
    val plan = sql(
      """
        |select t1.*, t2.*
        |from t49 t1, t50 t2
        |where t1.name = t2.name and t1.date = t2.date
      """.stripMargin).queryExecution.executedPlan
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t49 t1, t50 t2
        |where t1.name = t2.name and t1.date = t2.date) temp
      """.stripMargin), Row(100))
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist when all bucket columns" +
      "in query filter")

    val plan2 = sql(
      """
        |select t1.*, t2.*
        |from t49 t1, t50 t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t49 t1, t50 t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(100))
    var shuffleExists2 = false
    plan2.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists2 = true
    }
    assert(shuffleExists2, "shuffle should exist when some bucket columns not exist in filter")
  }

  test("test load data with boolean type as bucket column") {
    sql("drop table if exists boolean_table")
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | booleanField BOOLEAN,
         | stringField STRING,
         | intField INT
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('BUCKET_NUMBER'='1', 'BUCKET_COLUMNS'='booleanField')
       """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$resourcesPath/bool/supportBooleanWithFileHeader.csv'
         | INTO TABLE boolean_table
           """.stripMargin)

    checkAnswer(sql("select count(*) from boolean_table"), Row(10))
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("DROP TABLE IF EXISTS t4")
    sql("DROP TABLE IF EXISTS t5")
    sql("DROP TABLE IF EXISTS t6")
    sql("DROP TABLE IF EXISTS t6_")
    sql("DROP TABLE IF EXISTS t7")
    sql("DROP TABLE IF EXISTS t8")
    sql("DROP TABLE IF EXISTS t9")
    sql("DROP TABLE IF EXISTS t10")
    sql("DROP TABLE IF EXISTS t11")
    sql("DROP TABLE IF EXISTS t12")
    sql("DROP TABLE IF EXISTS t13")
    sql("DROP TABLE IF EXISTS t14")
    sql("DROP TABLE IF EXISTS t15")
    sql("DROP TABLE IF EXISTS t16")
    sql("DROP TABLE IF EXISTS t17")
    sql("DROP TABLE IF EXISTS t18")
    sql("DROP TABLE IF EXISTS t19")
    sql("DROP TABLE IF EXISTS t20")
    sql("DROP TABLE IF EXISTS t21")
    sql("DROP TABLE IF EXISTS t22")
    sql("DROP TABLE IF EXISTS t23")
    sql("DROP TABLE IF EXISTS t24")
    sql("DROP TABLE IF EXISTS t25")
    sql("DROP TABLE IF EXISTS t26")
    sql("DROP TABLE IF EXISTS t27")
    sql("DROP TABLE IF EXISTS t28")
    sql("DROP TABLE IF EXISTS t40")
    sql("DROP TABLE IF EXISTS t41")
    sql("DROP TABLE IF EXISTS t42")
    sql("DROP TABLE IF EXISTS t43")
    sql("DROP TABLE IF EXISTS t44")
    sql("DROP TABLE IF EXISTS t45")
    sql("DROP TABLE IF EXISTS t46")
    sql("DROP TABLE IF EXISTS t47")
    sql("DROP TABLE IF EXISTS t48")
    sql("DROP TABLE IF EXISTS t49")
    sql("DROP TABLE IF EXISTS t50")
    sql("DROP TABLE IF EXISTS bucketed_parquet_table")
    sql("DROP TABLE IF EXISTS parquet_table")
    if (null != threshold) {
      sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", threshold)
    }
  }
}
