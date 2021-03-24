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
package org.apache.carbondata.spark.testsuite.secondaryindex

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.statusmanager.SegmentStatus
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.sdk.file.{CarbonWriter, Schema}
import org.apache.carbondata.spark.testsuite.secondaryindex.TestSecondaryIndexUtils.isFilterPushedDownToSI

/**
 * test cases for IUD data retention on SI tables
 */
class TestIndexModelWithIUD extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    dropIndexAndTable()
  }

  test("test index with IUD delete all_rows") {
    sql(
      "create table dest (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table dest""")
    sql("create index index_dest1 on table dest (c3) AS 'carbondata'")
    // create second index table , result should be same
    sql("create index index_dest2 on table dest (c3,c5) AS 'carbondata'")
    // delete all rows in the segment
    sql("delete from dest d where d.c2 not in (56)").collect()
    checkAnswer(
      sql("""select c3 from dest"""),
      sql("""select c3 from index_dest1""")
    )
    checkAnswer(
      sql("""select c3,c5 from dest"""),
      sql("""select c3,c5 from index_dest2""")
    )
    sql("show segments for table index_dest1").collect()
    assert(sql("show segments for table index_dest1").collect()(0).get(1).toString()
             .equals(SegmentStatus.MARKED_FOR_DELETE.getMessage))
    assert(sql("show segments for table index_dest2").collect()(0).get(1).toString()
             .equals(SegmentStatus.MARKED_FOR_DELETE.getMessage))

    // execute clean files
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    sql("clean files for table dest options('force'='true')")
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED)

    sql("show segments for table index_dest2").collect()
    val exception_index_dest1 = intercept[IndexOutOfBoundsException] {
      assert(sql("show segments for table index_dest1").collect()(0).get(1).toString()
        .equals(SegmentStatus.MARKED_FOR_DELETE.getMessage))
    }
    val exception_index_dest2 = intercept[IndexOutOfBoundsException] {
      assert(sql("show segments for table index_dest2").collect()(0).get(1).toString()
        .equals(SegmentStatus.MARKED_FOR_DELETE.getMessage))
    }

    // load again and check result
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table dest""")
    checkAnswer(
      sql("""select c3 from dest"""),
      sql("""select c3 from index_dest1""")
    )
    checkAnswer(
      sql("""select c3,c5 from dest"""),
      sql("""select c3,c5 from index_dest2""")
    )


  }

  test("test index with IUD delete all_rows-1") {
    sql(
      "create table source (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table source""")
    sql("create index index_source1 on table source (c5) AS 'carbondata'")
    // delete (5-1)=4 rows
    try {
      sql("""delete from source d where d.c2 in (1,2,3,4)""").collect()
      assert(false)
    }
    catch {
      case ex: Exception => assert(true)
        // results should not be same
        val exception = intercept[Exception] {
          checkAnswer(
            sql("""select c5 from source"""),
            sql("""select c5 from index_source1""")
          )
        }
    }
    // crete second index table
    sql("create index index_source2 on table source (c3) AS 'carbondata'")
    // result should be same
      checkAnswer(
        sql("""select c3 from source"""),
        sql("""select c3 from index_source2""")
      )
    sql("clean files for table source")
    sql("show segments for table index_source2").collect()
    assert(sql("show segments for table index_source2").collect()(0).get(1).toString()
      .equals(SegmentStatus.SUCCESS.getMessage))
  }

  test("test index with IUD delete using Join") {
    sql(
      "create table test (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table test""")
    sql("create index index_test1 on table test (c3) AS 'carbondata'")
    // delete all rows in the segment
    sql("delete from test d where d.c2 not in (56)").collect()
    checkAnswer(
      sql(
        "select test.c3, index_test1.c3 from test right join index_test1  on test.c3 =  " +
        "index_test1.c3"),
      Seq())
  }

  test("test if secondary index gives correct result on limit query after row deletion") {
    sql("create table t10(id string, country string) STORED AS carbondata").collect()
    sql("create index si3 on table t10(country) AS 'carbondata'")
    sql(
      s" load data INPATH '$resourcesPath/secindex/IUD/sample_1.csv' INTO table t10 options " +
      "('DELIMITER'=',','FILEHEADER'='id,country')")
    sql(
      s" load data INPATH '$resourcesPath/secindex/IUD/sample_2.csv' INTO table t10 options " +
      "('DELIMITER'=',','FILEHEADER'='id,country')")
    try {
      sql("delete from t10 where id in (1,2)").collect()
    assert(false)
    }
    catch {
      case ex: Exception => assert(true)
    }
    sql(" select *  from t10").collect()
    checkAnswer(sql(" select country from t10 where country = 'china' order by id limit 1"),
      Row("china"))
  }

  test("test index with IUD delete and compaction") {
    sql(
      "create table test2 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table test2""")
    sql("create index index_test2 on table test2 (c3) AS 'carbondata'")
    sql("delete from test2 d where d.c2 = '1'").collect()
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table test2""")
    sql("alter table test2 compact 'major'")
    // delete all rows in the segment
    sql("delete from test2 d where d.c2 not in (56)").collect()
    checkAnswer(
      sql(
        "select test2.c3, index_test2.c3 from test2 right join index_test2  on test2.c3 =  " +
        "index_test2.c3"),
      Seq())
  }

  test("test set segments with SI") {
    sql("create table dest2 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table dest2""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table dest2""")
    sql("create index index_dest21 on table dest2 (c3) AS 'carbondata'")
    checkAnswer(sql("select count(*) from dest2"), Seq(Row(10)))
    sql("set carbon.input.segments.default.dest2=0")
    checkAnswer(sql("select count(*) from dest2"), Seq(Row(5)))
    checkAnswer(sql("select count(*) from index_dest21"), Seq(Row(5)))
  }

  test("Test block secondary index creation on external table") {
    var writerPath = new File(this.getClass.getResource("/").getPath
                              +
                              "../." +
                              "./target/SparkCarbonFileFormat/WriterOutput/")
      .getCanonicalPath
    writerPath = writerPath.replace("\\", "/")
    val schema = new StringBuilder()
      .append("[ \n")
      .append("   {\"NaMe\":\"string\"},\n")
      .append("   {\"age\":\"int\"},\n")
      .append("   {\"height\":\"double\"}\n")
      .append("]")
      .toString()

    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(writerPath)
          .uniqueIdentifier(
            System.currentTimeMillis).withBlockSize(2)
          .withCsvInput(Schema.parseJson(schema)).writtenBy("TestNonTransactionalCarbonTable")
          .build()
      var i = 0
      while (i < 2) {
        writer.write(Array[String]("robot" + i, String.valueOf(i), String.valueOf(i.toDouble / 2)))
        i = i + 1
      }
      writer.close()
    } catch {
      case ex: Throwable => throw new RuntimeException(ex)
    }
    sql(s"create external table test3 STORED AS carbondata location '$writerPath'")
    val exception = intercept[MalformedCarbonCommandException] {
      sql("create index idx_test3 on table test3(cert_no) AS 'carbondata'")
    }
    assert(exception.getMessage
      .contains("Unsupported operation on non transactional table"))
  }

  test("test SI with Union and Union All with same table") {
    sql("create table dest3 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
    sql("insert into dest3 values('a',1,'abc','b')")
    sql("create table dest3_parquet stored as parquet select * from dest3")
    sql("create index index_dest3 on table dest3 (c3) AS 'carbondata'")
    checkAnswer(sql(
      "select c3 from dest3 where c3 = 'abc' union select c3 from dest3  where c3 = 'abc'"),
      sql("select c3 from dest3_parquet where c3 = 'abc' union select c3 from " +
          "dest3_parquet where c3 = 'abc'"))
    checkAnswer(sql("select c3 from dest3 where c3 = 'abc' union all " +
                    "select c3 from dest3 where c3 = 'abc'"),
      sql("select c3 from dest3_parquet where c3 = 'abc' union all select c3 from " +
          "dest3_parquet  where c3 = 'abc'"))
  }

  test("test SI with Union and Union All with different table") {
    sql("create table dest4 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
    sql("insert into dest4 values('a',1,'abc','b')")
    sql("create table dest4_parquet stored as parquet select * from dest4")
    sql("create table dest4_parquet1 stored as parquet select * from dest4")
    sql("create table dest41 STORED AS carbondata select * from dest4")
    sql("create index index_dest4 on table dest4 (c3) AS 'carbondata'")
    sql("create index index_dest41 on table dest41 (c3) AS 'carbondata'")
    checkAnswer(sql(
      "select c3 from dest4 where c3 = 'abc' union select c3 from dest41  where c3 = 'abc'"),
      sql(
        "select c3 from dest4_parquet where c3 = 'abc' union select c3 from " +
        "dest4_parquet1 where c3 = 'abc'"))
    checkAnswer(sql("select c3 from dest4 where c3 = 'abc' union all select c3 from dest41 " +
                    "where c3 = 'abc'"),
      sql(
        "select c3 from dest4_parquet where c3 = 'abc' union all select c3 " +
        "from dest4_parquet1 where c3 = 'abc'"))
  }

  test("test SI with more than 2 Union and Union All with different table") {
    sql("create table dest5 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
    sql("insert into dest5 values('a',1,'abc','b')")
    sql("create table dest5_parquet stored as parquet select * from dest5")
    sql("create table dest5_parquet1 stored as parquet select * from dest5")
    sql("create table dest51 STORED AS carbondata select * from dest5")
    sql("create index index_dest5 on table dest5 (c3) AS 'carbondata'")
    sql("create index index_dest51 on table dest51 (c3) AS 'carbondata'")
    checkAnswer(sql(
      "select c3 from dest5 where c3 = 'abc' union select c3 from dest51  " +
      "where c3 = 'abc' union select c3 from dest51  where c3 = 'abc'"),
      sql(
        "select c3 from dest5_parquet where c3 = 'abc' union select c3 from dest5_parquet1" +
        " where c3 = 'abc' union select c3 from dest5_parquet1  where c3 = 'abc'"))

    checkAnswer(sql(
      "select c3 from dest5 where c3 = 'abc' union all select c3 from dest51 " +
      "where c3 = 'abc' union all select c3 from dest51  where c3 = 'abc'"),
      sql(
        "select c3 from dest5_parquet where c3 = 'abc' union all select c3 from " +
        "dest5_parquet1 where c3 = 'abc' union all select c3 from dest5_parquet1 " +
        "where c3 = 'abc'"))
  }

  test("test SI with more than 2 Union and Union All with same table") {
    sql("create table dest6 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
    sql("insert into dest6 values('a',1,'abc','b')")
    sql("create table dest6_parquet stored as parquet select * from dest6")
    sql("create index index_dest6 on table dest6 (c3) AS 'carbondata'")
    checkAnswer(sql(
      "select c3 from dest6 where c3 = 'abc' union select c3 from dest6  where c3 = 'abc' " +
      "union select c3 from dest6  where c3 = 'abc'"),
      sql(
        "select c3 from dest6_parquet where c3 = 'abc' union select c3 from dest6_parquet " +
        "where c3 = 'abc'"))
    checkAnswer(sql(
      "select c3 from dest6 where c3 = 'abc' union all select c3 from dest6  where c3 = 'abc' " +
      "union all select c3 from dest6  where c3 = 'abc'"),
      sql(
        "select c3 from dest6_parquet where c3 = 'abc' union all select c3 from dest6_parquet  " +
        "where c3 = 'abc' union all select c3 from dest6_parquet  where c3 = 'abc'"))
  }

  test("test SI with join") {
    sql("create table dest7 (c1 string,c2 int,c3 string,c5 string) STORED AS " +
        "carbondata")
    sql("insert into dest7 values('a',1,'abc','b')")
    sql("create table dest7_parquet stored as parquet select * from dest7")
    sql("create index index_dest7 on table dest7 (c3) AS 'carbondata'")
    checkAnswer(
      sql("select t1.c3,t2.c3 from dest7 t1, dest7 t2 where t1.c3=t2.c3 and t1.c3 = 'abc'"),
      sql("select t1.c3,t2.c3 from dest7_parquet t1, dest7 t2 where t1.c3=t2.c3 and t1.c3 = 'abc'"))
  }

  test("test SI with Union and Union All with donotPushtoSI operations") {
    sql("create table dest8 (c1 string,c2 int,c3 string,c5 string) STORED AS " +
        "carbondata")
    sql("insert into dest8 values('a',1,'abc','b')")
    sql("create table dest8_parquet stored as parquet select * from dest8")
    sql("create index index_dest8 on table dest8 (c3) AS 'carbondata'")
    checkAnswer(sql(
      "select c3 from dest8 where c3 = 'abc' union select c3 from dest8  where c3 != 'abc'"),
      sql("select c3 from dest8_parquet where c3 = 'abc' union select c3 from " +
          "dest8_parquet where c3 != 'abc'"))
    checkAnswer(sql("select c3 from dest8 where c3 = 'abc' union all " +
                    "select c3 from dest8 where c3 != 'abc'"),
      sql("select c3 from dest8_parquet where c3 = 'abc' union all select c3 from " +
          "dest8_parquet  where c3 != 'abc'"))
    checkAnswer(
      sql("select c3 from dest8 where c3 like '%bc' union select c3 from dest8 " +
          "where c3 not like '%bc'"),
      sql("select c3 from dest8_parquet where c3 like '%bc' union select c3 from " +
          "dest8_parquet where c3 not like '%bc'"))
    checkAnswer(sql("select c3 from dest8 where c3 like '%bc' union all " +
                    "select c3 from dest8 where c3 not like '%bc'"),
      sql("select c3 from dest8_parquet where c3 like '%bc' union all select c3 from " +
          "dest8_parquet  where c3 not like '%bc'"))
    checkAnswer(
      sql("select c3 from dest8 where c3 in ('abc') union select c3 from dest8 " +
          "where c3 not in ('abc')"),
      sql("select c3 from dest8_parquet where c3 in ('abc') union select c3 from " +
          "dest8_parquet where c3 not in ('abc')"))
    checkAnswer(sql("select c3 from dest8 where c3 in ('abc') union all " +
                    "select c3 from dest8 where c3 not in ('abc')"),
      sql("select c3 from dest8_parquet where c3 in ('abc') union all select c3 from " +
          "dest8_parquet  where c3 not in ('abc')"))
    checkAnswer(sql(
      "select c3 from dest8 where c3 = 'abc' union select c3 from dest8  where ni(c3 = 'abc')"),
      sql("select c3 from dest8_parquet where c3 = 'abc' union select c3 from " +
          "dest8_parquet where c3 = 'abc'"))
    checkAnswer(sql("select c3 from dest8 where c3 = 'abc' union all " +
                    "select c3 from dest8 where ni(c3 ='abc')"),
      sql("select c3 from dest8_parquet where c3 = 'abc' union all select c3 from " +
          "dest8_parquet  where c3 = 'abc'"))
  }

  test("test SI with more than 2 Union " +
       "and Union All with different table donotPushtoSI operations") {
    sql("create table dest9 (c1 string,c2 int,c3 string,c5 string) STORED AS " +
        "carbondata")
    sql("insert into dest9 values('a',1,'abc','b')")
    sql("create table dest9_parquet stored as parquet select * from dest9")
    sql("create table dest9_parquet1 stored as parquet select * from dest9")
    sql("create table dest91 STORED AS carbondata select * from dest9")
    sql("create index index_dest9 on table dest9 (c3) AS 'carbondata'")
    sql("create index index_dest91 on table dest91 (c3) AS 'carbondata'")
    checkAnswer(sql(
      "select c3 from dest9 where c3 = 'abc' union select c3 from dest91  " +
      "where c3 = 'abc' union select c3 from dest91  where c3 != 'abc'"),
      sql(
        "select c3 from dest9_parquet where c3 = 'abc' union select c3 from dest9_parquet1" +
        " where c3 = 'abc' union select c3 from dest9_parquet1  where c3 != 'abc'"))

    checkAnswer(sql(
      "select c3 from dest9 where c3 = 'abc' union all select c3 from dest91 " +
      "where c3 = 'abc' union all select c3 from dest91  where c3 != 'abc'"),
      sql(
        "select c3 from dest9_parquet where c3 = 'abc' union all select c3 from " +
        "dest9_parquet1 where c3 = 'abc' union all select c3 from dest9_parquet1 " +
        "where c3 != 'abc'"))
    checkAnswer(sql(
      "select c3 from dest9 where c3 like '%bc' " +
      "union select c3 from dest91  where c3 not like '%bc'"),
      sql("select c3 from dest9_parquet where c3 like '%bc' union select c3 from " +
          "dest9_parquet1 where c3 not like '%bc'"))
    checkAnswer(sql("select c3 from dest9 where c3 like '%bc' union all " +
                    "select c3 from dest91 where c3 not like '%bc'"),
      sql("select c3 from dest9_parquet where c3 like '%bc' union all select c3 from " +
          "dest9_parquet1  where c3 not like '%bc'"))
    checkAnswer(sql(
      "select c3 from dest9 where c3 in ('abc') " +
      "union select c3 from dest91  where c3 not in ('abc')"),
      sql("select c3 from dest9_parquet where c3 in ('abc') union select c3 from " +
          "dest9_parquet1 where c3 not in ('abc')"))
    checkAnswer(sql("select c3 from dest9 where c3 in ('abc') union all " +
                    "select c3 from dest91 where c3 not in ('abc')"),
      sql("select c3 from dest9_parquet where c3 in ('abc') union all select c3 from " +
          "dest9_parquet1  where c3 not in ('abc')"))
    checkAnswer(sql(
      "select c3 from dest9 where c3 = 'abc' union select c3 from dest91  where ni(c3 = 'abc')"),
      sql("select c3 from dest9_parquet where c3 = 'abc' union select c3 from " +
          "dest9_parquet1 where c3 = 'abc'"))
    checkAnswer(sql("select c3 from dest9 where c3 = 'abc' union all " +
                    "select c3 from dest91 where ni(c3 ='abc')"),
      sql("select c3 from dest9_parquet where c3 = 'abc' union all select c3 from " +
          "dest9_parquet1  where c3 = 'abc'"))
  }

  test("test update and delete operation on SI") {
    sql("drop table if exists test")
    sql(
      "create table test (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table test""")
    sql("create index index_test on table test (c3) AS 'carbondata'")
    // delete on index table
    var ex = intercept[RuntimeException] {
      sql("delete from index_test d where d.c3='bb'").collect()
    }
    assert(ex.getMessage.contains("Delete is not permitted on Index Table"))
    // update on index table
    ex = intercept[RuntimeException] {
      sql("update index_test set(c3) = ('zz')")
    }
    assert(ex.getMessage.contains("Update is not permitted on Index Table"))
    sql("drop table if exists test")
  }

  test("test SI with delete operation when parent and child table segments are not in sync") {
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    try {
      sql("drop table if exists test")
      sql("create table test (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table test""")
      sql("create index index_test on table test (c3) AS 'carbondata'")
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table test""")
      sql("delete from table index_test where segment.ID in(1)")
      sql("clean files for table index_test options('force'='true')")
      assert(sql("show segments on index_test").collect().length == 1)
      sql("delete from test where c3='bbb'")
      val df = sql("select * from test where c3='dd'").queryExecution.sparkPlan
      assert(isFilterPushedDownToSI(df))
      assert(sql("show segments on index_test").collect().length == 2)
      checkAnswer(sql("select * from test where c3='dd'"),
        Seq(Row("d", 4, "dd", "ddd"), Row("d", 4, "dd", "ddd")))
      CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED,
            CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED_DEFAULT)
      sql("drop table if exists test")
    } finally {
      CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED,
            CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED_DEFAULT)
    }
  }


  override def afterAll: Unit = {
    dropIndexAndTable()
  }

  private def dropIndexAndTable(): Unit = {
    sql("drop index if exists index_dest1 on dest")
    sql("drop index if exists index_dest2 on dest")
    sql("drop table if exists dest")
    sql("drop index if exists index_source1 on source")
    sql("drop index if exists index_source2 on source")
    sql("drop table if exists source")
    sql("drop index if exists index_test1 on test")
    sql("drop table if exists test")
    sql("drop index if exists si3 on t10")
    sql("drop table if exists t10")
    sql("drop index if exists index_test2 on test2")
    sql("drop table if exists test2")
    sql("drop index if exists index_dest21 on dest2")
    sql("drop table if exists dest2")
    sql("drop index if exists idx_test3 on test3")
    sql("drop table if exists test3")
    sql("drop index if exists index_dest3 on dest3")
    sql("drop table if exists dest3")
    sql("drop table if exists dest3_parquet")
    sql("drop index if exists index_dest4 on dest4")
    sql("drop table if exists dest4")
    sql("drop table if exists dest4_parquet")
    sql("drop table if exists dest4_parquet1")
    sql("drop index if exists index_dest41 on dest41")
    sql("drop table if exists dest41")
    sql("drop index if exists index_dest5 on dest5")
    sql("drop table if exists dest5")
    sql("drop table if exists dest5_parquet")
    sql("drop table if exists dest5_parquet1")
    sql("drop index if exists index_dest51 on dest51")
    sql("drop table if exists dest51")
    sql("drop index if exists index_dest6 on dest6")
    sql("drop table if exists dest6")
    sql("drop table if exists dest6_parquet")
    sql("drop index if exists index_dest7 on dest7")
    sql("drop table if exists dest7")
    sql("drop table if exists dest7_parquet")
    sql("drop index if exists index_dest8 on dest8")
    sql("drop table if exists dest8")
    sql("drop table if exists dest8_parquet")
    sql("drop index if exists index_dest9 on dest9")
    sql("drop table if exists dest9")
    sql("drop table if exists dest9_parquet")
    sql("drop table if exists dest9_parquet1")
    sql("drop index if exists index_dest91 on dest91")
    sql("drop table if exists dest91")
  }

}
