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
package org.apache.carbondata.spark.testsuite.iud

import java.io.File

import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.test.SparkTestQueryExecutor
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{CarbonEnv, Row, SaveMode}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath


class DeleteCarbonTableTestCase extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("use default")
    sql("drop database  if exists iud_db cascade")
    sql("create database  iud_db")

    sql("""create table iud_db.source2 (c11 string,c22 int,c33 string,c55 string, c66 int) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/source2.csv' INTO table iud_db.source2""")
    sql("use iud_db")
  }
  test("delete data from carbon table with alias [where clause ]") {
    sql("""create table iud_db.dest (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud_db.dest""")
    sql("""delete from iud_db.dest d where d.c1 = 'a'""").show
    checkAnswer(
      sql("""select c2 from iud_db.dest"""),
      Seq(Row(2), Row(3),Row(4), Row(5))
    )
  }
  test("delete data from  carbon table[where clause ]") {
    sql("""drop table if exists iud_db.dest""")
    sql("""create table iud_db.dest (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud_db.dest""")
    sql("""delete from iud_db.dest where c2 = 2""").show
    checkAnswer(
      sql("""select c1 from iud_db.dest"""),
      Seq(Row("a"), Row("c"), Row("d"), Row("e"))
    )
  }
  test("delete data from  carbon table[where IN  ]") {
    sql("""drop table if exists iud_db.dest""")
    sql("""create table iud_db.dest (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud_db.dest""")
    sql("""delete from dest where c1 IN ('d', 'e')""").show
    checkAnswer(
      sql("""select c1 from dest"""),
      Seq(Row("a"), Row("b"),Row("c"))
    )
  }

  test("delete data from  carbon table[with alias No where clause]") {
    sql("""drop table if exists iud_db.dest""")
    sql("""create table iud_db.dest (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud_db.dest""")
    sql("""delete from iud_db.dest a""").show
    checkAnswer(
      sql("""select c1 from iud_db.dest"""),
      Seq()
    )
  }
  test("delete data from  carbon table[No alias No where clause]") {
    sql("""drop table if exists iud_db.dest""")
    sql("""create table iud_db.dest (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud_db.dest""")
    sql("""delete from dest""").show()
    checkAnswer(
      sql("""select c1 from dest"""),
      Seq()
    )
  }

  test("delete data from  carbon table[ JOIN with another table ]") {
    sql("""drop table if exists iud_db.dest""")
    sql("""create table iud_db.dest (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud_db.dest""")
    sql(""" DELETE FROM dest t1 INNER JOIN source2 t2 ON t1.c1 = t2.c11""").show(truncate = false)
    checkAnswer(
      sql("""select c1 from iud_db.dest"""),
      Seq(Row("c"), Row("d"), Row("e"))
    )
  }

  test("delete data from  carbon table[where numeric condition  ]") {
    sql("""drop table if exists iud_db.dest""")
    sql("""create table iud_db.dest (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud_db.dest""")
    sql("""delete from  iud_db.dest where c2 >= 4""").show()
    checkAnswer(
      sql("""select count(*) from iud_db.dest"""),
      Seq(Row(3))
    )
  }

  test("partition delete data from carbon table with alias [where clause ]") {
    sql("drop table if exists iud_db.dest")
    sql("""create table iud_db.dest (c1 string,c2 int,c5 string) PARTITIONED BY(c3 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud_db.dest""")
    sql("""delete from iud_db.dest d where d.c1 = 'a'""").show
    checkAnswer(
      sql("""select c2 from iud_db.dest"""),
      Seq(Row(2), Row(3),Row(4), Row(5))
    )
  }

  test("partition delete data from  carbon table[where clause ]") {
    sql("""drop table if exists iud_db.dest""")
    sql("""create table iud_db.dest (c1 string,c2 int,c5 string) PARTITIONED BY(c3 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud_db.dest""")
    sql("""delete from iud_db.dest where c2 = 2""").show
    checkAnswer(
      sql("""select c1 from iud_db.dest"""),
      Seq(Row("a"), Row("c"), Row("d"), Row("e"))
    )
  }

  test("Records more than one pagesize after delete operation ") {
    sql("DROP TABLE IF EXISTS carbon2")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 2000000)
      .map(x => (x+"a", "b", x))
      .toDF("c1", "c2", "c3")
    df.write
      .format("carbondata")
      .option("tableName", "carbon2")
      .option("tempCSV", "true")
      .option("compress", "true")
      .mode(SaveMode.Overwrite)
      .save()

    checkAnswer(sql("select count(*) from carbon2"), Seq(Row(2000000)))

    sql("delete from carbon2 where c1 = '99999a'").show()

    checkAnswer(sql("select count(*) from carbon2"), Seq(Row(1999999)))

    checkAnswer(sql("select * from carbon2 where c1 = '99999a'"), Seq())

    sql("DROP TABLE IF EXISTS carbon2")
  }

  test("test select query after compaction, delete and clean files") {
    sql("drop table if exists select_after_clean")
    sql("create table select_after_clean(id int, name string) STORED AS carbondata")
    sql("insert into select_after_clean select 1,'abc'")
    sql("insert into select_after_clean select 2,'def'")
    sql("insert into select_after_clean select 3,'uhj'")
    sql("insert into select_after_clean select 4,'frg'")
    sql("alter table select_after_clean compact 'minor'")
    sql("clean files for table select_after_clean")
    sql("delete from select_after_clean where name='def'")
    sql("clean files for table select_after_clean")
    assertResult(false)(new File(
      CarbonTablePath.getSegmentPath(s"$storeLocation/iud_db.db/select_after_clean", "0")).exists())
    checkAnswer(sql("""select * from select_after_clean"""),
      Seq(Row(1, "abc"), Row(3, "uhj"), Row(4, "frg")))
  }

  test("test number of update table status files after delete query where no records are deleted") {
    sql("drop table if exists update_status_files")
    sql("create table update_status_files(name string,age int) STORED AS carbondata")
    sql("insert into update_status_files select 'abc',1")
    sql("insert into update_status_files select 'def',2")
    sql("insert into update_status_files select 'xyz',4")
    sql("insert into update_status_files select 'abc',6")
    sql("alter table update_status_files compact 'minor'")
    sql("delete from update_status_files where age=3").show()
    sql("delete from update_status_files where age=5").show()
    val carbonTable = CarbonEnv
      .getCarbonTable(Some("iud_db"), "update_status_files")(sqlContext.sparkSession)
    val metaPath = carbonTable.getMetadataPath
    val files = FileFactory.getCarbonFile(metaPath)
    val result = CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.getClass
    if(result.getCanonicalName.contains("CarbonFileMetastore")) {
      assert(files.listFiles(new CarbonFileFilter {
        override def accept(file: CarbonFile): Boolean = !file.isDirectory
      }).length == 2)
    }
    else
      assert(files.listFiles().length == 2)

    sql("drop table update_status_files")
  }

  test("tuple-id for partition table ") {
    sql("drop table if exists iud_db.dest_tuple_part")
    sql(
      """create table iud_db.dest_tuple_part (c1 string,c2 int,c5 string) PARTITIONED BY(c3 string) STORED AS carbondata""".stripMargin)
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud_db.dest_tuple_part""".stripMargin)
    sql("drop table if exists iud_db.dest_tuple")
    sql(
      """create table iud_db.dest_tuple (c1 string,c2 int,c5 string,c3 string) STORED AS carbondata""".stripMargin)
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud_db.dest_tuple""")

    val dataframe_part = sql("select getTupleId() as tupleId from iud_db.dest_tuple_part").collect()
    val listOfTupleId_part = dataframe_part.map(df => df.get(0).toString).sorted
    assert(listOfTupleId_part(0).startsWith("c3=aa/0/0-100100000100001_batchno0-0-0-") &&
           listOfTupleId_part(0).endsWith("/0/0/0"))
    assert(listOfTupleId_part(1).startsWith("c3=bb/0/0-100100000100002_batchno0-0-0-") &&
           listOfTupleId_part(1).endsWith("/0/0/0"))
    assert(listOfTupleId_part(2).startsWith("c3=cc/0/0-100100000100003_batchno0-0-0-") &&
           listOfTupleId_part(2).endsWith("/0/0/0"))
    assert(listOfTupleId_part(3).startsWith("c3=dd/0/0-100100000100004_batchno0-0-0-") &&
           listOfTupleId_part(3).endsWith("/0/0/0"))
    assert(listOfTupleId_part(4).startsWith("c3=ee/0/0-100100000100005_batchno0-0-0-") &&
           listOfTupleId_part(4).endsWith("/0/0/0"))

    val dataframe = sql("select getTupleId() as tupleId from iud_db.dest_tuple")
    val listOfTupleId = dataframe.collect().map(df => df.get(0).toString).sorted
    assert(
      listOfTupleId(0).contains("0/0/0-0_batchno0-0-0-") && listOfTupleId(0).endsWith("/0/0/0"))
    assert(
      listOfTupleId(1).contains("0/0/0-0_batchno0-0-0-") && listOfTupleId(1).endsWith("/0/0/1"))
    assert(
      listOfTupleId(2).contains("0/0/0-0_batchno0-0-0-") && listOfTupleId(2).endsWith("/0/0/2"))
    assert(
      listOfTupleId(3).contains("0/0/0-0_batchno0-0-0-") && listOfTupleId(3).endsWith("/0/0/3"))
    assert(
      listOfTupleId(4).contains("0/0/0-0_batchno0-0-0-") && listOfTupleId(4).endsWith("/0/0/4"))

    val carbonTable_part = CarbonEnv.getInstance(SparkTestQueryExecutor.spark).carbonMetaStore
      .lookupRelation(Option("iud_db"), "dest_tuple_part")(SparkTestQueryExecutor.spark)
      .asInstanceOf[CarbonRelation].carbonTable

    val carbonTable = CarbonEnv.getInstance(SparkTestQueryExecutor.spark).carbonMetaStore
      .lookupRelation(Option("iud_db"), "dest_tuple")(SparkTestQueryExecutor.spark)
      .asInstanceOf[CarbonRelation].carbonTable

    val carbonDataFilename = new File(carbonTable.getTablePath + "/Fact/Part0/Segment_0/")
      .listFiles().filter(fn => fn.getName.endsWith(".carbondata"))
    val blockId = CarbonUtil.getBlockId(carbonTable.getAbsoluteTableIdentifier,
      carbonDataFilename(0).getAbsolutePath,
      "0",
      carbonTable.isTransactionalTable,
      CarbonUtil.isStandardCarbonTable(carbonTable))

    assert(blockId.startsWith("Part0/Segment_0/part-0-0_batchno0-0-0-"))
    val carbonDataFilename_part = new File(carbonTable_part.getTablePath + "/c3=aa").listFiles()
      .filter(fn => fn.getName.endsWith(".carbondata"))
    val blockId_part = CarbonUtil.getBlockId(carbonTable.getAbsoluteTableIdentifier,
      carbonDataFilename_part(0).getAbsolutePath,
      "0",
      carbonTable.isTransactionalTable,
      CarbonUtil.isStandardCarbonTable(carbonTable))
    assert(blockId_part.startsWith("Part0/Segment_0/part-0-100100000100001_batchno0-0-0-"))
    val segment = Segment.getSegment("0", carbonTable.getTablePath)
    val tableBlockPath = CarbonUpdateUtil
      .getTableBlockPath(listOfTupleId(0),
        carbonTable.getTablePath,
        CarbonUtil.isStandardCarbonTable(carbonTable))
    val segment_part = Segment.getSegment("0", carbonTable_part.getTablePath)
    val tableBl0ckPath_part = CarbonUpdateUtil
      .getTableBlockPath(listOfTupleId_part(0),
        carbonTable_part.getTablePath,
        CarbonUtil.isStandardCarbonTable(carbonTable_part))
    assert(tableBl0ckPath_part.endsWith("iud_db.db/dest_tuple_part/c3=aa"))
    assert(tableBlockPath.endsWith("iud_db.db/dest_tuple/Fact/Part0/Segment_0"))

    sql("drop table if exists iud_db.dest_tuple_part")
    sql("drop table if exists iud_db.dest_tuple")

  }

  test("block deleting records from table which has index datamap") {
    sql("drop table if exists test_dm_index")

    sql("create table test_dm_index (a string, b string, c string) STORED AS carbondata")
    sql("insert into test_dm_index select 'ccc','bbb','ccc'")

    sql(
      s"""
         | CREATE DATAMAP dm_test_dm_index ON TABLE test_dm_index
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='a', 'BLOOM_SIZE'='640000')
      """.stripMargin)

    assert(intercept[UnsupportedOperationException] {
      sql("delete from test_dm_index where a = 'ccc'")
    }.getMessage.contains("Delete operation is not supported for table which has index datamaps"))

    sql("drop table if exists test_dm_index")
  }

  test("test delete on table with decimal column") {
    sql("drop table if exists decimal_table")
    sql(
      s"""create table decimal_table(smallIntField smallInt,intField int,bigIntField bigint,floatField float,
          doubleField double,decimalField decimal(25, 4),timestampField timestamp,dateField date,stringField string,
          varcharField varchar(10),charField char(10))stored as carbondata
      """.stripMargin)
    sql(s"load data local inpath '$resourcesPath/decimalData.csv' into table decimal_table")
    val frame = sql("select decimalfield from decimal_table where smallIntField = -1 or smallIntField = 3")
    sql(s"delete from decimal_table where smallIntField = 2")
    checkAnswer(frame, Seq(
      Row(-1.1234),
      Row(3.1234)
    ))
    sql("drop table if exists decimal_table")
  }

  test("[CARBONDATA-3491] Return updated/deleted rows count when execute update/delete sql") {
    sql("drop table if exists test_return_row_count")

    sql("create table test_return_row_count (a string, b string, c string) STORED AS carbondata").show()
    sql("insert into test_return_row_count select 'aaa','bbb','ccc'").show()
    sql("insert into test_return_row_count select 'bbb','bbb','ccc'").show()
    sql("insert into test_return_row_count select 'ccc','bbb','ccc'").show()
    sql("insert into test_return_row_count select 'ccc','bbb','ccc'").show()

    checkAnswer(sql("delete from test_return_row_count where a = 'aaa'"),
        Seq(Row(1))
    )
    checkAnswer(sql("select * from test_return_row_count"),
        Seq(Row("bbb", "bbb", "ccc"), Row("ccc", "bbb", "ccc"), Row("ccc", "bbb", "ccc"))
    )

    sql("drop table if exists test_return_row_count").show()
  }

  test("[CARBONDATA-3561] Fix incorrect results after execute delete/update operation if there are null values") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER , "true")
    val tableName = "fix_incorrect_results_for_iud"
    sql(s"drop table if exists ${tableName}")

    sql(s"create table ${tableName} (a string, b string, c string) STORED AS carbondata").show()
    sql(s"""insert into table ${tableName}
              select '1','1','2017' union all
              select '2','2','2017' union all
              select '3','3','2017' union all
              select '4','4','2017' union all
              select '5',null,'2017' union all
              select '6',null,'2017' union all
              select '7','7','2017' union all
              select '8','8','2017' union all
              select '9',null,'2017' union all
              select '10',null,'2017'""").show()

    checkAnswer(sql(s"select count(1) from ${tableName} where b is null"), Seq(Row(4)))

    checkAnswer(sql(s"delete from ${tableName} where b ='4'"), Seq(Row(1)))
    checkAnswer(sql(s"delete from ${tableName} where a ='9'"), Seq(Row(1)))
    checkAnswer(sql(s"update ${tableName} set (b) = ('10') where a = '10'"), Seq(Row(1)))

    checkAnswer(sql(s"select count(1) from ${tableName} where b is null"), Seq(Row(2)))
    checkAnswer(sql(s"select * from ${tableName} where a = '1'"), Seq(Row("1", "1", "2017")))
    checkAnswer(sql(s"select * from ${tableName} where a = '10'"), Seq(Row("10", "10", "2017")))

    sql(s"drop table if exists ${tableName}").show()
  }

  override def afterAll {
    sql("use default")
    sql("drop database  if exists iud_db cascade")
  }
}