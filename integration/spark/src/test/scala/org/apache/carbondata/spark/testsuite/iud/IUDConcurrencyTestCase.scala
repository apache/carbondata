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

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.concurrent.{Callable, Executors, Future}

import mockit.{Mock, MockUp}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.command.mutation.{CarbonProjectForDeleteCommand, CarbonProjectForUpdateCommand}
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.types.StructType
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory

class IUDConcurrencyTestCase extends QueryTest with BeforeAndAfterAll {

  val ONE_LOAD_SIZE = 5
  var testData: DataFrame = _

  override def beforeAll(): Unit = {
    sql("DROP DATABASE IF EXISTS iud_concurrency CASCADE")
    sql("CREATE DATABASE iud_concurrency")
    sql("USE iud_concurrency")

    buildTestData()
  }

  private def buildTestData(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
    import sqlContext.implicits._
    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    testData = sqlContext.sparkSession.sparkContext.parallelize(1 to ONE_LOAD_SIZE)
      .map(value => (value, new Date(sdf.parse("2015-07-" + (value % 10 + 10)).getTime),
        "china", "aaa" + value, "phone" + 555 * value, "ASD" + (60000 + value), 14999 + value,
        "ordersTable" + value))
      .toDF("o_id", "o_date", "o_country", "o_name",
       "o_phonetype", "o_serialname", "o_salary", "o_comment")
  }

  private def createTable(tableName: String, schema: StructType): Unit = {
    val schemaString = schema.fields.map(x => x.name + " " + x.dataType.typeName).mkString(", ")
    sql(s"CREATE TABLE $tableName ($schemaString) stored as carbondata tblproperties" +
      s"('sort_scope'='local_sort', 'sort_columns'='o_country, o_name, o_phonetype, o_serialname," +
      s"o_comment")
  }

  // ----------------------- Insert and Update ------------------------
  // update -> insert -> update
  test("Update should success when insert completes before it") {
    val insertSql = "insert into orders select * from orders_temp_table"
    val updateSql = "update orders set (o_country)=('newCountry') where o_country='china'"

    val mockInsert = new MockUp[CarbonProjectForUpdateCommand]() {
      @Mock
      def mockForConcurrentInsertTest(): Unit = {
        sql(insertSql)
      }
    }

    val updateFuture = runSqlAsync(updateSql)
    assert(updateFuture.get().contains("PASS"))
    mockInsert.tearDown()

    checkAnswer(
      sql("select count(1) from orders o_country='newCountry'"), Seq(Row(ONE_LOAD_SIZE)))
    checkAnswer(
      sql("select count(1) from orders where o_country='china'"), Seq(Row(ONE_LOAD_SIZE)))
  }

  // insert -> update -> insert
  test("Update should success when it completes before insert") {
    val insertSql = "insert into orders select * from orders_temp_table"
    val updateSql = "update orders set (o_country)=('newCountry') where o_country='china'"

    val mockUpdate = new MockUp[CarbonDataRDDFactory.type]() {
      @Mock
      def mockForConcurrentUpdateTest(): Unit = {
        this.tearDown()
        sql(updateSql)
      }
    }

    val insertFuture = runSqlAsync(insertSql)
    assert(insertFuture.get().contains("PASS"))

    checkAnswer(
      sql("select count(1) from orders o_country='newCountry'"), Seq(Row(ONE_LOAD_SIZE * 2)))
    checkAnswer(
      sql("select count(1) from orders where o_country='china'"), Seq(Row(ONE_LOAD_SIZE)))
  }

  // ----------------------- Insert and Delete ------------------------
  // delete -> insert -> delete
  test("Delete should success when insert completes before it") {
    val insertSql = "insert into orders select * from orders_temp_table"
    val deleteSql = "delete from orders where o_country='china'"

    val mockInsert = new MockUp[CarbonProjectForDeleteCommand]() {
      @Mock
      def mockForConcurrentInsertTest(): Unit = {
        sql(insertSql)
      }
    }

    val deleteFuture = runSqlAsync(deleteSql)
    assert(deleteFuture.get().contains("PASS"))
    mockInsert.tearDown()

    checkAnswer(
      sql("select count(1) from orders o_country='newCountry'"), Seq(Row(ONE_LOAD_SIZE * 2)))
    checkAnswer(
      sql("select count(1) from orders where o_country='china'"), Seq(Row(ONE_LOAD_SIZE)))
  }

  // insert -> delete -> insert
  test("Delete should success when it completes before insert") {
    val insertSql = "insert into orders select * from orders_temp_table"
    val deleteSql = "delete from orders where o_country='china'"

    val mockDelete = new MockUp[CarbonDataRDDFactory.type]() {
      @Mock
      def mockForConcurrentDeleteTest(): Unit = {
        this.tearDown()
        sql(deleteSql)
      }
    }

    val insertFuture = runSqlAsync(insertSql)
    assert(insertFuture.get().contains("PASS"))

    checkAnswer(
      sql("select count(1) from orders o_country='newCountry'"), Seq(Row(ONE_LOAD_SIZE * 2)))
    checkAnswer(
      sql("select count(1) from orders where o_country='china'"), Seq(Row(ONE_LOAD_SIZE)))
  }

  // ----------------------- Insert and Delete ------------------------
  // delete -> insert -> delete
  test("Delete should success when insert completes before it") {
    val insertSql = "insert into orders select * from orders_temp_table"
    val deleteSql = "delete from orders where o_country='china'"

    val mockInsert = new MockUp[CarbonProjectForDeleteCommand]() {
      @Mock
      def mockForConcurrentInsertTest(): Unit = {
        sql(insertSql)
      }
    }

    val deleteFuture = runSqlAsync(deleteSql)
    assert(deleteFuture.get().contains("PASS"))
    mockInsert.tearDown()

    checkAnswer(
      sql("select count(1) from orders o_country='newCountry'"), Seq(Row(ONE_LOAD_SIZE * 2)))
    checkAnswer(
      sql("select count(1) from orders where o_country='china'"), Seq(Row(ONE_LOAD_SIZE)))
  }

  // insert -> delete -> insert
  test("Delete should success when it completes before insert") {
    val insertSql = "insert into orders select * from orders_temp_table"
    val deleteSql = "delete from orders where o_country='china'"

    val mockDelete = new MockUp[CarbonDataRDDFactory.type]() {
      @Mock
      def mockForConcurrentDeleteTest(): Unit = {
        this.tearDown()
        sql(deleteSql)
      }
    }

    val insertFuture = runSqlAsync(insertSql)
    assert(insertFuture.get().contains("PASS"))

    checkAnswer(
      sql("select count(1) from orders o_country='newCountry'"), Seq(Row(ONE_LOAD_SIZE * 2)))
    checkAnswer(
      sql("select count(1) from orders where o_country='china'"), Seq(Row(ONE_LOAD_SIZE)))
  }

  // ----------------------- Update and Update ------------------------
  // 1st update -> 2nd update -> 1st update
  // segments updated by two update operations are disjoint
  test("Second update should success when segments are not updated by first update") {
    val firstUpdateSql = "update orders set (o_country)=('thirdCountry') where o_country='china'"
    val secondUpdateSql =
      "update orders set (o_country)=('fourthCountry') where o_country='newCountry'"

    val mockUpdate = new MockUp[CarbonProjectForUpdateCommand]() {
      @Mock
      def mockForConcurrentUpdateTest(): Unit = {
        this.tearDown()
        sql(secondUpdateSql)
      }
    }

    val deleteFuture = runSqlAsync(firstUpdateSql)
    assert(deleteFuture.get().contains("PASS"))
    mockUpdate.tearDown()

    checkAnswer(sql("select count(1) from orders o_country='china'"), Seq(Row(0)))
    checkAnswer(sql("select count(1) from orders o_country='newCountry'"), Seq(Row(0)))
    checkAnswer(sql("select count(1) from orders where o_country='thirdCountry'"),
      Seq(Row(ONE_LOAD_SIZE)))
    checkAnswer(sql("select count(1) from orders where o_country='fourthCountry'"),
      Seq(Row(ONE_LOAD_SIZE * 2)))
  }

  // 1st update -> 2nd update -> 1st update
  test("Second update should fail when segments are updated by first update") {
    val firstUpdateSql = "update orders set (o_country)=('fifthCountry')" +
      " where o_country='thirdCountry'"
    val secondUpdateSql = "update orders set (o_name)=('newName') where o_name='aaa1'"

    val mockUpdate = new MockUp[CarbonProjectForUpdateCommand]() {
      @Mock
      def mockForConcurrentUpdateTest(): Unit = {
        this.tearDown()
        val secondUpdateException = intercept[ConcurrentOperationException] {
          sql(secondUpdateSql)
        }
        assert(secondUpdateException.getMessage.contains(
          "Update operation failed because there is a concurrent " +
          "operation conflict. please try later."))
      }
    }

    val firstUpdateFuture = runSqlAsync(firstUpdateSql)
    assert(firstUpdateFuture.get().contains("PASS"))

    checkAnswer(
      sql("select count(1) from orders o_country='fifthCountry'"), Seq(Row(ONE_LOAD_SIZE)))
    checkAnswer(
      sql("select count(1) from orders where o_name='newName'"), Seq(Row(0)))
  }

  // ----------------------- Delete and Delete ------------------------
  // 1st delete -> 2nd delete -> 1st delete
  // segments deleted by two delete operations are disjoint
  test("Second delete should success when segments are not deleted by first delete") {
    sql("drop table if exists temp_table")
    sql("create table temp_table (c1 int, c2 String, c3 String) stored as carbondata")
    sql("insert into temp_table select 1, 'aaa', 'xxxxx'")
    sql("insert into temp_table select 2, 'bbb', 'xxxxx'")

    val firstDeleteSql = "delete from temp_table where c1=1"
    val secondDeleteSql = "delete from temp_table where c1=2"

    val mockUpdate = new MockUp[CarbonProjectForDeleteCommand]() {
      @Mock
      def mockForConcurrentDeleteTest(): Unit = {
        this.tearDown()
        sql(secondDeleteSql)
      }
    }

    val firstDeleteFuture = runSqlAsync(firstDeleteSql)
    assert(firstDeleteFuture.get().contains("PASS"))

    checkAnswer(sql("select count(1) from temp_table where c1=1"), Seq(Row(0)))
    checkAnswer(sql("select count(1) from temp_table where c1=2"), Seq(Row(0)))
  }

  // 1st delete -> 2nd delete -> 1st delete
  // segments deleted by two delete operations have intersection
  test("Second delete should fail when segments are deleted by first delete") {
    sql("DROP TABLE IF EXISTS table_1")
    sql("DROP TABLE IF EXISTS temp_table")
    sql("create table table_1 (c1 int, c2 String, c3 String) stored as carbondata")
    sql("create table temp_table (c1 int, c2 String, c3 String) stored as carbondata")
    sql("insert into temp_table select 1, 'aaa', 'xxxx'")
    sql("insert into temp_table select 2, 'bbb', 'xxxx'")
    sql("insert into table_1 select * from temp_table")
    sql("insert into temp_1 select 2, 'ccc', 'xxxx'")

    val firstDeleteSql = "delete from table_1 where c1=1"
    val secondDeleteSql = "delete from table_1 where c1=2"

    val mockDelete = new MockUp[CarbonProjectForDeleteCommand]() {
      @Mock
      def mockForConcurrentDeleteTest(): Unit = {
        this.tearDown()
        val secondDeleteException = intercept[ConcurrentOperationException] {
          sql(secondDeleteSql)
        }
        assert(secondDeleteException.getMessage.contains(
          "Delete operation failed because there is a concurrent " +
            "operation conflict. please try later."))
      }
    }

    val firstDeleteFuture = runSqlAsync(firstDeleteSql)
    assert(firstDeleteFuture.get().contains("PASS"))

    checkAnswer(sql("select count(1) from table_1 where c1=1"), Seq(Row(0)))
    checkAnswer(sql("select count(1) from table_1 where c1=2"), Seq(Row(0)))
  }

  // ----------------------- Update and Delete ------------------------
  // delete -> update -> delete
  // segments operated by update and delete are disjoint
  test("Update should success when segments are not deleted by delete operation") {
    sql("drop table if exists temp_table")
    sql("create table temp_table (c1 int, c2 String, c3 String) stored as carbondata")
    sql("insert into temp_table select 1, 'aaa', 'xxxxx'")
    sql("insert into temp_table select 2, 'bbb', 'xxxxx'")

    val deleteSql = "delete from temp_table where c1=1"
    val updateSql = "update temp_table set (c1)=(3) where c1=2"

    val mockUpdate = new MockUp[CarbonProjectForDeleteCommand]() {
      @Mock
      def mockForConcurrentUpdateTest(): Unit = {
        this.tearDown()
        sql(updateSql)
      }
    }

    val deleteFuture = runSqlAsync(deleteSql)
    assert(deleteFuture.get().contains("PASS"))

    checkAnswer(sql("select count(1) from temp_table where c1=1"), Seq(Row(0)))
    checkAnswer(sql("select count(1) from temp_table where c1=2"), Seq(Row(0)))
    checkAnswer(sql("select count(1) from temp_table where c1=3"), Seq(Row(1)))
  }

  // delete -> update -> delete
  // segments operated by update and delete have intersection
  test("Update should fail when segments are deleted by delete operation") {
    sql("DROP TABLE IF EXISTS table_1")
    sql("DROP TABLE IF EXISTS temp_table")
    sql("create table table_1 (c1 int, c2 String, c3 String) stored as carbondata")
    sql("create table temp_table (c1 int, c2 String, c3 String) stored as carbondata")
    sql("insert into temp_table select 1, 'aaa', 'xxxx'")
    sql("insert into temp_table select 2, 'bbb', 'xxxx'")
    sql("insert into table_1 select * from temp_table")
    sql("insert into temp_1 select 2, 'ccc', 'xxxx'")

    val deleteSql = "delete from table_1 where c1=1"
    val updateSql = "update table_1 set (c1)=(3) where c1=2"

    val mockUpdate = new MockUp[CarbonProjectForDeleteCommand]() {
      @Mock
      def mockForConcurrentUpdateTest(): Unit = {
        this.tearDown()
        val updateException = intercept[ConcurrentOperationException] {
          sql(updateSql)
        }
        assert(updateException.getMessage.contains(
          "Delete operation failed because there is a concurrent " +
            "operation conflict. please try later."))
      }
    }

    val firstDeleteFuture = runSqlAsync(deleteSql)
    assert(firstDeleteFuture.get().contains("PASS"))

    checkAnswer(sql("select count(1) from table_1 where c1=1"), Seq(Row(0)))
    checkAnswer(sql("select count(1) from table_1 where c1=2"), Seq(Row(2)))
    checkAnswer(sql("select count(1) from table_1 where c1=3"), Seq(Row(0)))
  }

  // update -> delete -> update
  // segments operated by update and delete are disjoint
  test("Delete should success when segments are not updated by update operation") {
    sql("drop table if exists temp_table")
    sql("create table temp_table (c1 int, c2 String, c3 String) stored as carbondata")
    sql("insert into temp_table select 1, 'aaa', 'xxxxx'")
    sql("insert into temp_table select 2, 'bbb', 'xxxxx'")

    val deleteSql = "delete from temp_table where c1=1"
    val updateSql = "update temp_table set (c1)=(3) where c1=2"

    val mockDelete = new MockUp[CarbonProjectForUpdateCommand]() {
      @Mock
      def mockForConcurrentDeleteTest(): Unit = {
        this.tearDown()
        sql(deleteSql)
      }
    }

    val updateFuture = runSqlAsync(updateSql)
    assert(updateFuture.get().contains("PASS"))

    checkAnswer(sql("select count(1) from temp_table where c1=1"), Seq(Row(0)))
    checkAnswer(sql("select count(1) from temp_table where c1=2"), Seq(Row(0)))
    checkAnswer(sql("select count(1) from temp_table where c1=3"), Seq(Row(1)))
  }

  // update -> delete -> update
  // segments operated by update and delete have intersection
  test("Delete should fail when segments are updated by update operation") {
    sql("DROP TABLE IF EXISTS table_1")
    sql("DROP TABLE IF EXISTS temp_table")
    sql("create table table_1 (c1 int, c2 String, c3 String) stored as carbondata")
    sql("create table temp_table (c1 int, c2 String, c3 String) stored as carbondata")
    sql("insert into temp_table select 1, 'aaa', 'xxxx'")
    sql("insert into temp_table select 2, 'bbb', 'xxxx'")
    sql("insert into table_1 select * from temp_table")
    sql("insert into temp_1 select 2, 'ccc', 'xxxx'")

    val updateSql = "update table_1 set (c1)=(3) where c1=2"
    val deleteSql = "delete from table_1 where c1=1"

    val mockDelete = new MockUp[CarbonProjectForUpdateCommand]() {
      @Mock
      def mockForConcurrentDeleteTest(): Unit = {
        this.tearDown()
        val deleteException = intercept[ConcurrentOperationException] {
          sql(updateSql)
        }
        assert(deleteException.getMessage.contains(
          "Delete operation failed because there is a concurrent " +
            "operation conflict. please try later."))
      }
    }

    val updateFuture = runSqlAsync(updateSql)
    assert(updateFuture.get().contains("PASS"))

    checkAnswer(sql("select count(1) from table_1 where c1=1"), Seq(Row(1)))
    checkAnswer(sql("select count(1) from table_1 where c1=2"), Seq(Row(0)))
    checkAnswer(sql("select count(1) from table_1 where c1=3"), Seq(Row(2)))
  }


  val executorService = Executors.newFixedThreadPool(2)

  def runSqlAsync(sqlStr: String): Future[String] = {
    executorService.submit(new SqlRunner(sqlStr))
  }

  class SqlRunner(sqlQuery: String) extends Callable[String] {
    var result = "PASS"

    override def call(): String = {
      try {
        sql(sqlQuery).show()
      } catch {
        case exception: Exception =>
          LOGGER.error(exception.getMessage)
          result = "FAIL"
      }
      result
    }
  }

  override def afterAll(): Unit = {
    sql("DROP DATABASE IF EXISTS iud_concurrency CASCADE")
  }

}
