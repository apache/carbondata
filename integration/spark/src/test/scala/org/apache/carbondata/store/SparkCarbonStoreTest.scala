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

package org.apache.carbondata.store

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression
import org.apache.carbondata.core.scan.expression.{ColumnExpression, LiteralExpression}

class SparkCarbonStoreTest extends QueryTest with BeforeAndAfterAll {

  private var store: CarbonStore = _

  override def beforeAll {
    sql("DROP TABLE IF EXISTS t1")
    sql("CREATE TABLE t1 (" +
        "empno int, empname String, designation String, doj Timestamp, " +
        "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String," +
        "projectcode int, projectjoindate Timestamp, projectenddate Timestamp," +
        "attendance int,utilization int,salary int)" +
        "STORED AS carbondata")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE t1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")

    store = new SparkCarbonStore("test", storeLocation)
  }

  test("test CarbonStore.get, compare projection result") {
    val table = CarbonEnv.getCarbonTable(None, "t1")(sqlContext.sparkSession)
    val rows = store.scan(table.getAbsoluteTableIdentifier, Seq("empno", "empname").toArray)
    val sparkResult: Array[Row] = sql("select empno, empname from t1").collect()
    sparkResult.zipWithIndex.foreach { case (r: Row, i: Int) =>
      val carbonRow = rows.next()
      assertResult(r.get(0))(carbonRow.getData()(0))
      assertResult(r.get(1))(carbonRow.getData()(1))
    }
    assert(!rows.hasNext)
  }

  test("test CarbonStore.get, compare projection and filter result") {
    val table = CarbonEnv.getCarbonTable(None, "t1")(sqlContext.sparkSession)
    val filter = new EqualToExpression(
      new ColumnExpression("empno", DataTypes.INT),
      new LiteralExpression(10, DataTypes.INT))
    val rows = store.scan(table.getAbsoluteTableIdentifier, Seq("empno", "empname").toArray, filter)
    val sparkResult: Array[Row] = sql("select empno, empname from t1 where empno = 10").collect()
    sparkResult.zipWithIndex.foreach { case (r: Row, i: Int) =>
      val carbonRow = rows.next()
      assertResult(r.get(0))(carbonRow.getData()(0))
      assertResult(r.get(1))(carbonRow.getData()(1))
    }
    assert(!rows.hasNext)
  }

  test("test CarbonStore.sql") {
    val rows = store.sql("select empno, empname from t1 where empno = 10")
    val sparkResult: Array[Row] = sql("select empno, empname from t1 where empno = 10").collect()
    sparkResult.zipWithIndex.foreach { case (r: Row, i: Int) =>
      val carbonRow = rows.next()
      assertResult(r.get(0))(carbonRow.getData()(0))
      assertResult(r.get(1))(carbonRow.getData()(1))
    }
    assert(!rows.hasNext)
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS t1")
  }
}