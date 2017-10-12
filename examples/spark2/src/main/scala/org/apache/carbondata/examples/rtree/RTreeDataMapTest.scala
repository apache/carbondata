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

package org.apache.carbondata.examples.rtree

import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier


class RTreeDataMapTest extends QueryTest with BeforeAndAfterAll {

  def buildTestData(numRows: Int): DataFrame = {
    import sqlContext.implicits._
    sqlContext.sparkContext.parallelize(1 to numRows)
      .map(x => (x.toDouble, (x + 10).toDouble, x))
      .toDF("c1", "c2", "c3")
  }

  def dropTable(): Unit = {
    sql("DROP TABLE IF EXISTS carbonrtree")
  }

  override def beforeAll {
    dropTable()
  }

  test("Test Min Max DataMap") {
    DataMapStoreManager.getInstance().createAndRegisterDataMap(
      AbsoluteTableIdentifier.from(storeLocation, "default", "carbonminmax"),
      classOf[RTreeDataMapFactory].getName,
      RTreeDataMap.NAME)


    // register datamap writer
    val df = buildTestData(33000)

    // XX: Cannot use Overwrite since it will eliminate the DataMap Meta in DataMapStoreManager.
    // Need to delete the table manually every time.
    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("dbName", "default")
      .option("tableName", "carbonminmax")
      .save()

    // Query the table.
    sql("select c1, c2 from carbonminmax").show(20, false)
    sql("select c1, c2 from carbonminmax where c1 = 20.0 and c2 = 10.0").show(20, false)

  }

  override def afterAll {
    dropTable()
  }
}
