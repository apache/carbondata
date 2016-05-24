/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.spark.testsuite.dataretention

import java.io.File
import java.text.SimpleDateFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.scalatest.BeforeAndAfterAll

/**
  * This class contains data retention test cases
  * Created by Manohar on 5/9/2016.
  */
class DataRetentionTestCase extends QueryTest with BeforeAndAfterAll {

  val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
    .getCanonicalPath

  val resource = currentDirectory + "/src/test/resources/"
  var time = ""

  override def beforeAll {
    CarbonProperties.getInstance.addProperty(CarbonCommonConstants.MAX_QUERY_EXECUTION_TIME, "1")

    sql(
      "CREATE table DataRetentionTable (ID int, date String, country String, name " +
        "String," +
        "phonetype String, serialname String, salary int) stored by 'org.apache.carbondata.format'"

    )

    sql("LOAD DATA LOCAL INPATH '" + resource + "dataretention1.csv' INTO TABLE DataRetentionTable " +
      "OPTIONS('DELIMITER' =  ',')")
    sql("LOAD DATA LOCAL INPATH '" + resource + "dataretention2.csv' INTO TABLE DataRetentionTable " +
      "OPTIONS('DELIMITER' = ',')")

    Thread.sleep(1000)
    time = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .format(System.currentTimeMillis())
    Thread.sleep(1000)

    sql("LOAD DATA LOCAL INPATH '" + resource + "dataretention3.csv' INTO TABLE DataRetentionTable " +
      "OPTIONS('DELIMITER' = ',')")

  }

  override def afterAll {
    sql("drop table DataRetentionTable")
  }

  test("RetentionTest_withoutDelete") {
    checkAnswer(
      sql("SELECT country, count(salary) AS amount FROM DataRetentionTable WHERE country" +
        " IN ('china','ind','aus','eng') GROUP BY country"
      ),
      Seq(Row("aus", 9), Row("eng", 9), Row("ind", 9))
    )
  }

  test("RetentionTest_DeleteSegmentsByLoadTime") {
    // delete ind, aus segments
    sql("DELETE SEGMENTS FROM TABLE DataRetentionTable where STARTTIME before '" + time + "'")
    checkAnswer(
      sql("SELECT country, count(salary) AS amount FROM DataRetentionTable WHERE country" +
        " IN ('china','ind','aus','eng') GROUP BY country"
      ),
      Seq(Row("eng", 9))
    )
  }

  test("RetentionTest3_DeleteByLoadId") {
    // delete load 2 and load ind segment
    sql("DELETE LOAD 2 FROM TABLE DataRetentionTable")
    sql("LOAD DATA LOCAL INPATH '" + resource + "dataretention1.csv' INTO TABLE DataRetentionTable " +
      "OPTIONS('DELIMITER' = ',')")
    checkAnswer(
      sql("SELECT country, count(salary) AS amount FROM DataRetentionTable WHERE country" +
        " IN ('china','ind','aus','eng') GROUP BY country"
      ),
      Seq(Row("ind", 9))
    )
  }


}
