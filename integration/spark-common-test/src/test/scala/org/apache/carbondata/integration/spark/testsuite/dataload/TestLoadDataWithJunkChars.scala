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

import java.io.{BufferedWriter, File, FileWriter}
import java.util.Random

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestLoadDataWithJunkChars extends QueryTest with BeforeAndAfterAll {
  var filePath = ""
  val junkchars = "ǍǎǏǐǑǒǓǔǕǖǗǘǙǚǛǜǝǞǟǠǡǢǣǤǥǦǧǨǩǪǫǬǭǮǯǰ"

  def buildTestData() = {
    filePath = s"$integrationPath/spark-common-test/target/junkcharsdata.csv"
    val file = new File(filePath)
    val writer = new BufferedWriter(new FileWriter(file))
    writer.write("c1,c2\n")
    val random = new Random
    for (i <- 1 until 1000) {
      writer.write("a" + i + "," + junkchars + "\n")
      if ( i % 100 == 0) {
        writer.flush()
      }
    }
    writer.write("a1000000," + junkchars)
    writer.close
  }

  test("[bug]fix bug of duplicate rows in UnivocityCsvParser #877") {
    buildTestData()
    sql("drop table if exists junkcharsdata")
    sql("""create table if not exists junkcharsdata
             (c1 string, c2 string)
             STORED AS carbondata""")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' into table junkcharsdata")
    checkAnswer(sql("select count(*) from junkcharsdata"), Seq(Row(1000)))
    sql("drop table if exists junkcharsdata")
    new File(filePath).delete()
  }
}
