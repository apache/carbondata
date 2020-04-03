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

package org.apache.carbondata.index.bloom

import java.io.{File, PrintWriter}
import java.util.UUID

import scala.util.Random

import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.DataFrame

object BloomCoarseGrainIndexTestUtil extends QueryTest {

  def createFile(fileName: String, line: Int = 10000, start: Int = 0): Unit = {
    if (!new File(fileName).exists()) {
      val write = new PrintWriter(new File(fileName))
      for (i <- start until (start + line)) {
        write.println(
          s"$i,n$i,city_$i,${ Random.nextInt(80) }," +
          s"${ UUID.randomUUID().toString },${ UUID.randomUUID().toString }," +
          s"${ UUID.randomUUID().toString },${ UUID.randomUUID().toString }," +
          s"${ UUID.randomUUID().toString },${ UUID.randomUUID().toString }," +
          s"${ UUID.randomUUID().toString },${ UUID.randomUUID().toString }")
      }
      write.close()
    }
  }

  def deleteFile(fileName: String): Unit = {
    val file = new File(fileName)
    if (file.exists()) {
      file.delete()
    }
  }

  private def checkSqlHitIndex(sqlText: String, dataMapName: String, shouldHit: Boolean): DataFrame = {
    // we will not check whether the query will hit the index because index may be skipped
    // if the former index pruned all the blocklets
    sql(sqlText)
  }

  def checkBasicQuery(dataMapName: String, bloomDMSampleTable: String, normalTable: String, shouldHit: Boolean = true): Unit = {
    checkAnswer(
      checkSqlHitIndex(s"select * from $bloomDMSampleTable where id = 1", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id = 1"))
    checkAnswer(
      checkSqlHitIndex(s"select * from $bloomDMSampleTable where id = 999", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id = 999"))
    checkAnswer(
      checkSqlHitIndex(s"select * from $bloomDMSampleTable where city = 'city_1'", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city = 'city_1'"))
    checkAnswer(
      checkSqlHitIndex(s"select * from $bloomDMSampleTable where city = 'city_999'", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city = 'city_999'"))
    checkAnswer(
      sql(s"select min(id), max(id), min(name), max(name), min(city), max(city)" +
          s" from $bloomDMSampleTable"),
      sql(s"select min(id), max(id), min(name), max(name), min(city), max(city)" +
          s" from $normalTable"))
  }
}
