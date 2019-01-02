package org.apache.carbondata.datamap.bloom

import java.io.{File, PrintWriter}
import java.util.UUID

import scala.util.Random

import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.DataFrame

object BloomCoarseGrainDataMapTestUtil extends QueryTest {

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

  private def checkSqlHitDataMap(sqlText: String, dataMapName: String, shouldHit: Boolean): DataFrame = {
    // we will not check whether the query will hit the datamap because datamap may be skipped
    // if the former datamap pruned all the blocklets
    sql(sqlText)
  }

  def checkBasicQuery(dataMapName: String, bloomDMSampleTable: String, normalTable: String, shouldHit: Boolean = true): Unit = {
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where id = 1", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id = 1"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where id = 999", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id = 999"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where city = 'city_1'", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city = 'city_1'"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where city = 'city_999'", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city = 'city_999'"))
     checkAnswer(
      sql(s"select min(id), max(id), min(name), max(name), min(city), max(city)" +
          s" from $bloomDMSampleTable"),
      sql(s"select min(id), max(id), min(name), max(name), min(city), max(city)" +
          s" from $normalTable"))
  }
}
