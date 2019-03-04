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

package org.apache.carbondata.sql.commands

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.constants.CarbonCommonConstants

class TestCarbonDropCacheCommand extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {

  val dbName = "cache_db"

  override protected def beforeAll(): Unit = {
    sql(s"DROP DATABASE IF EXISTS $dbName CASCADE")
    sql(s"CREATE DATABASE $dbName")
    sql(s"USE $dbName")
  }

  override protected def afterAll(): Unit = {
    sql(s"DROP DATABASE $dbName CASCADE")
  }

  override protected def beforeEach(): Unit = {
    sql("DROP TABLE IF EXISTS t1")
    sql("DROP TABLE IF EXISTS t2")
  }


  test("Test dictionary") {
    val tableName = "t1"

    sql(s"CREATE TABLE $tableName(age int, name string) stored by 'carbondata' " +
        s"TBLPROPERTIES('DICTIONARY_INCLUDE'='age, name')")
    sql(s"LOAD DATA INPATH '/home/root1/Desktop/CSVs/age_name.csv' INTO TABLE $tableName")
    sql(s"LOAD DATA INPATH '/home/root1/Desktop/CSVs/age_name.csv' INTO TABLE $tableName")
    sql(s"SELECT * FROM $tableName").collect()

    val droppedCacheKeys = clone(CacheProvider.getInstance().getCarbonCache.getCacheMap.keySet())

    sql(s"DROP METACACHE FOR TABLE $tableName")

    val cacheAfterDrop = clone(CacheProvider.getInstance().getCarbonCache.getCacheMap.keySet())
    droppedCacheKeys.removeAll(cacheAfterDrop)

    val tableIdentifier = new TableIdentifier(tableName, Some(dbName))
    val table = sqlContext.sparkSession.sessionState.catalog.listTables(dbName)
      .find(_.table.equalsIgnoreCase(tableName))
    if (table.isEmpty) {
      fail("Table does not exists")
    }
    val carbonTable = CarbonEnv.getCarbonTable(tableIdentifier)(sqlContext.sparkSession)
    val tablePath = carbonTable.getTablePath + CarbonCommonConstants.FILE_SEPARATOR
    val dictIds = carbonTable.getAllDimensions.asScala.filter(_.isGlobalDictionaryEncoding)
      .map(_.getColumnId).toArray

    // Check if table index entries are dropped
    assert(droppedCacheKeys.asScala.exists(key => key.startsWith(tablePath)))

    // check if cache does not have any more table index entries
    assert(!cacheAfterDrop.asScala.exists(key => key.startsWith(tablePath)))

    // check if table dictionary entries are dropped
    for (dictId <- dictIds) {
      assert(droppedCacheKeys.asScala.exists(key => key.contains(dictId)))
    }

    // check if cache does not have any more table dictionary entries
    for (dictId <- dictIds) {
      assert(!cacheAfterDrop.asScala.exists(key => key.contains(dictId)))
    }
  }


  test("Test preaggregate datamap") {
    val tableName = "t2"

    sql(s"CREATE TABLE $tableName(age int, name string) stored by 'carbondata'")
    sql(s"CREATE DATAMAP dpagg ON TABLE $tableName USING 'preaggregate' AS " +
        s"SELECT AVG(age), name from $tableName GROUP BY name")
    sql(s"LOAD DATA INPATH '/home/root1/Desktop/CSVs/age_name.csv' INTO TABLE $tableName")
    sql(s"SELECT * FROM $tableName").collect()
    sql(s"SELECT AVG(age), name from $tableName GROUP BY name").collect()

    val droppedCacheKeys = clone(CacheProvider.getInstance().getCarbonCache.getCacheMap.keySet())

    sql(s"DROP METACACHE FOR TABLE $tableName")

    val cacheAfterDrop = clone(CacheProvider.getInstance().getCarbonCache.getCacheMap.keySet())
    droppedCacheKeys.removeAll(cacheAfterDrop)

    val tableIdentifier = new TableIdentifier(tableName, Some(dbName))
    val table = sqlContext.sparkSession.sessionState.catalog.listTables(dbName)
      .find(_.table.equalsIgnoreCase(tableName))
    if (table.isEmpty) {
      fail("Table does not exists")
    }
    val carbonTable = CarbonEnv.getCarbonTable(tableIdentifier)(sqlContext.sparkSession)
    val dbPath = CarbonEnv
      .getDatabaseLocation(tableIdentifier.database.get, sqlContext.sparkSession)
    val tablePath = carbonTable.getTablePath
    val preaggPath = dbPath + CarbonCommonConstants.FILE_SEPARATOR + carbonTable.getTableName +
                     "_" + carbonTable.getTableInfo.getDataMapSchemaList.get(0).getDataMapName +
                     CarbonCommonConstants.FILE_SEPARATOR

    // Check if table index entries are dropped
    assert(droppedCacheKeys.asScala.exists(key => key.startsWith(tablePath)))

    // check if cache does not have any more table index entries
    assert(!cacheAfterDrop.asScala.exists(key => key.startsWith(tablePath)))

    // Check if preaggregate index entries are dropped
    assert(droppedCacheKeys.asScala.exists(key => key.startsWith(preaggPath)))

    // check if cache does not have any more preaggregate index entries
    assert(!cacheAfterDrop.asScala.exists(key => key.startsWith(preaggPath)))
  }


  test("Test bloom filter") {
    val tableName = "t3"

    sql(s"CREATE TABLE $tableName(age int, name string) stored by 'carbondata'")
    sql(s"CREATE DATAMAP dblom ON TABLE $tableName USING 'bloomfilter' " +
        "DMPROPERTIES('INDEX_COLUMNS'='age')")
    sql(s"LOAD DATA INPATH '/home/root1/Desktop/CSVs/age_name.csv' INTO TABLE $tableName")
    sql(s"SELECT * FROM $tableName").collect()
    sql(s"SELECT * FROM $tableName WHERE age=25").collect()

    val droppedCacheKeys = clone(CacheProvider.getInstance().getCarbonCache.getCacheMap.keySet())

    sql(s"DROP METACACHE FOR TABLE $tableName")

    val cacheAfterDrop = clone(CacheProvider.getInstance().getCarbonCache.getCacheMap.keySet())
    droppedCacheKeys.removeAll(cacheAfterDrop)

    val tableIdentifier = new TableIdentifier(tableName, Some(dbName))
    val table = sqlContext.sparkSession.sessionState.catalog.listTables(dbName)
      .find(_.table.equalsIgnoreCase(tableName))
    if (table.isEmpty) {
      fail("Table does not exists")
    }
    val carbonTable = CarbonEnv.getCarbonTable(tableIdentifier)(sqlContext.sparkSession)
    val tablePath = carbonTable.getTablePath
    val bloomPath = tablePath + CarbonCommonConstants.FILE_SEPARATOR + "dblom" +
                    CarbonCommonConstants.FILE_SEPARATOR

    // Check if table index entries are dropped
    assert(droppedCacheKeys.asScala.exists(key => key.startsWith(tablePath)))

    // check if cache does not have any more table index entries
    assert(!cacheAfterDrop.asScala.exists(key => key.startsWith(tablePath)))

    // Check if bloom entries are dropped
    assert(droppedCacheKeys.asScala.exists(key => key.contains(bloomPath)))

    // check if cache does not have any more bloom entries
    assert(!cacheAfterDrop.asScala.exists(key => key.contains(bloomPath)))
  }

  def clone(oldSet: util.Set[String]): util.HashSet[String] = {
    val newSet = new util.HashSet[String]
    newSet.addAll(oldSet)
    newSet
  }
}
