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
import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.constants.CarbonCommonConstants

class TestCarbonDropCacheCommand extends CarbonQueryTest with BeforeAndAfterAll {

  val dbName = "cache_db"

  override protected def beforeAll(): Unit = {
    sql(s"DROP DATABASE IF EXISTS $dbName CASCADE")
    sql(s"CREATE DATABASE $dbName")
    sql(s"USE $dbName")
  }

  override protected def afterAll(): Unit = {
    sql(s"use default")
    sql(s"DROP DATABASE $dbName CASCADE")
  }

  test("Test preaggregate datamap") {
    val tableName = "t2"

    sql(s"CREATE TABLE $tableName(empno int, empname String, designation String, " +
        s"doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, " +
        s"deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp," +
        s"attendance int, utilization int, salary int) stored by 'carbondata'")
    sql(s"CREATE DATAMAP dpagg ON TABLE $tableName USING 'preaggregate' AS " +
        s"SELECT AVG(salary), workgroupcategoryname from $tableName GROUP BY workgroupcategoryname")
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE $tableName")
    sql(s"SELECT * FROM $tableName").collect()
    sql(s"SELECT AVG(salary), workgroupcategoryname from $tableName " +
        s"GROUP BY workgroupcategoryname").collect()
    val droppedCacheKeys = clone(CacheProvider.getInstance().getCarbonCache.getCacheMap.keySet())

    sql(s"DROP METACACHE ON TABLE $tableName")

    val cacheAfterDrop = clone(CacheProvider.getInstance().getCarbonCache.getCacheMap.keySet())
    droppedCacheKeys.removeAll(cacheAfterDrop)

    val tableIdentifier = new TableIdentifier(tableName, Some(dbName))
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

    sql(s"CREATE TABLE $tableName(empno int, empname String, designation String, " +
        s"doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, " +
        s"deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp," +
        s"attendance int, utilization int, salary int) stored by 'carbondata'")
    sql(s"CREATE DATAMAP dblom ON TABLE $tableName USING 'bloomfilter' " +
        "DMPROPERTIES('INDEX_COLUMNS'='deptno')")
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE $tableName")
    sql(s"SELECT * FROM $tableName").collect()
    sql(s"SELECT * FROM $tableName WHERE deptno=10").collect()

    val droppedCacheKeys = clone(CacheProvider.getInstance().getCarbonCache.getCacheMap.keySet())

    sql(s"DROP METACACHE ON TABLE $tableName")

    val cacheAfterDrop = clone(CacheProvider.getInstance().getCarbonCache.getCacheMap.keySet())
    droppedCacheKeys.removeAll(cacheAfterDrop)

    val tableIdentifier = new TableIdentifier(tableName, Some(dbName))
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


  test("Test preaggregate datamap fail") {
    val tableName = "t4"

    sql(s"CREATE TABLE $tableName(empno int, empname String, designation String, " +
        s"doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, " +
        s"deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp," +
        s"attendance int, utilization int, salary int) stored by 'carbondata'")
    sql(s"CREATE DATAMAP dpagg ON TABLE $tableName USING 'preaggregate' AS " +
        s"SELECT AVG(salary), workgroupcategoryname from $tableName GROUP BY workgroupcategoryname")
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE $tableName")
    sql(s"SELECT * FROM $tableName").collect()
    sql(s"SELECT AVG(salary), workgroupcategoryname from $tableName " +
        s"GROUP BY workgroupcategoryname").collect()

    val fail_message = intercept[UnsupportedOperationException] {
      sql(s"DROP METACACHE ON TABLE ${tableName}_dpagg")
    }.getMessage
    assert(fail_message.contains("Operation not allowed on child table."))
  }

  def clone(oldSet: util.Set[String]): util.HashSet[String] = {
    val newSet = new util.HashSet[String]
    newSet.addAll(oldSet)
    newSet
  }
}
