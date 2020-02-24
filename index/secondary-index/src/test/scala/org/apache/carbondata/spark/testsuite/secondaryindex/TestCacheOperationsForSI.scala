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

package org.apache.carbondata.spark.testsuite.secondaryindex

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.secondaryindex.util.CarbonInternalScalaUtil
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.core.cache.CacheProvider
import org.scalatest.BeforeAndAfterAll

class TestCacheOperationsForSI extends QueryTest with BeforeAndAfterAll {

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

  test("Test SI for Drop Cache") {
    val tableName = "t1"
    val indexName = "index1"

    sql(s"DROP TABLE IF EXISTS $tableName")
    sql(s"CREATE TABLE $tableName(empno int, empname String, designation String, " +
      s"doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, " +
      s"deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp," +
      s"attendance int,utilization int, salary int) STORED AS carbondata")
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE $tableName")
    sql(s"CREATE INDEX $indexName ON TABLE $tableName (workgroupcategoryname, empname) " +
      s"AS 'carbondata'")
    sql(s"SELECT * FROM $tableName WHERE empname='arvind'").collect()

    val droppedCacheKeys = clone(CacheProvider.getInstance().getCarbonCache.getCacheMap.keySet())

    sql(s"DROP METACACHE ON TABLE $tableName")

    val cacheAfterDrop = clone(CacheProvider.getInstance().getCarbonCache.getCacheMap.keySet())
    droppedCacheKeys.removeAll(cacheAfterDrop)

    val tableIdentifier = new TableIdentifier(tableName, Some(dbName))
    val carbonTable = CarbonEnv.getCarbonTable(tableIdentifier)(sqlContext.sparkSession)
    val allIndexTables = CarbonInternalScalaUtil.getIndexesTables(carbonTable).asScala
    val indexTablePaths = allIndexTables.map {
      tableName =>
        CarbonEnv.getCarbonTable(Some(dbName), tableName)(sqlContext.sparkSession).getTablePath
    }

    // check if table SI entries are dropped
    for (indexPath <- indexTablePaths) {
      assert(droppedCacheKeys.asScala.exists(key => key.contains(indexPath)))
    }

    // check if cache does not have any more table SI entries
    for (indexPath <- indexTablePaths) {
      assert(!cacheAfterDrop.asScala.exists(key => key.contains(indexPath)))
    }

    sql(s"DROP TABLE $tableName")
  }

  test("Test SI for Show Cache") {
    val tableName = "t2"
    val indexName = "index1"

    sql(s"DROP TABLE IF EXISTS $tableName")
    sql(s"CREATE TABLE $tableName(empno int, empname String, designation String, " +
        s"doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, " +
        s"deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp," +
        s"attendance int,utilization int, salary int) STORED AS carbondata")
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE $tableName")
    sql(s"CREATE INDEX $indexName ON TABLE $tableName (workgroupcategoryname, empname) " +
        s"AS 'carbondata'")
    sql(s"SELECT * FROM $tableName WHERE empname='arvind'").collect()

    val result = sql(s"SHOW METACACHE ON TABLE $tableName").collectAsList()

    assert(result.get(1).getString(2).equalsIgnoreCase("Secondary Index"))
    assert(!result.get(1).getString(1).equalsIgnoreCase("0 B"))

    val result2 = sql(s"SHOW METACACHE").collectAsList()

    // Ensure there is not separate entry for SI table
    assert(result2.size() == 2)
    // Ensure table has summed up index size for SI
    assert(result2.get(1).getString(2).equalsIgnoreCase(result2.get(1).getString(2)))

    sql(s"DROP TABLE $tableName")
  }

  def clone(oldSet: util.Set[String]): util.HashSet[String] = {
    val newSet = new util.HashSet[String]
    newSet.addAll(oldSet)
    newSet
  }

}
