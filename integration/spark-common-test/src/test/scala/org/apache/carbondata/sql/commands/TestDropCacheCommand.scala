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
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.constants.CarbonCommonConstants

class TestDropCacheCommand extends QueryTest with BeforeAndAfterAll {

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

  test("Test dictionary") {
    val tableName = "t1"

    sql(s"CREATE TABLE $tableName(empno int, empname String, designation String, " +
        s"doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, " +
        s"deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp," +
        s"attendance int,utilization int, salary int) stored by 'carbondata' " +
        s"TBLPROPERTIES('DICTIONARY_INCLUDE'='designation, workgroupcategoryname')")
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE $tableName")
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE $tableName")
    sql(s"SELECT * FROM $tableName").collect()

    val droppedCacheKeys = clone(CacheProvider.getInstance().getCarbonCache.getCacheMap.keySet())

    sql(s"DROP METACACHE ON TABLE $tableName")

    val cacheAfterDrop = clone(CacheProvider.getInstance().getCarbonCache.getCacheMap.keySet())
    droppedCacheKeys.removeAll(cacheAfterDrop)

    val tableIdentifier = new TableIdentifier(tableName, Some(dbName))
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

  def clone(oldSet: util.Set[String]): util.HashSet[String] = {
    val newSet = new util.HashSet[String]
    newSet.addAll(oldSet)
    newSet
  }
}
