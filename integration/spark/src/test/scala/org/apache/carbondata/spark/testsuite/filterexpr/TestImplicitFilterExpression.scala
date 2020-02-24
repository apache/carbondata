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
package org.apache.carbondata.spark.testsuite.filterexpr

import java.util

import org.apache.spark.sql.{CarbonEnv, DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.datamap.DataMapFilter
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.scan.expression.logical.{AndExpression, TrueExpression}
import org.apache.carbondata.core.scan.filter.FilterUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.rdd.CarbonScanRDD

/**
 * test class to verify the functionality of Implicit filter expression
 */
class TestImplicitFilterExpression extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    sql("drop table if exists implicit_test")
    sql(
      "create table implicit_test(firstname string, lastname string, age int) " +
      "STORED AS carbondata")
    sql("insert into implicit_test select 'bob','marshall',35")
  }

  test("test implicit filter expression for data pruning with valid implicit filter value") {
    val query: DataFrame = sql("select count(*) from implicit_test where lastname='marshall'")
    // 1 row should be returned for blockletId 0
    verifyResultWithImplicitFilter(query, 1, 0)
  }

  test("test implicit filter expression for data pruning with invalid implicit filter value") {
    val query: DataFrame = sql("select count(*) from implicit_test where lastname='marshall'")
    // No row should be returned for blockletId 1
    verifyResultWithImplicitFilter(query, 0, 1)
  }

  private def verifyResultWithImplicitFilter(query: DataFrame,
      expectedResultCount: Int,
      blockletId: Int): Unit = {
    // from the plan extract the CarbonScanRDD
    val scanRDD = query.queryExecution.sparkPlan.collect {
      case scan: CarbonDataSourceScan if (scan.rdd.isInstanceOf[CarbonScanRDD[InternalRow]]) =>
        scan.rdd.asInstanceOf[CarbonScanRDD[InternalRow]]
    }.head
    // get carbon relation
    val relation: CarbonRelation = CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore
      .lookupRelation(Some("default"), "implicit_test")(sqlContext.sparkSession)
      .asInstanceOf[CarbonRelation]
    // get carbon table from carbon relation
    val carbonTable = relation.carbonTable
    // get the segment path
    val segmentPath = CarbonTablePath.getSegmentPath(carbonTable.getTablePath, "0")
    // list carbondata files from the segment path
    val files = FileFactory.getCarbonFile(segmentPath).listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = {
        if (file.getName.endsWith(CarbonTablePath.getCarbonDataExtension)) {
          true
        } else {
          false
        }
      }
    })
    // assert that only 1 carbondata file exists
    assert(files.length == 1)
    // get the carbondata file complete path and name
    val carbondataFileName = FileFactory.getUpdatedFilePath(files.head.getPath)
    // get the shourt unique name for the carbondata file
    // Example: complete file name will be as below
    // /opt/db/implicit_test/Fact/Part0/Segment_0/part-0-0_batchno0-0-0-1545986389020.carbondata
    // short file name: 0/0/0-0_batchno0-0-0-1545986389020
    val carbondataFileShortName = CarbonTablePath
      .getShortBlockId(carbondataFileName.substring(carbondataFileName.lastIndexOf("/Part") + 1))
    // create block to blocklet mapping indicating which all blocklets for a given block
    // contain the data
    val blockToBlockletMap = new util.HashMap[String, util.Set[Integer]]()
    val blockletList = new util.HashSet[Integer]()
    // add blocklet Id 0 to the list
    blockletList.add(blockletId)
    blockToBlockletMap.put(carbondataFileShortName, blockletList)
    // create a new AND expression with True expression as right child
    val filterExpression = new AndExpression(scanRDD.dataMapFilter.getExpression, new TrueExpression(null))
    // create implicit expression which will replace the right child (True expression)
    FilterUtil.createImplicitExpressionAndSetAsRightChild(filterExpression, blockToBlockletMap)
    // update the filter expression
    scanRDD.dataMapFilter = new DataMapFilter(carbonTable, filterExpression)
    // execute the query and get the result count
    checkAnswer(query.toDF(), Seq(Row(expectedResultCount)))
  }

  override def afterAll(): Unit = {
    sql("drop table if exists implicit_test")
  }

}
