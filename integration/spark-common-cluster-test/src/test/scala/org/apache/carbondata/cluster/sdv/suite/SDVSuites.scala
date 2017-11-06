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
package org.apache.carbondata.cluster.sdv.suite

import org.apache.spark.sql.test.TestQueryExecutor
import org.scalatest.{BeforeAndAfterAll, Suites}

import org.apache.carbondata.cluster.sdv.generated._

/**
 * Suite class for all tests.
 */
class SDVSuites extends Suites with BeforeAndAfterAll {

  val suites =                new AlterTableTestCase ::
                              new BadRecordTestCase ::
                              new BatchSortLoad1TestCase ::
                              new BatchSortLoad2TestCase ::
                              new BatchSortQueryTestCase ::
                              new ColumndictTestCase ::
                              new DataLoadingTestCase ::
                              new DataLoadingV3TestCase ::
                              new InvertedindexTestCase ::
                              new OffheapQuery1TestCase ::
                              new OffheapQuery2TestCase ::
                              new OffheapSort1TestCase ::
                              new OffheapSort2TestCase ::
                              new PartitionTestCase ::
                              new QueriesBasicTestCase ::
                              new QueriesBVATestCase ::
                              new QueriesCompactionTestCase ::
                              new QueriesExcludeDictionaryTestCase ::
                              new QueriesIncludeDictionaryTestCase ::
                              new QueriesNormalTestCase ::
                              new QueriesRangeFilterTestCase ::
                              new QueriesSparkBlockDistTestCase ::
                              new ShowLoadsTestCase ::
                              new SinglepassTestCase ::
                              new SortColumnTestCase ::
                              new TimestamptypesTestCase ::
                              new V3offheapvectorTestCase ::
                              new Vector1TestCase ::
                              new Vector2TestCase ::Nil

  override val nestedSuites = suites.toIndexedSeq

  override protected def afterAll() = {
    println("---------------- Stopping spark -----------------")
    TestQueryExecutor.INSTANCE.stop()
    println("---------------- Stopped spark -----------------")
  }
}

/**
 * Suite class for all tests.
 */
class SDVSuites1 extends Suites with BeforeAndAfterAll {

  val suites =     new BadRecordTestCase ::
                   new BatchSortLoad1TestCase ::
                   new BatchSortQueryTestCase ::
                   new DataLoadingTestCase ::
                   new OffheapSort2TestCase ::
                   new PartitionTestCase ::
                   new QueriesBasicTestCase ::
                   new BatchSortLoad3TestCase ::
                   new GlobalSortTestCase ::
                   new MergeIndexTestCase :: Nil

  override val nestedSuites = suites.toIndexedSeq

  override protected def afterAll() = {
    println("---------------- Stopping spark -----------------")
    TestQueryExecutor.INSTANCE.stop()
    println("---------------- Stopped spark -----------------")
  }
}

/**
 * Suite class for all tests.
 */
class SDVSuites2 extends Suites with BeforeAndAfterAll {

  val suites =      new QueriesBVATestCase ::
                    new QueriesCompactionTestCase ::
                    new QueriesExcludeDictionaryTestCase ::
                    new DataLoadingIUDTestCase :: Nil

  override val nestedSuites = suites.toIndexedSeq

  override protected def afterAll() = {
    println("---------------- Stopping spark -----------------")
    TestQueryExecutor.INSTANCE.stop()
    println("---------------- Stopped spark -----------------")
  }
}

/**
 * Suite class for all tests.
 */
class SDVSuites3 extends Suites with BeforeAndAfterAll {

  val suites =      new AlterTableTestCase ::
                    new BatchSortLoad2TestCase ::
                    new BucketingTestCase ::
                    new InvertedindexTestCase ::
                    new OffheapQuery1TestCase ::
                    new OffheapQuery2TestCase ::
                    new OffheapSort1TestCase ::
                    new ShowLoadsTestCase ::
                    new SinglepassTestCase ::
                    new SortColumnTestCase ::
                    new TimestamptypesTestCase ::
                    new V3offheapvectorTestCase ::
                    new Vector1TestCase ::
                    new Vector2TestCase ::
                    new QueriesNormalTestCase ::
                    new ColumndictTestCase ::
                    new QueriesRangeFilterTestCase ::
                    new QueriesSparkBlockDistTestCase ::
                    new DataLoadingV3TestCase ::
                    new QueriesIncludeDictionaryTestCase :: Nil

  override val nestedSuites = suites.toIndexedSeq

  override protected def afterAll() = {
    println("---------------- Stopping spark -----------------")
    TestQueryExecutor.INSTANCE.stop()
    println("---------------- Stopped spark -----------------")
  }
}

/**
 * Suite class for compatabiity tests
 */
class SDVSuites4 extends Suites with BeforeAndAfterAll {

  val suites =     new CarbonV1toV3CompatabilityTestCase  :: Nil

  override val nestedSuites = suites.toIndexedSeq

  override protected def afterAll() = {
    println("---------------- Stopping spark -----------------")
    TestQueryExecutor.INSTANCE.stop()
    println("---------------- Stopped spark -----------------")
  }
}
