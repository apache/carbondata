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

package org.apache.indexserver

import java.util.concurrent.{ConcurrentHashMap, Executors}

import scala.collection.JavaConverters._

import mockit.{Mock, MockUp}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.{IndexInputFormat, Segment}
import org.apache.carbondata.core.index.dev.expr.IndexInputSplitWrapper
import org.apache.carbondata.core.indexstore.blockletindex.BlockletIndexInputSplit
import org.apache.carbondata.indexserver.{DistributedIndexJob, DistributedRDDUtils, IndexRDDPartition}

class DistributedRDDUtilsTest extends FunSuite with BeforeAndAfterEach {

  val executorCache: ConcurrentHashMap[String, ConcurrentHashMap[String, Long]] =
    DistributedRDDUtils.executorToCacheSizeMapping

  val tableCache: ConcurrentHashMap[String, ConcurrentHashMap[String, String]] =
    DistributedRDDUtils.tableToExecutorMapping

  val indexServerTempFolder = "file:////tmp/indexservertmp/"

  override protected def beforeEach(): Unit = {
    executorCache.clear()
    tableCache.clear()
    FileFactory.deleteFile(indexServerTempFolder)
    buildTestData
  }

  def buildTestData {
    val tableMap = new ConcurrentHashMap[String, String]
    tableMap.put("0", "IP1_EID1")
    tableMap.put("1", "IP1_EID2")
    tableCache.put("Table1", tableMap)
    val executorMap1 = new ConcurrentHashMap[String, Long]
    executorMap1.put("EID1", 1L)
    executorMap1.put("EID2", 1L)
    val executorMap2 = new ConcurrentHashMap[String, Long]
    executorMap2.put("EID1", 1L)
    executorMap2.put("EID2", 1L)
    executorCache.put("IP1", executorMap1)
    executorCache.put("IP2", executorMap2)
  }

  test("test server mappings when 1 host is dead") {
    DistributedRDDUtils.invalidateHosts(Seq("IP1"))
    assert(DistributedRDDUtils.executorToCacheSizeMapping.size() == 1)
    assert(!DistributedRDDUtils.executorToCacheSizeMapping.containsKey("IP1"))
    assert(DistributedRDDUtils.tableToExecutorMapping.get("Table1").size() == 2)
    assert(!DistributedRDDUtils.tableToExecutorMapping.get("Table1").values().contains("IP1"))
  }

  test("test server mappings when all executor hosts are dead") {
    DistributedRDDUtils.invalidateHosts(Seq("IP1", "IP2"))
    assert(DistributedRDDUtils.executorToCacheSizeMapping.size() == 0)
    assert(!DistributedRDDUtils.executorToCacheSizeMapping.containsKey("IP1"))
    assert(!DistributedRDDUtils.executorToCacheSizeMapping.containsKey("IP2"))
    // table cache may be present because even if the executor comes up it can handle further
    // requests. If another executor is up then reassignment will happen.
  }

  test("test server mappings when 1 executor is dead") {
    DistributedRDDUtils.invalidateExecutors(Seq("IP1_EID1"))
    assert(DistributedRDDUtils.executorToCacheSizeMapping.size() == 2)
    assert(DistributedRDDUtils.executorToCacheSizeMapping.containsKey("IP1"))
    assert(!DistributedRDDUtils.executorToCacheSizeMapping.get("IP1").contains("EID1"))
    assert(DistributedRDDUtils.tableToExecutorMapping.get("Table1").size() == 2)
    assert(!DistributedRDDUtils.tableToExecutorMapping
      .get("Table1")
      .get("0")
      .equalsIgnoreCase("IP1_EID1"))
  }

  test("Test distribution for legacy segments") {
    val executorList = (0 until 10).map {
      host =>
        val executorIds = (0 until 2).map {
          executor => executor.toString
        }
        (host.toString, executorIds)
    }.toMap
    val indexDistributableWrapper = (0 to 5010).map {
      i =>
        val segment = new Segment(i.toString)
        segment.setIndexSize(1)
        val blockletIndexDistributable = new BlockletIndexInputSplit(i.toString)
        blockletIndexDistributable.setSegment(segment)
        new IndexInputSplitWrapper("", blockletIndexDistributable)
    }

    DistributedRDDUtils
      .getExecutors(indexDistributableWrapper.toArray, executorList, "default_table1", 1)
    DistributedRDDUtils.executorToCacheSizeMapping.asScala.foreach {
      a => a._2.values().asScala.foreach(size => assert(size == 250 || size == 251))
    }
  }

  test("Test distribution for legacy segments and non-legacy segments") {
    val executorList = (0 until 10).map {
      host =>
        val executorIds = (0 until 2).map {
          executor => executor.toString
        }
        (host.toString, executorIds)
    }.toMap
    val indexDistributableWrapper = (0 to 10).map {
      i =>
        val segment = new Segment(i.toString)
        if (i < 5) { segment.setIndexSize(0) } else {
          segment.setIndexSize(1)
        }
        val blockletIndexDistributable = new BlockletIndexInputSplit(i.toString)
        blockletIndexDistributable.setSegment(segment)
        new IndexInputSplitWrapper("", blockletIndexDistributable)
    }

    val partitions = DistributedRDDUtils
      .getExecutors(indexDistributableWrapper.toArray, executorList, "default_table1", 1)
    var size = 0
    partitions.zipWithIndex.foreach {
      case (partition: IndexRDDPartition, _) =>
        size += partition.inputSplit.size
    }
    assert(size == 11)
    DistributedRDDUtils.executorToCacheSizeMapping.asScala.foreach {
      a => a._2.values().asScala.foreach(size => assert(size == 1 || size == 1))
    }
  }

  test("Test distribution for non legacy segments") {
    val executorList = (0 until 10).map {
      host =>
        val executorIds = (0 until 2).map {
          executor => executor.toString
        }
        (host.toString, executorIds)
    }.toMap
    val indexDistributableWrapper = (0 to 5010).map {
      i =>
        val segment = new Segment(i.toString)
        segment.setIndexSize(111)
        val blockletIndexDistributable = new BlockletIndexInputSplit(i.toString)
        blockletIndexDistributable.setSegment(segment)
        new IndexInputSplitWrapper("", blockletIndexDistributable)
    }

    DistributedRDDUtils
      .getExecutors(indexDistributableWrapper.toArray, executorList, "default_table1", 1)
    DistributedRDDUtils.executorToCacheSizeMapping.asScala.foreach {
      a => a._2.values().asScala.foreach(size => assert(size > 27500 && size < 28000))
    }
  }

  test("Test file create and delete when query") {
    val distributedRDDUtilsTest = new DistributedIndexJob()

    val mockDataMapFormat = new MockUp[IndexInputFormat]() {
      @Mock
      def getQueryId: String = {
        "a885a111-439f-4b91-ad81-f0bd48164b84"
      }
    }
    try {
      distributedRDDUtilsTest.execute(mockDataMapFormat.getMockInstance, new Configuration())
    } catch {
      case ex: Exception =>
    }
    val tmpPath = "file:////tmp/indexservertmp/a885a111-439f-4b91-ad81-f0bd48164b84"
    assert(!FileFactory.isFileExist(tmpPath))
    assert(FileFactory.isFileExist(indexServerTempFolder))
  }

  test("Test file create and delete when query the getQueryId path is exists") {
    val distributedRDDUtilsTest = new DistributedIndexJob()
    val tmpPath = "file:////tmp/indexservertmp/a885a111-439f-4b91-ad81-f0bd48164b84"
    val newPath = "file:////tmp/indexservertmp/a885a111-439f-4b91-ad81-f0bd48164b84/ip1"
    val newFile = "file:////tmp/indexservertmp/a885a111-439f-4b91-ad81-f0bd48164b84/ip1/as1"
    val tmpPathAnother = "file:////tmp/indexservertmp/a885a111-439f-4b91-ad81-f0bd48164b8412"
    FileFactory.createDirectoryAndSetPermission(tmpPath,
      new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    FileFactory.createDirectoryAndSetPermission(newPath,
      new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    FileFactory.createNewFile(newFile, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    FileFactory.createDirectoryAndSetPermission(tmpPathAnother,
      new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))

    assert(FileFactory.isFileExist(newFile))
    assert(FileFactory.isFileExist(tmpPath))
    assert(FileFactory.isFileExist(newPath))
    assert(FileFactory.isFileExist(tmpPathAnother))

    val mockDataMapFormat = new MockUp[IndexInputFormat]() {
      @Mock
      def getQueryId: String = {
        "a885a111-439f-4b91-ad81-f0bd48164b84"
      }
    }
    try {
      distributedRDDUtilsTest.execute(mockDataMapFormat.getMockInstance, new Configuration())
    } catch {
      case ex: Exception =>
    }
    assert(!FileFactory.isFileExist(tmpPath))
    assert(FileFactory.isFileExist(indexServerTempFolder))
    assert(FileFactory.isFileExist(tmpPathAnother))
  }

  test("test concurrent assigning of executors") {
    executorCache.clear()
    tableCache.clear()
    // val executorsList: Map[String, Seq[String]]
    val executorList = Map("EX1" -> Seq("1"), "EX2" -> Seq("2"))
    val seg = new Segment("5")
    seg.setIndexSize(10)
    val executorService = Executors.newFixedThreadPool(8)
    for (num <- 1 to 8) {
      executorService.submit(
        new Runnable {
          override def run(): Unit = {
            DistributedRDDUtils.assignExecutor("tablename", seg, executorList)
          }
        }).get()
    }
    executorService.shutdownNow()
    assert(executorCache.size() == 1)
    assert(executorCache.entrySet().iterator().next().getValue
      .entrySet().iterator().next().getValue == 10)
    executorCache.clear()
    tableCache.clear()
  }
}
