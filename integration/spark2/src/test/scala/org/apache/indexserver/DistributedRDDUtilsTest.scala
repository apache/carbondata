package org.apache.indexserver

import java.util.concurrent.ConcurrentHashMap

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datamap.dev.expr.DataMapDistributableWrapper
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapDistributable
import org.apache.carbondata.indexserver.DistributedRDDUtils

class DistributedRDDUtilsTest extends FunSuite with BeforeAndAfterEach {

  val executorCache: ConcurrentHashMap[String, ConcurrentHashMap[String, Long]] = DistributedRDDUtils
    .executorToCacheSizeMapping

  val tableCache: ConcurrentHashMap[String, ConcurrentHashMap[String, String]] = DistributedRDDUtils.tableToExecutorMapping

  override protected def beforeEach(): Unit = {
    executorCache.clear()
    tableCache.clear()
    buildTestData
  }

  def buildTestData {
    val tableMap = new ConcurrentHashMap[String, String]
    tableMap.put("0" , "IP1_EID1")
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
    assert(!DistributedRDDUtils.tableToExecutorMapping.get("Table1").get("0").equalsIgnoreCase("IP1_EID1"))
  }

  test("Test distribution for legacy segments") {
    val executorList = (0 until 10).map {
      host =>
        val executorIds = (0 until 2).map {
          executor => executor.toString
        }
        (host.toString, executorIds)
    }.toMap
    val dataMapDistributableWrapper = (0 to 5010).map {
      i =>
        val segment = new Segment(i.toString)
        segment.setIndexSize(1)
        val blockletDataMapDistributable = new BlockletDataMapDistributable(i.toString)
        blockletDataMapDistributable.setSegment(segment)
        new DataMapDistributableWrapper("", blockletDataMapDistributable)
    }

    DistributedRDDUtils
      .getExecutors(dataMapDistributableWrapper.toArray, executorList, "default_table1", 1)
    DistributedRDDUtils.executorToCacheSizeMapping.asScala.foreach {
      a => a._2.values().asScala.foreach(size => assert(size == 250 || size == 251))
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
    val dataMapDistributableWrapper = (0 to 5010).map {
      i =>
        val segment = new Segment(i.toString)
        segment.setIndexSize(111)
        val blockletDataMapDistributable = new BlockletDataMapDistributable(i.toString)
        blockletDataMapDistributable.setSegment(segment)
        new DataMapDistributableWrapper("", blockletDataMapDistributable)
    }

    DistributedRDDUtils
      .getExecutors(dataMapDistributableWrapper.toArray, executorList, "default_table1", 1)
    DistributedRDDUtils.executorToCacheSizeMapping.asScala.foreach {
      a => a._2.values().asScala.foreach(size => assert(size > 27500 && size < 28000))
    }
  }
}
