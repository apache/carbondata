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

package org.apache.spark.rpc

import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class SchedulerSuite extends FunSuite with BeforeAndAfterEach {

  var scheduler: Scheduler = _
  var w1: Schedulable = _
  var w2: Schedulable = _
  var w3: Schedulable = _

  override def beforeEach(): Unit = {
    scheduler = new Scheduler()
    w1 = new Schedulable("id1", "1.1.1.1", 1000, 4, new DummyRef())
    w2 = new Schedulable("id2", "1.1.1.2", 1000, 4, new DummyRef())
    w3 = new Schedulable("id3", "1.1.1.3", 1000, 4, new DummyRef())

    scheduler.addWorker("1.1.1.1", w1)
    scheduler.addWorker("1.1.1.2", w2)
    scheduler.addWorker("1.1.1.3", w3)
  }

  test("test addWorker, removeWorker, getAllWorkers") {
    assertResult(Set("1.1.1.1", "1.1.1.2", "1.1.1.3"))(scheduler.getAllWorkers.toMap.keySet)

    scheduler.removeWorker("1.1.1.2")
    assertResult(Set("1.1.1.1", "1.1.1.3"))(scheduler.getAllWorkers.toMap.keySet)

    val w4 = new Schedulable("id4", "1.1.1.4", 1000, 4, new DummyRef())
    scheduler.addWorker("1.1.1.4", w4)
    assertResult(Set("1.1.1.1", "1.1.1.3", "1.1.1.4"))(scheduler.getAllWorkers.toMap.keySet)
    assertResult("id4")(scheduler.getAllWorkers.toMap.get("1.1.1.4").get.id)
  }

  test("test normal schedule") {
    val (r1, _) = scheduler.sendRequestAsync("1.1.1.1", null)
    assertResult(w1.id)(r1.id)
    val (r2, _) = scheduler.sendRequestAsync("1.1.1.2", null)
    assertResult(w2.id)(r2.id)
    val (r3, _) = scheduler.sendRequestAsync("1.1.1.3", null)
    assertResult(w3.id)(r3.id)
    val (r4, _) = scheduler.sendRequestAsync("1.1.1.1", null)
    assertResult(w1.id)(r4.id)
    val (r5, _) = scheduler.sendRequestAsync("1.1.1.2", null)
    assertResult(w2.id)(r5.id)
    val (r6, _) = scheduler.sendRequestAsync("1.1.1.3", null)
    assertResult(w3.id)(r6.id)
  }

  test("test worker unavailable") {
    val (r1, _) = scheduler.sendRequestAsync("1.1.1.5", null)
    assert(scheduler.getAllWorkers.map(_._2.id).contains(r1.id))
  }

  test("test reschedule when target worker is overload") {
    // by default, maxWorkload is number of core * 10, so it is 40 in this test suite
    (1 to 40).foreach { i =>
      val (r2, _) = scheduler.sendRequestAsync("1.1.1.2", null)
      val (r3, _) = scheduler.sendRequestAsync("1.1.1.3", null)
    }
    val (r, _) = scheduler.sendRequestAsync("1.1.1.3", null)
    // it must be worker1 since worker3 exceed max workload
    assertResult(w1.id)(r.id)
  }

  test("test all workers are overload") {
    // by default, maxWorkload is number of core * 10, so it is 40 in this test suite
    (1 to 40).foreach { i =>
      val (r1, _) = scheduler.sendRequestAsync("1.1.1.1", null)
      val (r2, _) = scheduler.sendRequestAsync("1.1.1.2", null)
      val (r3, _) = scheduler.sendRequestAsync("1.1.1.3", null)
    }

    val e = intercept[WorkerTooBusyException] {
      scheduler.sendRequestAsync("1.1.1.3", null)
    }
  }

  test("test user configured overload param") {
    val original = CarbonProperties.getInstance().getProperty(
      CarbonCommonConstants.CARBON_SEARCH_MODE_WORKER_WORKLOAD_LIMIT)

    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_SEARCH_MODE_WORKER_WORKLOAD_LIMIT, "3")

    (1 to 3).foreach { i =>
      val (r1, _) = scheduler.sendRequestAsync("1.1.1.1", null)
      val (r2, _) = scheduler.sendRequestAsync("1.1.1.2", null)
      val (r3, _) = scheduler.sendRequestAsync("1.1.1.3", null)
    }

    val e = intercept[WorkerTooBusyException] {
      scheduler.sendRequestAsync("1.1.1.3", null)
    }

    if (original != null) {
      CarbonProperties.getInstance().addProperty(
        CarbonCommonConstants.CARBON_SEARCH_MODE_WORKER_WORKLOAD_LIMIT, original)
    }
  }

  test("test invalid property") {
    intercept[IllegalArgumentException] {
      CarbonProperties.getInstance().addProperty(
        CarbonCommonConstants.CARBON_SEARCH_MODE_WORKER_WORKLOAD_LIMIT, "-3")
    }
    var value = CarbonProperties.getInstance().getProperty(
      CarbonCommonConstants.CARBON_SEARCH_MODE_WORKER_WORKLOAD_LIMIT)
    assertResult(null)(value)

    intercept[NumberFormatException] {
      CarbonProperties.getInstance().addProperty(
        CarbonCommonConstants.CARBON_SEARCH_MODE_WORKER_WORKLOAD_LIMIT, "3s")
    }
    value = CarbonProperties.getInstance().getProperty(
      CarbonCommonConstants.CARBON_SEARCH_MODE_WORKER_WORKLOAD_LIMIT)
    assertResult(null)(value)
  }
}

class DummyRef extends RpcEndpointRef(new SparkConf) {
  override def address: RpcAddress = null

  override def name: String = ""

  override def send(message: Any): Unit = { }

  override def ask[T](message: Any, timeout: RpcTimeout)
    (implicit evidence$1: ClassTag[T]): Future[T] = null
}