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

package org.apache.carbon.flink

import java.util.Random

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction

abstract class TestSource(val dataCount: Int)
  extends SourceFunction[Array[AnyRef]] with CheckpointedFunction {
  private var dataIndex = 0
  private var dataIndexState: ListState[Integer] = _
  private var running = false

  @throws[Exception]
  def get(index: Int): Array[AnyRef]

  @throws[Exception]
  def onFinish(): Unit = {
    // to do nothing.
  }

  @throws[Exception]
  override def run(sourceContext: SourceFunction.SourceContext[Array[AnyRef]]): Unit = {
    this.running = true
    while ( {
      this.running && this.dataIndex < this.dataCount
    }) {
      sourceContext.collectWithTimestamp(this.get(this.dataIndex), System.currentTimeMillis)
      this.dataIndex += 1
    }
    this.onFinish()
  }

  override def cancel(): Unit = {
    this.running = false
  }

  @throws[Exception]
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    this.dataIndexState.clear()
    this.dataIndexState.add(this.dataIndex)
  }

  @throws[Exception]
  override def initializeState(context: FunctionInitializationContext): Unit = {
    this.dataIndexState = context.getOperatorStateStore
      .getListState(new ListStateDescriptor[Integer]("dataIndex", classOf[Integer]))
    if (!context.isRestored) {
      return
    }
    import scala.collection.JavaConverters._
    for (dataIndex <- this.dataIndexState.get().asScala) {
      this.dataIndex = dataIndex
    }
  }
}

object TestSource {

  val randomCache = new ThreadLocal[Random] {
    override def initialValue(): Random = new Random()
  }

}
