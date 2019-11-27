package org.apache.carbon.flink

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction

abstract class TestSource(val dataCount: Int) extends SourceFunction[String] with CheckpointedFunction {
  private var dataIndex = 0
  private var dataIndexState: ListState[Integer] = _
  private var running = false

  @throws[Exception]
  def get(index: Int): String

  @throws[Exception]
  def onFinish(): Unit = {
    // to do nothing.
  }

  @throws[Exception]
  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
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
    this.dataIndexState = context.getOperatorStateStore.getListState(new ListStateDescriptor[Integer]("dataIndex", classOf[Integer]))
    if (!context.isRestored) return
    import scala.collection.JavaConversions._
    for (dataIndex <- this.dataIndexState.get) {
      this.dataIndex = dataIndex
    }
  }
}