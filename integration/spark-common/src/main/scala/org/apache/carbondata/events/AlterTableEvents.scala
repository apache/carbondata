package org.apache.carbondata.events

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.execution.command.{AlterTableDropColumnModel, AlterTableRenameModel}

import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.processing.loading.model.CarbonLoadModel

/**
 *
 */
case class AlterTableDropColumnPreEvent(carbonTable: CarbonTable,
    alterTableDropColumnModel: AlterTableDropColumnModel,
    sparkSession: SparkSession) extends AlterTableDropColumnEvent {
  /**
   * Method for getting the event type. Used for invoking all listeners registered for an event
   *
   * @return
   */
  override def getEventType: String = {
    AlterTableDropColumnPreEvent.eventType
  }
}

case class AlterTableRenamePreEvent(carbonTable: CarbonTable,
    alterTableRenameModel: AlterTableRenameModel, newTablePath: String,
    sparkSession: SparkSession) extends AlterTableRenameEvent {

  /**
   * Method for getting the event type. Used for invoking all listeners registered for an event
   *
   * @return
   */
  override def getEventType: String = {
    AlterTableRenamePreEvent.eventType
  }
}

case class AlterTableCompactionPreEvent(carbonTable: CarbonTable,
    carbonLoadModel: CarbonLoadModel,
    mergedLoadName: String,
    sQLContext: SQLContext) extends AlterTableCompactionEvent {
  /**
   * Method for getting the event type. Used for invoking all listeners registered for an event
   *
   * @return
   */
  override def getEventType: String = {
    AlterTableCompactionPreEvent.eventType
  }
}

object AlterTableDropColumnPreEvent {
  val eventType = AlterTableDropColumnPreEvent.getClass.getName
}

object AlterTableRenamePreEvent {
  val eventType = AlterTableRenamePreEvent.getClass.getName
}

object AlterTableCompactionPreEvent{
  val eventType = AlterTableCompactionPreEvent.getClass.getName
}