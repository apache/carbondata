package org.apache.carbondata.events

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.metadata.schema.table.CarbonTable

/**
 *
 */
case class CleanFilesPostEvent(carbonTable: CarbonTable, sparkSession: SparkSession)
  extends CleanFilesEvent {

  /**
   * Method for getting the event type. Used for invoking all listeners registered for an event
   *
   * @return
   */
  override def getEventType: String = {
    CleanFilesPostEvent.eventType
  }
}

object CleanFilesPostEvent {
  val eventType = CleanFilesPostEvent.getClass.getName
}