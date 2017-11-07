package org.apache.carbondata.events

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.metadata.schema.table.CarbonTable

/**
 *
 */

case class LookupRelationPostEvent(
    carbonTable: CarbonTable,
    sparkSession: SparkSession) extends LookupRelationEvent {

  /**
   * Method for getting the event type. Used for invoking all listeners registered for an event
   *
   * @return
   */
  override def getEventType: String = {
    LookupRelationPostEvent.eventType
  }
}

object LookupRelationPostEvent {
  val eventType = LookupRelationPostEvent.getClass.getName
}
