package org.apache.carbondata.events

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.metadata.schema.table.CarbonTable

/**
  *
  */

case class DropTablePreEvent(carbonTable: CarbonTable, ifExistsSet: Boolean, sparkSession: SparkSession)
  extends DropTableEvent {
  /**
    * Method for getting the event type. Used for invoking all listeners registered for an event
    *
    * @return
    */
  override def getEventType: String = {
    DropTablePreEvent.eventType
  }
}

case class DropTablePostEvent(carbonTable: CarbonTable, ifExistsSet: Boolean, sparkSession: SparkSession)
  extends DropTableEvent {
  /**
    * Method for getting the event type. Used for invoking all listeners registered for an event
    *
    * @return
    */
  override def getEventType: String = {
    DropTablePostEvent.eventType
  }
}

object DropTablePreEvent {
  val eventType = DropTablePreEvent.getClass.getName
}

object DropTablePostEvent {
  val eventType = DropTablePostEvent.getClass.getName
}