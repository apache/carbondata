package org.apache.carbondata.events

import org.apache.carbondata.core.metadata.schema.table.CarbonTable

/**
 *
 */

case class UpdateTablePreEvent(carbonTable: CarbonTable) extends UpdateTableEvent {
  /**
   * Method for getting the event type. Used for invoking all listeners registered for an event
   *
   * @return
   */
  override def getEventType: String = {
    UpdateTablePreEvent.eventType
  }
}

case class DeleteFromTablePreEvent(carbonTable: CarbonTable) extends DeleteFromTableEvent {
  /**
   * Method for getting the event type. Used for invoking all listeners registered for an event
   *
   * @return
   */
  override def getEventType: String = {
    DeleteFromTablePreEvent.eventType
  }
}

object UpdateTablePreEvent {
  val eventType = UpdateTablePreEvent.getClass.getName
}

object DeleteFromTablePreEvent {
  val eventType = DeleteFromTablePreEvent.getClass.getName
}
