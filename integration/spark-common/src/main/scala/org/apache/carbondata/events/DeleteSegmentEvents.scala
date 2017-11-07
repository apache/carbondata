package org.apache.carbondata.events

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.metadata.schema.table.CarbonTable

/**
 *
 */
case class DeleteSegmentByIdPostEvent(carbonTable: CarbonTable,loadIds:Seq[String],
    sparkSession: SparkSession) extends DeleteSegmentbyIdEvent {
  /**
   * Method for getting the event type. Used for invoking all listeners registered for an event
   *
   * @return
   */
  override def getEventType: String = {
    DeleteSegmentByIdPostEvent.eventType
  }
}

case class DeleteSegmentByDatePostEvent(carbonTable: CarbonTable,loadDates:String,
    sparkSession: SparkSession) extends DeleteSegmentbyDateEvent {
  /**
   * Method for getting the event type. Used for invoking all listeners registered for an event
   *
   * @return
   */
  override def getEventType: String = {
    DeleteSegmentByDatePostEvent.eventType
  }
}

object DeleteSegmentByIdPostEvent {
  val eventType = DeleteSegmentByIdPostEvent.getClass.getName
}

object DeleteSegmentByDatePostEvent {
  val eventType = DeleteSegmentByDatePostEvent.getClass.getName
}