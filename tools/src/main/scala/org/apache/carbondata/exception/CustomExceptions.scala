package org.apache.carbondata.exception

case class InvalidParameterException(message: String = "", casue: Option[Throwable] = None) extends Exception(message) {
  casue.foreach(initCause)
}
