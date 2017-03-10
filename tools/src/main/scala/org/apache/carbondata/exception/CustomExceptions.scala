package org.apache.carbondata.exception

case class InvalidParameterException(message: String = "", casue: Option[Throwable] = None) extends Exception(message) {
  casue foreach initCause
}

case class InvalidHeaderException(message: String = "", cause: Option[Throwable] = None) extends Exception(message) {
  cause foreach initCause
}

case class EmptyFileException(message: String = "", cause: Option[Throwable] = None) extends Exception(message) {
  cause foreach initCause
}
