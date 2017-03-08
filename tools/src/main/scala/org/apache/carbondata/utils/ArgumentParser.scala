package org.apache.carbondata.utils

import org.apache.carbondata.exception.InvalidParameterException

import scala.util.parsing.combinator.RegexParsers

trait ArgumentParser extends RegexParsers {

  private lazy val loadProperties: Parser[(String, String)] =
    (word <~ "=") ~ word ^^ {
      case option ~ value => (option.trim.toLowerCase(), value)
      case _ => ("", "")
    }

  private lazy val loadsValue: Parser[Map[String, String]] = repsep(loadProperties, ",") ^^ (_.toMap)

  def getProperties(arguments: String): LoadProperties = {
    parse(loadsValue, arguments) match {
      case Success(properties, _) => convertProperties(properties)
      case Failure(msg, _) => throw InvalidParameterException(msg)
      case Error(msg, _) => throw InvalidParameterException(msg)
    }
  }

  private def convertProperties(loadProperties: Map[String, String]): LoadProperties = {
    val inputPath: Option[String] = loadProperties.get("inputpath")
    val fileHeader: Option[List[String]] = loadProperties.get("fileheader").map { header => header.split(",").toList }
    val delimiter: String = loadProperties.get("delimiter").fold(",")(delimiter => delimiter)
    val quoteChar: String = loadProperties.get("quotecharacter").fold("\"")(quoteChar => quoteChar)
    val badRecordAction: String = loadProperties.get("badrecordaction").fold("IGNORE")(badRecordAction => badRecordAction)
    val storeLocation: String = loadProperties.get("storelocation").fold("../tools/target")(storeLocation => storeLocation)
    if (inputPath.isDefined) {
      LoadProperties(inputPath.get, fileHeader, delimiter, quoteChar, badRecordAction, storeLocation)
    } else {
      throw InvalidParameterException("Input file path missing")
    }
  }

  private def word: Parser[String] =
    """\w+""".r ^^ {
      _.toString
    }

}

object ArgumentParser extends ArgumentParser
