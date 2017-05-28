/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.utils

import scala.util.parsing.combinator.RegexParsers

import org.apache.carbondata.exception.InvalidParameterException

trait ArgumentParser extends RegexParsers {

  private lazy val loadProperties: Parser[(String, String)] =
    (word <~ "=") ~ word ^^ {
      case option ~ value => (option.trim.toLowerCase(), value)
      case _ => ("", "")
    }

  private lazy val loadsValue: Parser[Map[String, String]] = repsep(loadProperties, ",") ^^
                                                             (_.toMap)

  /**
   * This method with parse command line arguments.
   *
   * @param arguments
   * @return
   */
  def getProperties(arguments: String): LoadProperties = {
    parse(loadsValue, arguments) match {
      case Success(properties, _) =>
        convertProperties(filterProperties(properties))
      case Failure(msg, _) => throw InvalidParameterException(msg)
      case Error(msg, _) => throw InvalidParameterException(msg)
    }
  }

  private def convertProperties(loadProperties: Map[String, String]): LoadProperties = {
    val inputPath: Option[String] = loadProperties.get("inputpath")
    val fileHeader: Option[List[String]] = loadProperties.get("fileheader")
      .map { header => header.split(",").toList }
    val delimiter: String = loadProperties.get("delimiter").fold(",")(delimiter => delimiter)
    val quoteChar: String = loadProperties.get("quotechar").fold("\"")(quoteChar => quoteChar)
    val badRecordAction: String = loadProperties.get("badrecordaction")
      .fold("IGNORE")(badRecordAction => badRecordAction)
    val storeLocation: String = loadProperties.get("storelocation")
      .fold("../tools/target")(storeLocation => storeLocation)
    if (inputPath.isDefined) {
      LoadProperties(inputPath.get,
        fileHeader,
        delimiter,
        quoteChar,
        badRecordAction,
        storeLocation)
    } else {
      throw InvalidParameterException("Input file path missing")
    }
  }

  private def filterProperties(properties: Map[String, String]): Map[String, String] = {
    properties.map { case (key, value) =>
      key.substring(1, key.length - 1) -> value.substring(1, value.length-1)
    }


  }

  private def word: Parser[String] = {
    """(["'])(?:(?=(\\?))\2.)*?\1""".r ^^ {
      _.toString
    }
  }

}

object ArgumentParser extends ArgumentParser
