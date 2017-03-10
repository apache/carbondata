package org.apache.carbondata.utils

case class LoadProperties(inputPath: String,
    fileHeaders: Option[List[String]] = None,
    delimiter: String = ",",
    quoteCharacter: String = "\"",
    badRecordAction: String = "IGNORE",
    storeLocation: String = "../tools/target")