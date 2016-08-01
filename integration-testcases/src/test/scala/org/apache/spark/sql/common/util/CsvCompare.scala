package org.apache.spark.sql.common.util

import java.io.BufferedReader
import java.io.File
import java.util.HashMap
import java.io.FileReader

import scala.io.Source

/**
 * CSV compare utility
 */
class CsvCompare {

  /**
   * compares csv files and gives result
   */
  def compareCSVWithFailureReason(file1: String, file2: String): String = {

    val failureReason = new StringBuilder()
    val hive = new File(file1)
    val olap = new File(file2)
    val map = new HashMap[String, Integer]()
    val carbon = new HashMap[String, Integer]()
    val hivefile = new BufferedReader(new FileReader(hive))
    val olapfile = new BufferedReader(new FileReader(olap))
    
 
      for(line <- Source.fromFile(file1).getLines())
        {
      println(line)
      println(line.equals("null"))
      if (map.containsKey(line)) {
        map.put(line, map.get(line) + 1)
      } else {
        map.put(line, 1)
      }
    }
    
    for (line <- Source.fromFile(file2).getLines()) {
      if (carbon.containsKey(line)) {
        carbon.put(line, carbon.get(line) + 1)
      } else {
        carbon.put(line, 1)
      }
    }
    hivefile.close()
    olapfile.close()
    var isPass = true
    if (carbon.size != map.size) {
      isPass = false
      failureReason.append("Result set size is not matched.")
    } else {
      val keySet = map.keySet
      val iterator = keySet.iterator()
      while (iterator.hasNext) {
        val next = iterator.next()
        val hiveNum = map.get(next)
        if (carbon.containsKey(next)) {
          val carbonNum = carbon.get(next)
          if (hiveNum != carbonNum) {
            isPass = false
            failureReason.append(next + " record occurences are not matched.")
            //break
          }
        } else {
          isPass = false
          failureReason.append(next + " record is not matched.")
        }
      }
    }
    failureReason.toString
  }

}