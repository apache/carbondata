package org.apache.spark.sql.common.util

import org.apache.spark.sql.DataFrame
import java.io.File
import java.io.PrintWriter

class DataFrameUtil(var df: DataFrame) {

  /**
   * writes result to csv file
   */
  def writeDataFrameResulttoCSVFile(fileName: String) {
    val file = new File(fileName)
    if (file.exists()) {
      file.delete()
    } else {
      val path = fileName.substring(0, fileName.lastIndexOf("/"))
      val mypath = new File(path)
      mypath.mkdirs()
    }
    val writer = new PrintWriter(file)
    var buffer = new StringBuffer()
    val schema = df.schema
    val iterator = schema.iterator
    while (iterator.hasNext) {
      val next = iterator.next()
      val name = next.name
      val dataType = next.dataType
      buffer.append(name).append(" ").append(dataType).append(",")
    }
    var strsss = buffer.toString
    strsss = strsss.substring(0, strsss.length - 1)
    writer.append(strsss)
    writer.append("\n")
    buffer = new StringBuffer()
    val collect = df.collect()
    for (r <- collect) {
      val size = r.size
      for (i <- 0 until size) {
        buffer.append(r.apply(i) + "").append(",")
      }
      var str = buffer.toString
      str = str.substring(0, str.length - 1)
      writer.append(str)
      writer.append("\n")
      buffer = new StringBuffer()
    }
    writer.flush()
    writer.close()
  }

}