package org.apache.spark.util

object FunctionRegistory {

  def replace = (value:String,orgString:String,replaceString:String) =>
    value.replaceAll(orgString,replaceString)

}
