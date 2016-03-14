package com.huawei.datasight.spark.rdd

import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.BinaryArithmetic
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.UnaryExpression

object SqlUdfLib {

  def sample(row: Row): Any = {
    row.getDouble(0) * 100
  }

  def range(row: Row): Any = {
    var value = row.getDouble(0);
    if (value >= 0 && value < 10) "0-10"
    else if (value >= 10 && value < 20) "10-20"
    else if (value >= 20 && value < 30) "20-30"
    else if (value >= 30 && value < 40) "30-40"
    else if (value >= 40 && value < 50) "40-50"
    else if (value >= 50 && value < 60) "50-60"
    else if (value >= 60 && value < 70) "60-70"
    else if (value >= 70 && value < 80) "70-80"
    else if (value >= 80 && value < 90) "80-90"
    else if (value >= 90 && value < 100) "90-100"
    else ">=100"
  }


}
