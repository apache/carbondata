package org.apache.spark.sql.hive


import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Strategy

/**
  * @author V71149
  */
private[sql] object CarbonStrategy {
  def getStrategy(context: SQLContext): Strategy = {
    new OlapStrategies(context).OlapCubeScans
  }
}