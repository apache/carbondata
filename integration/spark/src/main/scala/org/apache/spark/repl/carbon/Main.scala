package org.apache.spark.repl.carbon

import org.apache.spark.repl.{CarbonSparkILoop, SparkILoop}

object Main {
  private var _interp: SparkILoop = _

  def interp = _interp

  def interp_=(i: SparkILoop) { _interp = i }

  def main(args: Array[String]) {
    try {
      _interp = new CarbonSparkILoop
      _interp.process(args)
    } catch {
      case o => o.printStackTrace()
    }
  }
}
