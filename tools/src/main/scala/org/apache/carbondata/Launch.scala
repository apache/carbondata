package org.apache.carbondata

object Launch {

  def main(args: Array[String]) {
    DictionaryFileGeneration.startGeneration(args)
  }

}
