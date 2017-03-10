package org.apache.carbondata

object Launch {

  /**
    * This is the starting point of this tool which starts dictionary creation
    * @param args
    */
  def main(args: Array[String]) {
    DictionaryFileGeneration.startGeneration(args)
  }

}
