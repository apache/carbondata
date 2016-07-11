package org.apache.spark.repl

class CarbonSparkILoop extends SparkILoop {



  override def initializeSpark() {
    intp.beQuietDuring {
      command("""
         if(org.apache.spark.repl.carbon.Main.interp == null) {
           org.apache.spark.repl.carbon.Main.main(Array[String]())
         }
              """)
      command("val i1 = org.apache.spark.repl.carbon.Main.interp")
      command("import i1._")
      command("""
         @transient val sc = {
           val _sc = i1.createSparkContext()
           println("Spark context available as sc.")
           _sc
         }
              """)
      command("import org.apache.spark.SparkContext._")
      command("import org.apache.spark.sql.CarbonContext")
      command("""
         @transient val cc = {
           val _cc = new CarbonContext(sc)
           println("Carbon context available as cc.")
           _cc
         }
              """)
      command("import cc.implicits._")
      command("import cc.sql")
      command("import org.apache.spark.sql.functions._")
    }
  }
}
