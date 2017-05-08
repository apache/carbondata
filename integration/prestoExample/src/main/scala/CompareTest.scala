import java.util.Calendar

object CompareTest {

  val tableName = "uniq_data"

  val queries: List[String] = List(s"select count(*) from $tableName",
    s"select count(distinct col1) from $tableName",
    s"select sum(col31)+10 as a ,col1 from $tableName group by col1 order by col1",
    s"select col9, count(col9) a from $tableName group by col9 order by col9",
    s"select col2 from $tableName where (col31 =  1000002 ) and (col2='cust_name_21000002')",
    s"select col6,count(col6) count from $tableName group by col6 order by col6 limit 1000",
    s"select col2 from $tableName where col2 !='cust_name_21000002' order by col2",
    s"select col6 from $tableName where col2 >'cust_name_21000002' order by col6",
    s"Select count(col4),min(col35) from $tableName",
    s"select max(col36),min(col40),count(col1) from $tableName",
    s"select sum(col37)+10101 as a ,col3  from $tableName group by col3",
    s"select min(col33), max(col39+27) Total from $tableName group by col2 order by Total"
  )

  def time(code: => Unit): Double = {
    val start = System.currentTimeMillis()
    code
    // return time in seconds
    (System.currentTimeMillis() - start).toDouble / 1000
  }

  def writeResults(content:String)={
    scala.tools.nsc.io.File("integration/prestoExample/src/main/resources/BenchmarkingResults.txt").appendAll(content)
  }

  def compareResults(): Unit = {
    val executionResult: Option[(List[Double], List[Double])] = for {
      timeForOrc <- PrestoHiveClientRunner.prestoJdbcClient()
      timeForCarbon <- PrestoCarbonDataClientRunner.prestoJdbcClient()
    } yield (timeForOrc, timeForCarbon)

    executionResult.foreach { result =>
      val executionResults: List[(Double, Double)] = result._1.zip(result._2)
      val aggregatedResults: List[((Double, Double), String)] = executionResults.zip(queries)
      writeResults("\n\n-------------------------DATE/TIME:"+Calendar.getInstance().getTime()+" -----------------------------\n\n")
      aggregatedResults.foreach { result =>
        val resultContent: String ="|QUERY : " + result._2 + "\n" +
          "|\t\t\tORC EXECUTION TIME :" + result._1._1 + "\n" +
          "|\t\t\tCARBON EXECUTION TIME :" + result._1._2+"\n"
        writeResults(resultContent)
        println("-------------------------------------------------------------------------------------------------------------------------")
        println(resultContent)
      }
      println("-------------------------------------------------------------------------------------------------------------------------")
    }
  }

  def main(args: Array[String]): Unit = {

    compareResults()
  }

}
