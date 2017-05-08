import java.sql.{Connection, DriverManager, SQLException, Statement}

import PrestoHiveServerRunner.createQueryRunner
import com.facebook.presto.tests.DistributedQueryRunner
import com.google.common.collect.ImmutableMap
import org.slf4j.LoggerFactory

object PrestoHiveClientRunner {

  @throws[Exception]
  def prestoJdbcClient(): Option[List[Double]] = {
    val logger = LoggerFactory.getLogger("Hive Server")
    logger.info("======== STARTING PRESTO SERVER ========")
    val queryRunner: DistributedQueryRunner = createQueryRunner(ImmutableMap.of("http-server.http.port", "8080"))
    Thread.sleep(10)
    logger.info("======== SERVER STARTED :" + queryRunner.getCoordinator.getBaseUrl + "==========")

    val JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver"
    val DB_URL = "jdbc:presto://localhost:8080/hive/benchmarking"
    val USER = "username"
    val PASS = "password"

    try {
      logger.info("=============Connecting to database/table===============")
      //STEP 2: Register JDBC driver
      Class.forName(JDBC_DRIVER)
      //STEP 3: Open a connection
      val conn: Connection = DriverManager.getConnection(DB_URL, USER, PASS)
      val stmt: Statement = conn.createStatement
      val executionTime: List[Double] = CompareTest.queries.map { query =>
        //STEP 4: Execute a query
        CompareTest.time(stmt.executeQuery(query))
      }
      //STEP 5: Close the statement
      conn.close()
      Some(executionTime)
    } catch {
      case se: SQLException =>
        //Handle errors for JDBC
        logger.error(se.getMessage)
        None
      case e: Exception =>
        //Handle errors for Class.forName
        logger.error(e.getMessage)
        None
    }
  }
}
