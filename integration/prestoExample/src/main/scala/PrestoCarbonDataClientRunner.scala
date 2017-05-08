import java.sql.{Connection, DriverManager, SQLException, Statement}

import com.facebook.presto.tests.DistributedQueryRunner
import com.google.common.collect.ImmutableMap
import org.slf4j.{Logger, LoggerFactory}

object PrestoCarbonDataClientRunner {
  /**
    * Creates a JDBC Client to connect CarbonData to PrestoDb
    *
    * @throws Exception
    */
  @throws[Exception]
  def prestoJdbcClient(): Option[List[Double]] = {
    val logger: Logger = LoggerFactory.getLogger("CarbonData Server")
    logger.info("======== STARTING PRESTO SERVER ========")
    val queryRunner: DistributedQueryRunner = PrestoCarbonDataServerRunner.createQueryRunner(ImmutableMap.of("http-server.http.port", "8086"))
    Thread.sleep(10)
    logger.info("========STARTED SERVER : " + queryRunner.getCoordinator.getBaseUrl + "========")

    /**
      * The Format for Presto JDBC Driver is :
      * jdbc:presto://<host>:<port>/<catalog>/<schema>
      */
    //Step 1: Create Connection Strings
    val JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver"
    val DB_URL = "jdbc:presto://localhost:8086/carbondata/benchmarking"
    /**
      * The database Credentials
      */
    val USER = "username"
    val PASS = "password"

    try {
      logger.info("=============Connecting to database/table ===============")
      //STEP 2: Register JDBC driver
      Class.forName(JDBC_DRIVER)
      //STEP 3: Open a connection
      val conn: Connection = DriverManager.getConnection(DB_URL, USER, PASS)
      val stmt: Statement = conn.createStatement
      val executionTime: List[Double] = CompareTest.queries.map { query =>
        CompareTest.time(stmt.executeQuery(query))
      }
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
