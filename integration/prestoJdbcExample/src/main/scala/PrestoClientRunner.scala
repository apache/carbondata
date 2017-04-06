import java.sql.{Connection, DriverManager, SQLException, Statement}

import com.facebook.presto.tests.DistributedQueryRunner
import com.google.common.collect.ImmutableMap
import org.slf4j.{Logger, LoggerFactory}

object PrestoClientRunner {

  /**
    * Creates a JDBC Client to connect CarbonData to Presto
    *
    * @throws Exception
    */
  @throws[Exception]
  def prestoJdbcClient(): Unit = {
    val logger: Logger = LoggerFactory.getLogger("Presto Server on CarbonData")
    logger.info("======== STARTING PRESTO SERVER ========")
    val queryRunner: DistributedQueryRunner = PrestoServerRunner.createQueryRunner(ImmutableMap.of("http-server.http.port", "8086"))
    Thread.sleep(10)
    logger.info("========STARTED SERVER ========")
    logger.info("\n====\n%s\n====", queryRunner.getCoordinator.getBaseUrl)
    /**
      * The Format for Presto JDBC Driver is :
      * jdbc:presto://<host>:<port>/<catalog>/<schema>
      */
    //Step 1: Create Connection Strings
    val JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver"
    val DB_URL = "jdbc:presto://localhost:8086/carbondata/demo"
    /**
      * The database Credentials
      */
    val USER = "username"
    val PASS = "password"
    var conn: Option[Connection] = None
    var stmt: Option[Statement] = None
    try {
      logger.info("=============Connecting to database/table : demo/uniqdata ===============")
      //STEP 2: Register JDBC driver
      Class.forName(JDBC_DRIVER)
      //STEP 3: Open a connection
      conn = Some(DriverManager.getConnection(DB_URL, USER, PASS))
      conn match {
        case Some(connection) => {
          //STEP 4: Execute a query
          stmt = Some(connection.createStatement)
          val sql = "select * from uniqdata_data"
          stmt match {
            case Some(statement) => {
              val res = statement.executeQuery(sql)
              //STEP 5: Extract data from result set
              println("|" + "Customer Id" + "\t | " + "Customer Name" + "\t |" + "Customer Active EMUI" + "\t |" + "Date of Birth" + "|")
              while (res.next()) {
                //Retrieve by column name and Display
                println("|" + res.getInt("cust_id") + "\t | " + res.getString("cust_name") + "\t |" + res.getString("active_emui_version") + "\t |" + res.getDate("dob") + "|")
              }
              res.close()
              logger.info(s"Query ${sql} executed successfully !!")
              //STEP 7: Close the Connection
              statement.close()
              connection.close()
            }
            case None => {
              connection.close()
              logger.error("Unable to execute the query")
            }
          }
        }
        case None => logger.error("Unable to establish the the connection.")
      }
    } catch {
      case se: SQLException =>
        //Handle errors for JDBC
        logger.error(se.getMessage)
      case e: Exception =>
        //Handle errors for Class.forName
        logger.error(e.getMessage)
    }
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    prestoJdbcClient()
  }
}
