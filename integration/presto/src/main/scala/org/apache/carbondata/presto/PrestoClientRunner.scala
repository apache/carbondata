/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.presto

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
    val queryRunner: DistributedQueryRunner = PrestoServerRunner.createQueryRunner(
      ImmutableMap.of("http-server.http.port", "8086"))
    Thread.sleep(10)
    logger.info("========STARTED SERVER ========")
    logger.info("\n====\n%s\n====", queryRunner.getCoordinator.getBaseUrl)

    /**
     * The Format for Presto JDBC Driver is :
     * jdbc:presto://<host>:<port>/<catalog>/<schema>
     */

    // Step 1: Create Connection Strings

    val JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver"
    val DB_URL = "jdbc:presto://localhost:8086/carbondata/default"
    /**
     * The database Credentials
     */
    val USER = "username"
    val PASS = "password"
    var conn: Option[Connection] = None
    var stmt: Option[Statement] = None
    try {
      logger.info("=============Connecting to default.carbon_table ===============")
      // STEP 2: Register JDBC driver
      Class.forName(JDBC_DRIVER)
      // STEP 3: Open a connection
      conn = Some(DriverManager.getConnection(DB_URL, USER, PASS))
      conn match {
        case Some(connection) =>

          // STEP 4: Execute a query

          stmt = Some(connection.createStatement)
          val sql = "SELECT * FROM carbon_table"
          stmt match {
            case Some(statement) =>
              val res = statement.executeQuery(sql)

              // STEP 5: Extract data from result set

              while (res.next()) {

                // scalastyle:off
                // Retrieve by column name and Display

                println("|" + res.getInt("intField") + "\t | "
                        + res.getString("stringField") + "\t |")

                // scalastyle:on

              }
              res.close()
              logger.info(s"Query ${ sql } executed successfully !!   ")

              // STEP 7: Close the Connection

              statement.close()
              connection.close()
            case None =>
              connection.close()
              logger.error("Unable to execute the query")
          }
        case None => logger.error("Unable to establish the the connection.")
      }
    } catch {
      case se: SQLException =>

        // Handle errors for JDBC

        logger.error(se.getMessage)
      case e: Exception =>

        // Handle errors for Class.forName

        logger.error(e.getMessage)
    } finally {
      queryRunner.close()
    }
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    prestoJdbcClient()
    System.exit(0)
  }
}

