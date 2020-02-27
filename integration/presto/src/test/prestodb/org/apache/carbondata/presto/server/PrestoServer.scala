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
package org.apache.carbondata.presto.server

import java.sql.{Connection, DriverManager, ResultSet}
import java.util
import java.util.{Locale, Optional, Properties}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import com.facebook.presto.Session
import com.facebook.presto.execution.QueryIdGenerator
import com.facebook.presto.jdbc.PrestoStatement
import com.facebook.presto.metadata.SessionPropertyManager
import com.facebook.presto.spi.`type`.TimeZoneKey.UTC_KEY
import com.facebook.presto.spi.security.Identity
import com.facebook.presto.tests.DistributedQueryRunner
import com.google.common.collect.ImmutableMap
import org.slf4j.{Logger, LoggerFactory}

import org.apache.carbondata.presto.CarbondataPlugin

class PrestoServer {

  val CARBONDATA_CATALOG = "carbondata"
  val CARBONDATA_CONNECTOR = "carbondata"
  val CARBONDATA_SOURCE = "carbondata"
  val LOGGER: Logger = LoggerFactory.getLogger(this.getClass)


  val prestoProperties: util.Map[String, String] = Map(("http-server.http.port", "8086")).asJava
  val carbonProperties: util.Map[String, String] = new util.HashMap[String, String]()
  createSession
  lazy val queryRunner = new DistributedQueryRunner(createSession, 4, prestoProperties)
  var dbName : String = null
  var statement : PrestoStatement = _


  /**
   * start the presto server
   *
   */
  def startServer(): Unit = {

    LOGGER.info("======== STARTING PRESTO SERVER ========")
    val queryRunner: DistributedQueryRunner = createQueryRunner(prestoProperties)

    LOGGER.info("STARTED SERVER AT :" + queryRunner.getCoordinator.getBaseUrl)
  }

  /**
   * start the presto server
   *
   * @param dbName the database name, if not a default database
   */
  def startServer(dbName: String, properties: util.Map[String, String] = new util.HashMap[String, String]()): Unit = {

    this.dbName = dbName
    carbonProperties.putAll(properties)
    LOGGER.info("======== STARTING PRESTO SERVER ========")
    val queryRunner: DistributedQueryRunner = createQueryRunner(prestoProperties)
    val conn: Connection = createJdbcConnection(dbName)
    statement = conn.createStatement().asInstanceOf[PrestoStatement]
    LOGGER.info("STARTED SERVER AT :" + queryRunner.getCoordinator.getBaseUrl)
  }

  /**
   * Instantiates the Presto Server to connect with the Apache CarbonData
   */
  private def createQueryRunner(extraProperties: util.Map[String, String]) = {
    Try {
      queryRunner.installPlugin(new CarbondataPlugin)
      val carbonProperties = ImmutableMap.builder[String, String]
        .putAll(this.carbonProperties)
        .put("carbon.unsafe.working.memory.in.mb", "512").build

      // CreateCatalog will create a catalog for CarbonData in etc/catalog.
      queryRunner.createCatalog(CARBONDATA_CATALOG, CARBONDATA_CONNECTOR, carbonProperties)
    } match {
      case Success(result) => queryRunner
      case Failure(exception) => queryRunner.close()
        throw exception
    }
  }

  /**
   * stop the presto server
   */
  def stopServer(): Unit = {
    queryRunner.close()
    statement.close()
    LOGGER.info("***** Stopping The Server *****")
  }

  /**
   * execute the query by establishing the jdbc connection
   *
   * @param query
   * @return
   */
  def executeQuery(query: String): List[Map[String, Any]] = {

    Try {
      LOGGER.info(s"***** executing the query ***** \n $query")
      val result: ResultSet = statement.executeQuery(query)
      convertResultSetToList(result)
    } match {
      case Success(result) => result
      case Failure(jdbcException) => LOGGER
        .error(s"exception occurs${ jdbcException.getMessage } \n query failed $query")
        throw jdbcException
    }
  }

  def execute(query: String) = {
    Try {
      LOGGER.info(s"***** executing the query ***** \n $query")
      statement.execute(query)
    } match {
      case Success(result) => result
      case Failure(jdbcException) => LOGGER
        .error(s"exception occurs${ jdbcException.getMessage } \n query failed $query")
        throw jdbcException
    }
  }

  /**
   * Creates a JDBC Client to connect CarbonData to Presto
   *
   * @return
   */
  private def createJdbcConnection(dbName: String) = {
    val JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver"
    var DB_URL : String = null
    if (dbName == null) {
      DB_URL = "jdbc:presto://localhost:8086/carbondata/default"
    } else {
      DB_URL = "jdbc:presto://localhost:8086/carbondata/" + dbName
    }
    val properties = new Properties
    // The database Credentials
    properties.setProperty("user", "test")

    // STEP 2: Register JDBC driver
    Class.forName(JDBC_DRIVER)
    // STEP 3: Open a connection
    DriverManager.getConnection(DB_URL, properties)
  }

  /**
   * convert result set into scala list of map
   * each map represents a row
   *
   * @param queryResult
   * @return
   */
  private def convertResultSetToList(queryResult: ResultSet): List[Map[String, Any]] = {
    val metadata = queryResult.getMetaData
    val colNames = (1 to metadata.getColumnCount) map metadata.getColumnName
    Iterator.continually(buildMapFromQueryResult(queryResult, colNames)).takeWhile(_.isDefined)
      .map(_.get).toList
  }

  private def buildMapFromQueryResult(queryResult: ResultSet,
      colNames: Seq[String]): Option[Map[String, Any]] = {
    if (queryResult.next()) {
      Some(colNames.map(name => name -> queryResult.getObject(name)).toMap)
    }
    else {
      None
    }
  }

  /**
   * CreateSession will create a new session in the Server to connect and execute queries.
   */
  private def createSession: Session = {
    LOGGER.info("\n Creating The Presto Server Session")
    Session.builder(new SessionPropertyManager)
      .setQueryId(new QueryIdGenerator().createNextQueryId)
      .setIdentity(new Identity("user", Optional.empty()))
      .setSource(CARBONDATA_SOURCE).setCatalog(CARBONDATA_CATALOG)
      .setTimeZoneKey(UTC_KEY).setLocale(Locale.ENGLISH)
      .setRemoteUserAddress("address")
      .setUserAgent("agent").build
  }

}
