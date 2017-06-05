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

import java.io.File
import java.util
import java.util.Locale.ENGLISH
import java.util.Optional

import com.facebook.presto.Session
import com.facebook.presto.execution.QueryIdGenerator
import com.facebook.presto.metadata.SessionPropertyManager
import com.facebook.presto.spi.`type`.TimeZoneKey.UTC_KEY
import com.facebook.presto.spi.security.Identity
import com.facebook.presto.tests.DistributedQueryRunner
import com.google.common.collect.ImmutableMap

object PrestoServerRunner {

  /**
   * CARBONDATA_CATALOG : stores the name of the catalog in the etc/catalog for Apache CarbonData.
   * CARBONDATA_CONNECTOR : stores the name of the CarbonData connector for Presto.
   * The etc/catalog will contain a catalog file for CarbonData with name <CARBONDATA_CATALOG>
   *   .properties
   * and following content :
   * connector.name = <CARBONDATA_CONNECTOR>
   * carbondata-store = <CARBONDATA_STOREPATH>
   */

  val rootPath = new File(this.getClass.getResource("/").getPath
                          + "../../../..").getCanonicalPath
  val CARBONDATA_CATALOG = "carbondata"
  val CARBONDATA_CONNECTOR = "carbondata"
  val CARBONDATA_STOREPATH = s"$rootPath/integration/presto/data/store"
  val CARBONDATA_SOURCE = "carbondata"

  /**
   * Instantiates the Presto Server to connect with the Apache CarbonData
   *
   * @param extraProperties
   * @return Instance of running server.
   * @throws Exception
   */
  @throws[Exception]
  def createQueryRunner(extraProperties: util.Map[String, String]): DistributedQueryRunner = {
    val queryRunner = new DistributedQueryRunner(createSession, 4, extraProperties)
    try {
      queryRunner.installPlugin(new CarbondataPlugin)
      val carbonProperties = ImmutableMap.builder[String, String]
        .put("carbondata-store", CARBONDATA_STOREPATH).build
      /**
       * createCatalog will create a catalog for CarbonData in etc/catalog. It takes following
       * parameters
       * CARBONDATA_CATALOG : catalog name
       * CARBONDATA_CONNECTOR : connector name
       * carbonProperties : Map of properties to be configured for CarbonData Connector.
       */
      queryRunner.createCatalog(CARBONDATA_CATALOG, CARBONDATA_CONNECTOR, carbonProperties)
      queryRunner
    } catch {
      case e: Exception =>
        queryRunner.close()
        throw e
    }
  }

  /**
   * createSession will create a new session in the Server to connect and execute queries.
   *
   * @return a Session instance
   */
  def createSession: Session = {
    Session.builder(new SessionPropertyManager)
      .setQueryId(new QueryIdGenerator().createNextQueryId)
      .setIdentity(new Identity("user", Optional.empty())).setSource(CARBONDATA_SOURCE)
      .setCatalog(CARBONDATA_CATALOG).setTimeZoneKey(UTC_KEY).setLocale(ENGLISH)
      .setRemoteUserAddress("address").setUserAgent("agent").build
  }

}

