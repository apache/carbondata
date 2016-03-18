/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
  *
  */
package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.HttpServer
import org.apache.spark.SecurityManager
import java.io.File
import org.apache.spark.util.Utils

/**
  * @author R00900208
  *
  */
object SparkHttpServer {

  def init: HttpServer = {
    val conf = new SparkConf()
    //	 	  private[spark] class HttpServer(
    //    resourceBase: File,
    //    securityManager: SecurityManager,
    //    requestedPort: Int = 0,
    //    serverName: String = "HTTP server")
    //  extends Logging {
    //val broadcastDir = Utils.createTempDir(Utils.getLocalDir(conf))
    val broadcastDir = new File("temp")
    broadcastDir.mkdirs()
    val broadcastPort = conf.getInt("spark.broadcast.port", 0)
    //    val classServer = new HttpServer(broadcastDir, new SecurityManager(conf),0,"HTTP server")
    val classServer = new HttpServer(conf, broadcastDir, new SecurityManager(conf), 0, "HTTP server")
    // Start the classServer and store its URI in a spark system property
    // (which will be passed to executors so that they can connect to it)
    classServer.start()
    println("Class server started, URI = " + classServer.uri)

    classServer
  }

}