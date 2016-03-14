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