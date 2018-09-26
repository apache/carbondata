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

package org.apache.carbondata.server

import java.io.File
import java.lang.reflect.Field
import java.util.{ArrayList, Collections, HashMap, List, Map, Properties, Random}
import java.util.concurrent.{TimeoutException, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.commons.lang.StringUtils
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.MetaStoreUtils
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hive.service.cli.{CLIService, OperationHandle, RowSet, SessionHandle}
import org.apache.hive.service.server.HiveServer2

/**
 * Utility starting a local/embedded Hive server for testing purposes.
 * Uses sensible defaults to properly clean between reruns.
 * Additionally it wrangles the Hive internals so it rather executes the jobs locally not within
 * a child JVM (which Hive calls local) or external.
 */

object HiveEmbeddedServer2 {

  private val SCRATCH_DIR: String = "/tmp/hive"
  private val log: Log = LogFactory.getLog(classOf[Hive])
  private var hiveServer: HiveServer2 = _

  def start(): Int = {
    log.info("Starting Hive Local/Embedded Server...")
    if (Option(hiveServer).isEmpty) {
      val config = configure()
      hiveServer = new HiveServer2()
      val port = MetaStoreUtils.findFreePort()
      log.info(s"Free port Available is $port")
      config.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, port)
      hiveServer.init(config)
      hiveServer.start()
      waitForStartup()
      port
    }
    else {
      throw new Exception("Server Is Already Running Stop It First")
    }
  }

  private def waitForStartup(): Unit = {
    val timeout: Long = TimeUnit.MINUTES.toMillis(1)
    val unitOfWait: Long = TimeUnit.SECONDS.toMillis(1)
    val hs2Client: CLIService = getServiceClientInternal
    var sessionHandle: SessionHandle = null
    for (_ <- 0 until (timeout / unitOfWait).toInt) {
      Thread.sleep(unitOfWait)
      try {
        val sessionConf: Map[String, String] = new HashMap[String, String]()
        sessionHandle = hs2Client.openSession("foo", "bar", sessionConf)
        return
      } catch {
        case e: Exception => log.error(e.getMessage)

      } finally hs2Client.closeSession(sessionHandle)
    }
    throw new TimeoutException("Couldn't get a hold of HiveServer2...")
  }

  private def getServiceClientInternal(): CLIService = {
    hiveServer.getServices.asScala.toList.find(_.isInstanceOf[CLIService])
      .fold(throw new Exception("Cli Service Not Found")) { cliService =>
        cliService
          .asInstanceOf[CLIService]
      }
  }

  private def configure(): HiveConf = {
    log.info("Setting The Hive Conf Variables")
    val scratchDir: String = SCRATCH_DIR
    val scratchDirFile: File = new File(scratchDir)
    val cfg: Configuration = new Configuration()
    val conf: HiveConf = new HiveConf(cfg, classOf[HiveConf])
    conf.addToRestrictList("columns.comments")
    conf.set("hive.scratch.dir.permission", "777")
    conf.setVar(ConfVars.SCRATCHDIRPERMISSION, "777")
    scratchDirFile.mkdirs()
    // also set the permissions manually since Hive doesn't do it...
    scratchDirFile.setWritable(true, false)
    val random: Int = new Random().nextInt()
    conf.set("hive.metastore.warehouse.dir",
      scratchDir + "/warehouse" + random)
    conf.set("hive.metastore.metadb.dir",
      scratchDir + "/metastore_db" + random)
    conf.set("hive.exec.scratchdir", scratchDir)
    conf.set("fs.permissions.umask-mode", "022")
    conf.set("javax.jdo.option.ConnectionURL",
      "jdbc:derby:;databaseName=" + scratchDir + "/metastore_db" +
      random +
      ";create=true")
    conf.set("hive.metastore.local", "true")
    conf.set("hive.aux.jars.path", "")
    conf.set("hive.added.jars.path", "")
    conf.set("hive.added.files.path", "")
    conf.set("hive.added.archives.path", "")
    conf.set("fs.default.name", "file:///")
    // clear mapred.job.tracker - Hadoop defaults to 'local' if not defined. Hive however expects
    // this to be set to 'local' - if it's not, it does a remote execution (i.e. no child JVM)
    val field: Field = classOf[Configuration].getDeclaredField("properties")
    field.setAccessible(true)
    val props: Properties = field.get(conf).asInstanceOf[Properties]
    props.remove("mapred.job.tracker")
    props.remove("mapreduce.framework.name")
    props.setProperty("fs.default.name", "file:///")
    // intercept SessionState to clean the threadlocal
    val tss: Field = classOf[SessionState].getDeclaredField("tss")
    tss.setAccessible(true)
    new HiveConf(conf)
  }

  def getFreePort: Int = MetaStoreUtils.findFreePort()

  def execute(cmd: String): List[String] = {
    if (cmd.toUpperCase().startsWith("ADD JAR")) {
      // skip the jar since we're running in local mode
      Collections.emptyList()
    }
    // remove bogus configuration
    val client: CLIService = getServiceClientInternal
    var sh: SessionHandle = null
    try {
      val opConf: Map[String, String] = new HashMap[String, String]()
      sh = client.openSession("anonymous", "anonymous", opConf)
      val oh: OperationHandle = client.executeStatement(sh, cmd, opConf)
      if (oh.hasResultSet()) {
        val rows: RowSet = client.fetchResults(oh)
        val result: List[String] = new ArrayList[String](rows.numRows())
        for (objects <- rows.asScala) {
          result.add(concatenate(objects, ","))
        }
        return result
      }
      Collections.emptyList()
    } finally if (sh != null) {
      client.closeSession(sh)
    }
  }


  private def concatenate(array: Array[AnyRef], delimiter: String): String = {
    if (array == null || array.length == 0) {
      StringUtils.EMPTY
    }

    val sb: StringBuilder = new StringBuilder()
    for (i <- array.indices) {
      if (i > 0) {
        sb.append(delimiter)
      }
      sb.append(array(i))
    }
    sb.toString
  }

  def stop(): Unit = {
    if (hiveServer != null) {
      log.info("Stopping Hive Local/Embedded Server...")
      hiveServer.stop()
      hiveServer = null
      log.info("Hive Local/Embedded Server Stopped SucessFully...")
    }
  }

}
