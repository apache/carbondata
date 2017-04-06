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

package org.apache.carbondata.hive.server;

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.Service;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.server.HiveServer2;

/**
 * Utility starting a local/embedded Hive org.apache.carbondata.hive.server for testing purposes.
 * Uses sensible defaults to properly clean between reruns.
 * Additionally it wrangles the Hive internals so it rather executes the jobs locally not within a child JVM (which Hive calls local) or external.
 */
public class HiveEmbeddedServer2 {
  private static final String SCRATCH_DIR = "/tmp/hive";
  private static Log log = LogFactory.getLog(Hive.class);
  private HiveServer2 hiveServer;
  private HiveConf config;
  private int port;

  public void start() throws Exception {
    log.info("Starting Hive Local/Embedded Server...");
    if (hiveServer == null) {
      config = configure();
      hiveServer = new HiveServer2();
      port = MetaStoreUtils.findFreePort();
      config.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, port);
      hiveServer.init(config);
      hiveServer.start();
      waitForStartup();
    }
  }

  public int getFreePort() {
    log.info("Free Port Available is " + port);
    return port;
  }

  private void waitForStartup() throws Exception {
    long timeout = TimeUnit.MINUTES.toMillis(1);
    long unitOfWait = TimeUnit.SECONDS.toMillis(1);

    CLIService hs2Client = getServiceClientInternal();
    SessionHandle sessionHandle = null;
    for (int interval = 0; interval < timeout / unitOfWait; interval++) {
      Thread.sleep(unitOfWait);
      try {
        Map<String, String> sessionConf = new HashMap<String, String>();
        sessionHandle = hs2Client.openSession("foo", "bar", sessionConf);
        return;
      } catch (Exception e) {
        // service not started yet
        continue;
      } finally {
        hs2Client.closeSession(sessionHandle);
      }
    }
    throw new TimeoutException("Couldn't get a hold of HiveServer2...");
  }

  private CLIService getServiceClientInternal() {
    for (Service service : hiveServer.getServices()) {
      if (service instanceof CLIService) {
        return (CLIService) service;
      }
    }
    throw new IllegalStateException("Cannot find CLIService");
  }

  private HiveConf configure() throws Exception {
    log.info("Setting The Hive Conf Variables");
    String scratchDir = SCRATCH_DIR;

    File scratchDirFile = new File(scratchDir);
    //TestUtils.delete(scratchDirFile);

    Configuration cfg = new Configuration();
    HiveConf conf = new HiveConf(cfg, HiveConf.class);
    conf.addToRestrictList("columns.comments");
    conf.set("hive.scratch.dir.permission", "777");
    conf.setVar(ConfVars.SCRATCHDIRPERMISSION, "777");
    scratchDirFile.mkdirs();
    // also set the permissions manually since Hive doesn't do it...
    scratchDirFile.setWritable(true, false);

    int random = new Random().nextInt();

    conf.set("hive.metastore.warehouse.dir", scratchDir + "/warehouse" + random);
    conf.set("hive.metastore.metadb.dir", scratchDir + "/metastore_db" + random);
    conf.set("hive.exec.scratchdir", scratchDir);
    conf.set("fs.permissions.umask-mode", "022");
    conf.set("javax.jdo.option.ConnectionURL",
        "jdbc:derby:;databaseName=" + scratchDir + "/metastore_db" + random + ";create=true");
    conf.set("hive.metastore.local", "true");
    conf.set("hive.aux.jars.path", "");
    conf.set("hive.added.jars.path", "");
    conf.set("hive.added.files.path", "");
    conf.set("hive.added.archives.path", "");
    conf.set("fs.default.name", "file:///");

    // clear mapred.job.tracker - Hadoop defaults to 'local' if not defined. Hive however expects this to be set to 'local' - if it's not, it does a remote execution (i.e. no child JVM)
    Field field = Configuration.class.getDeclaredField("properties");
    field.setAccessible(true);
    Properties props = (Properties) field.get(conf);
    props.remove("mapred.job.tracker");
    props.remove("mapreduce.framework.name");
    props.setProperty("fs.default.name", "file:///");

    // intercept SessionState to clean the threadlocal
    Field tss = SessionState.class.getDeclaredField("tss");
    tss.setAccessible(true);
    return new HiveConf(conf);
  }

  public void stop() {
    if (hiveServer != null) {
      log.info("Stopping Hive Local/Embedded Server...");
      hiveServer.stop();
      hiveServer = null;
      config = null;
      log.info("Hive Local/Embedded Server Stopped SucessFully...");

    }
  }

}