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
package org.apache.carbondata.spark.dictionary.server;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessageType;
import org.apache.carbondata.core.dictionary.server.DictionaryServer;
import org.apache.carbondata.core.dictionary.service.AbstractDictionaryServer;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.util.CarbonProperties;

import com.google.common.collect.Lists;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SecurityManager;
import org.apache.spark.SparkConf;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.netty.SparkTransportConf;
import org.apache.spark.network.sasl.SaslServerBootstrap;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.TransportConf;
import scala.Some;

/**
 * Dictionary Server to generate dictionary keys.
 */
public class SecureDictionaryServer extends AbstractDictionaryServer implements DictionaryServer  {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SecureDictionaryServer.class.getName());

  private SecureDictionaryServerHandler secureDictionaryServerHandler;

  private EventLoopGroup boss;
  private EventLoopGroup worker;
  private int port;
  private String host;
  private SparkConf conf;
  private String secretKey = null;
  private boolean encryptSecureServer;
  private static Object lock = new Object();
  private static SecureDictionaryServer INSTANCE = null;

  private SecureDictionaryServer(SparkConf conf, String host, int port) {
    this.conf = conf;
    this.host = host;
    this.port = port;
    try {
      UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Void>() {
        @Override public Void run() throws Exception {
          startServer();
          return null;
        }
      });
    } catch (IOException io) {
      LOGGER.error(io, "Failed to start Dictionary Server in secure mode");
    } catch (InterruptedException ie) {
      LOGGER.error(ie, "Failed to start Dictionary Server in secure mode");
    }
  }

  public static synchronized DictionaryServer getInstance(SparkConf conf, String host, int port,
      CarbonTable carbonTable) throws Exception {
    if (INSTANCE == null) {
      INSTANCE = new SecureDictionaryServer(conf, host, port);
    }
    INSTANCE.initializeDictionaryGenerator(carbonTable);
    return INSTANCE;
  }

  /**
   * start dictionary server
   *
   */
  @Override
  public void startServer() {
    LOGGER.info("Starting Dictionary Server in Secure Mode");
    secureDictionaryServerHandler = new SecureDictionaryServerHandler();
    String workerThreads = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.DICTIONARY_WORKER_THREADS,
            CarbonCommonConstants.DICTIONARY_WORKER_THREADS_DEFAULT);
    boss = new NioEventLoopGroup(1);
    worker = new NioEventLoopGroup(Integer.parseInt(workerThreads));
    // Configure the server.
    bindToPort();
  }

  /**
   * Binds dictionary server to an available port.
   *
   */
  @Override
  public void bindToPort() {
    long start = System.currentTimeMillis();
    // Configure the server.
    int i = 0;
    while (i < 10) {
      int newPort = port + i;
      try {
        SecurityManager securityManager;
        SparkConf conf = this.conf.clone();
        conf.setAppName("Carbon Dictionary Server");

        // As spark.network.sasl.serverAlwaysEncrypt is not exposed parameter
        // set it explicitly so that Dictionary Server and Client communication
        // occurs encrypted. The below parameter can be removed once spark Documents it.
        // conf.set("spark.network.sasl.serverAlwaysEncrypt", "true");
        conf.set("spark.authenticate.enableSaslEncryption", "true");

        if (conf.get("spark.authenticate.enableSaslEncryption", "false").equalsIgnoreCase("true")) {
          setEncryptSecureServer(true);
        } else {
          setEncryptSecureServer(false);
        }

        TransportConf transportConf =
            SparkTransportConf.fromSparkConf(conf, "Carbon Dictionary Server", 0);
        securityManager = new SecurityManager(conf, Some.<byte[]>empty());
        secretKey = securityManager.getSecretKey();
        TransportContext context =
            new TransportContext(transportConf, secureDictionaryServerHandler);
        TransportServerBootstrap bootstrap =
            new SaslServerBootstrap(transportConf, securityManager);
        String host = findLocalIpAddress(LOGGER);
        //iteratively listening to newports
        context
            .createServer(host, newPort, Lists.<TransportServerBootstrap>newArrayList(bootstrap));
        LOGGER.audit("Dictionary Server started, Time spent " + (System.currentTimeMillis() - start)
            + " Listening on port " + newPort);
        this.port = newPort;
        this.host = host;
        break;
      } catch (Exception e) {
        LOGGER.error(e, "Dictionary Server Failed to bind to port: " + newPort);
        if (i == 9) {
          throw new RuntimeException("Dictionary Server Could not bind to any port");
        }
      }
      i++;
    }
  }

  private void setEncryptSecureServer(boolean encryptSecureServer) {
    this.encryptSecureServer = encryptSecureServer;
  }

  /**
   * @return Port on which the SecureDictionaryServer has started.
   */
  @Override
  public int getPort() {
    return port;
  }

  /**
   * @return IP address on which the SecureDictionaryServer has Started.
   */
  @Override
  public String getHost() {
    return host;
  }

  /**
   * @return Secret Key of Dictionary Server.
   */
  @Override
  public String getSecretKey() {
    return secretKey;
  }

  @Override public boolean isEncryptSecureServer() {
    return encryptSecureServer;
  }

  /**
   * shutdown dictionary server
   *
   * @throws Exception
   */
  @Override
  public void shutdown() throws Exception {
    LOGGER.info("Shutting down dictionary server");
    try {
      UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Void>() {
        @Override public Void run() throws Exception {
          worker.shutdownGracefully();
          boss.shutdownGracefully();
          return null;
        }
      });
    } catch (IOException io) {
      LOGGER.error(io, "Failed to stop Dictionary Server in secure mode");
    } catch (InterruptedException ie) {
      LOGGER.error(ie, "Failed to stop Dictionary Server in secure mode");
    }
  }

  public void initializeDictionaryGenerator(CarbonTable carbonTable) throws Exception {
    secureDictionaryServerHandler.initializeTable(carbonTable);
  }

  /**
   * Write Dictionary for one table.
   *
   * @throws Exception
   */

  @Override
  public void writeTableDictionary(String uniqueTableName) throws Exception {
    DictionaryMessage key = new DictionaryMessage();
    key.setTableUniqueId(uniqueTableName);
    key.setType(DictionaryMessageType.WRITE_TABLE_DICTIONARY);
    secureDictionaryServerHandler.processMessage(key);
  }

}