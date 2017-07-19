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



import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessageType;
import org.apache.carbondata.core.dictionary.server.DictionaryServer;
import org.apache.carbondata.core.dictionary.server.SecureDictionaryServerHandler;
import org.apache.carbondata.core.dictionary.service.AbstractDictionaryServer;
import org.apache.carbondata.core.util.CarbonProperties;

import com.google.common.collect.Lists;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.spark.SecurityManager;
import org.apache.spark.SparkConf;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.netty.SparkTransportConf;
import org.apache.spark.network.sasl.SaslServerBootstrap;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.security.CryptoStreamUtils;

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
  private String secretKey;
  private static Object lock = new Object();
  private static SecureDictionaryServer INSTANCE = null;

  private SecureDictionaryServer(SparkConf conf, String host, int port) {
    this.conf = conf;
    this.host = host;
    this.port = port;
    startServer();
  }

  public static DictionaryServer getInstance(SparkConf conf, String host, int port) {
    if (INSTANCE == null) {
      synchronized (lock) {
        if (INSTANCE == null) {
          INSTANCE = new SecureDictionaryServer(conf, host, port);
        }
      }
    }
    return INSTANCE;
  }

  /**
   * start dictionary server
   *
   */
  @Override
  public void startServer() {
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
        SparkConf conf = this.conf.clone();
        conf.setAppName("Carbon Dictionary Server");
        secretKey = generateSecretKey(conf);
        conf.set("spark.authenticate.secret", secretKey);
        TransportConf transportConf =
            SparkTransportConf.fromSparkConf(conf, "Carbon Dictionary Server", 0);
        SecurityManager securityMgr =
            new SecurityManager(conf, scala.Option.apply(secretKey.getBytes()));
        TransportContext context =
            new TransportContext(transportConf, secureDictionaryServerHandler);

        TransportServerBootstrap bootstrap = new SaslServerBootstrap(transportConf, securityMgr);
        String host = findLocalIpAddress(LOGGER);
        TransportServer transportServer = context
            .createServer(host, port, Lists.<TransportServerBootstrap>newArrayList(bootstrap));
        LOGGER.audit("Dictionary Server started, Time spent " + (System.currentTimeMillis() - start)
            + " Listening on port " + newPort);
        this.port = newPort;
        this.host = host;
        break;
      } catch (Exception e) {
        LOGGER.error(e, "Dictionary Server Failed to bind to port:");
        if (i == 9) {
          throw new RuntimeException("Dictionary Server Could not bind to any port");
        }
      }
      i++;
    }
  }

  public String generateSecretKey(SparkConf conf) {
    byte[] keygen;
    if (conf.get("spark.io.encryption.enabled", "false").equalsIgnoreCase("true")) {
      keygen = CryptoStreamUtils.createKey(conf);
      return keygen.toString();
    } else {
      return null;
    }
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

  /**
   * shutdown dictionary server
   *
   * @throws Exception
   */
  @Override
  public void shutdown() throws Exception {
    LOGGER.info("Shutting down dictionary server");
    worker.shutdownGracefully();
    boss.shutdownGracefully();
  }

  /**
   * Write dictionary to the store.
   *
   * @throws Exception
   */
  @Override
  public void writeDictionary() throws Exception {
    DictionaryMessage key = new DictionaryMessage();
    key.setType(DictionaryMessageType.WRITE_DICTIONARY);
    secureDictionaryServerHandler.processMessage(key);
  }

  /**
   * Write Dictionary for one table.
   *
   * @throws Exception
   */

  @Override
  public void writeTableDictionary(String uniqueTableName) throws Exception {
    DictionaryMessage key = new DictionaryMessage();
    key.setTableUniqueName(uniqueTableName);
    key.setType(DictionaryMessageType.WRITE_TABLE_DICTIONARY);
    secureDictionaryServerHandler.processMessage(key);
  }

}