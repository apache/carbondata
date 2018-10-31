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
package org.apache.carbondata.spark.dictionary.client;

import java.nio.charset.Charset;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.dictionary.client.DictionaryClient;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;

import com.google.common.collect.Lists;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.log4j.Logger;
import org.apache.spark.SecurityManager;
import org.apache.spark.SparkConf;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.netty.SparkTransportConf;
import org.apache.spark.network.sasl.SaslClientBootstrap;
import org.apache.spark.network.util.TransportConf;

/**
 * Dictionary client to connect to Dictionary server and generate dictionary values
 */
public class SecureDictionaryClient implements DictionaryClient {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(SecureDictionaryClient.class.getName());

  private SecureDictionaryClientHandler dictionaryClientHandler =
      new SecureDictionaryClientHandler();

  private NioEventLoopGroup workerGroup;
  private TransportClient client;
  private TransportClientFactory clientFactory;

  /**
   * start dictionary client
   *
   * @param address
   * @param port
   */
  @Override public void startClient(String secretKey, String address, int port,
      boolean encryptSecureServer) {
    LOGGER.info("Starting client on " + address + " " + port);
    long start = System.currentTimeMillis();

    SecurityManager securityMgr;
    SparkConf conf = new SparkConf().setAppName("Carbon Dictionary Client");

    conf.set("spark.authenticate", "true");

    if (null != secretKey) {
      conf.set("spark.authenticate.secret", secretKey);
    }

    if (encryptSecureServer) {
      conf.set("spark.authenticate.enableSaslEncryption", "true");
    }

    TransportConf transportConf =
        SparkTransportConf.fromSparkConf(conf, "Carbon Dictionary Client", 0);
    if (null != secretKey) {
      securityMgr = new SecurityManager(conf, scala.Option.apply(secretKey.getBytes(Charset.forName(
          CarbonCommonConstants.DEFAULT_CHARSET))));
    } else {
      securityMgr = new SecurityManager(conf, null);
    }

    TransportContext context = new TransportContext(transportConf, dictionaryClientHandler);
    clientFactory = context.createClientFactory(Lists.<TransportClientBootstrap>newArrayList(
        new SaslClientBootstrap(transportConf, "Carbon Dictionary Client", securityMgr)));

    try {
      client = clientFactory.createClient(address, port);
    } catch (Exception e) {
      LOGGER.error("Dictionary Client Failed to bind to port:", e);
    }
    LOGGER.info(
        "Dictionary client Started, Total time spent : " + (System.currentTimeMillis() - start));
  }

  /**
   * for client request
   *
   * @param key
   * @return
   */
  @Override public DictionaryMessage getDictionary(DictionaryMessage key) {
    return dictionaryClientHandler.getDictionary(key, this.client);
  }

  /**
   * shutdown dictionary client
   */
  @Override public void shutDown() {
    clientFactory.close();
  }
}
