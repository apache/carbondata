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
package org.apache.carbondata.core.dictionary.client;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryKey;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ClassResolvers;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;


/**
 * Dictionary client to connect to Dictionary server and generate dictionary values
 */
public class DictionaryClient {

  private static final LogService LOGGER =
          LogServiceFactory.getLogService(DictionaryClient.class.getName());

  private DictionaryClientHandler dictionaryClientHandler = new DictionaryClientHandler();

  private ClientBootstrap clientBootstrap;

  /**
   * start dictionary client
   *
   * @param address
   * @param port
   */
  public void startClient(String address, int port) {
    clientBootstrap = new ClientBootstrap();
    ExecutorService boss = Executors.newCachedThreadPool();
    ExecutorService worker = Executors.newCachedThreadPool();
    clientBootstrap.setFactory(new NioClientSocketChannelFactory(boss, worker));
    clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("ObjectEncoder", new ObjectEncoder());
        pipeline.addLast("ObjectDecoder", new ObjectDecoder(ClassResolvers.cacheDisabled(
            getClass().getClassLoader())));
        pipeline.addLast("DictionaryClientHandler", dictionaryClientHandler);
        return pipeline;
      }
    });
    clientBootstrap.connect(new InetSocketAddress(address, port));
    LOGGER.audit("Client Start!");
  }

  /**
   * for client request
   *
   * @param key
   * @return
   */
  public DictionaryKey getDictionary(DictionaryKey key) {
    return dictionaryClientHandler.getDictionary(key);
  }

  /**
   * shutdown dictionary client
   */
  public void shutDown() {
    clientBootstrap.releaseExternalResources();
    clientBootstrap.shutdown();
  }
}
