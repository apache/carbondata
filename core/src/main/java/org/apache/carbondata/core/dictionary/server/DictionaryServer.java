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
package org.apache.carbondata.core.dictionary.server;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryKey;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ClassResolvers;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;


/**
 * Dictionary Server to generate dictionary keys.
 */
public class DictionaryServer {

  private static final LogService LOGGER =
          LogServiceFactory.getLogService(DictionaryServer.class.getName());

  private ServerBootstrap bootstrap;

  private DictionaryServerHandler dictionaryServerHandler;

  /**
   * start dictionary server
   *
   * @param port
   * @throws Exception
   */
  public void startServer(int port) {
    bootstrap = new ServerBootstrap();
    dictionaryServerHandler = new DictionaryServerHandler();

    ExecutorService boss = Executors.newCachedThreadPool();
    ExecutorService worker = Executors.newCachedThreadPool();

    bootstrap.setFactory(new NioServerSocketChannelFactory(boss, worker));

    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("ObjectDecoder", new ObjectDecoder(ClassResolvers.cacheDisabled(
            getClass().getClassLoader())));
        pipeline.addLast("ObjectEncoder", new ObjectEncoder());
        pipeline.addLast("DictionaryServerHandler", dictionaryServerHandler);
        return pipeline;
      }
    });
    bootstrap.bind(new InetSocketAddress(port));
    LOGGER.audit("Server Start!");
  }

  /**
   * shutdown dictionary server
   *
   * @throws Exception
   */
  public void shutdown() throws Exception {
    DictionaryKey key = new DictionaryKey();
    key.setType("WRITE_DICTIONARY");
    dictionaryServerHandler.processMessage(key);
    bootstrap.releaseExternalResources();
    bootstrap.shutdown();
  }
}