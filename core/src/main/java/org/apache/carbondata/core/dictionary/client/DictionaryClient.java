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
package org.apache.carbondata.core.dictionary.client;

import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.carbondata.core.dictionary.generator.key.DictionaryKey;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
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

  private DictionaryClientHandler clientHandler = new DictionaryClientHandler();
  ;

  public void startClient(String host, int port) throws Exception {
    Executor bossPool = Executors.newCachedThreadPool();
    Executor workerPool = Executors.newCachedThreadPool();
    ChannelFactory channelFactory = new NioClientSocketChannelFactory(bossPool, workerPool);
    ClientBootstrap bootstrap = new ClientBootstrap(channelFactory);
    try {
      ChannelPipelineFactory pipelineFactory = new ChannelPipelineFactory() {
        public ChannelPipeline getPipeline() throws Exception {
          return Channels.pipeline(new ObjectEncoder(),
              new ObjectDecoder(ClassResolvers.cacheDisabled(getClass().getClassLoader())),
              clientHandler);
        }
      };
      bootstrap.setPipelineFactory(pipelineFactory);

      InetSocketAddress addressToConnectTo = new InetSocketAddress(host, port);
      ChannelFuture cf = bootstrap.connect(addressToConnectTo);
      // Wait until the connection attempt is finished and then the connection is closed.
      cf.sync().getChannel().getCloseFuture().sync();
    } finally {
      bootstrap.releaseExternalResources();
    }
  }

  public DictionaryKey getDictionary(DictionaryKey key) {
    return clientHandler.getDictionary(key);
  }
}
