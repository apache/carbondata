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

import org.apache.carbondata.core.dictionary.generator.key.DictionaryKey;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Dictionary client to connect to Dictionary server and generate dictionary values
 */
public class DictionaryClient {

  private DictionaryClientHandler dictionaryClientHandler = new DictionaryClientHandler();

  public void startClient(String address, int port) {


    ClientBootstrap clientBootstrap = new ClientBootstrap();

    ExecutorService boss = Executors.newCachedThreadPool();
    ExecutorService worker = Executors.newCachedThreadPool();

    clientBootstrap.setFactory(new NioClientSocketChannelFactory(boss, worker));

    clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("ObjectEncoder", new ObjectEncoder());
        pipeline.addLast("ObjectDecoder", new ObjectDecoder());
        pipeline.addLast("DictionaryClientHandler", dictionaryClientHandler);
        return pipeline;
      }
    });
    clientBootstrap.connect(new InetSocketAddress(address, port));
    System.out.println("Client Start!");
  }

  public DictionaryKey getDictionary(DictionaryKey key) {
    return dictionaryClientHandler.getDictionary(key);
  }
}
