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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * Dictionary client to connect to Dictionary server and generate dictionary values
 */
public class DictionaryClient {

  private static final LogService LOGGER =
          LogServiceFactory.getLogService(DictionaryClient.class.getName());

  private DictionaryClientHandler dictionaryClientHandler = new DictionaryClientHandler();

  private NioEventLoopGroup workerGroup;

  /**
   * start dictionary client
   *
   * @param address
   * @param port
   */
  public void startClient(String address, int port) {
    long start = System.currentTimeMillis();
    workerGroup = new NioEventLoopGroup();
    Bootstrap clientBootstrap = new Bootstrap();
    clientBootstrap.group(workerGroup).channel(NioSocketChannel.class)
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override public void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            // Based on length provided at header, it collects all packets
            pipeline
                .addLast("LengthDecoder", new LengthFieldBasedFrameDecoder(1048576, 0, 2, 0, 2));
            pipeline.addLast("DictionaryClientHandler", dictionaryClientHandler);
          }
        });
    clientBootstrap.connect(new InetSocketAddress(address, port));
    LOGGER.info(
        "Dictionary client Started, Total time spent : " + (System.currentTimeMillis() - start));
  }

  /**
   * for client request
   *
   * @param key
   * @return
   */
  public DictionaryMessage getDictionary(DictionaryMessage key) {
    return dictionaryClientHandler.getDictionary(key);
  }

  /**
   * shutdown dictionary client
   */
  public void shutDown() {
    workerGroup.shutdownGracefully();
    try {
      workerGroup.terminationFuture().sync();
    } catch (InterruptedException e) {
      LOGGER.error(e);
    }
  }
}
