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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessageType;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * Dictionary Server to generate dictionary keys.
 */
public class DictionaryServer {

  private static final LogService LOGGER =
          LogServiceFactory.getLogService(DictionaryServer.class.getName());

  private DictionaryServerHandler dictionaryServerHandler;

  private EventLoopGroup boss;
  private EventLoopGroup worker;

  /**
   * start dictionary server
   *
   * @param port
   * @throws Exception
   */
  public void startServer(int port) {
    long start = System.currentTimeMillis();
    dictionaryServerHandler = new DictionaryServerHandler();
    boss = new NioEventLoopGroup();
    worker = new NioEventLoopGroup();
    // Configure the server.
    try {
      ServerBootstrap bootstrap = new ServerBootstrap();
      bootstrap.group(boss, worker);
      bootstrap.channel(NioServerSocketChannel.class);

      bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
        @Override public void initChannel(SocketChannel ch) throws Exception {
          ChannelPipeline pipeline = ch.pipeline();
          // Based on length provided at header, it collects all packets
          pipeline.addLast("LengthDecoder", new LengthFieldBasedFrameDecoder(1048576, 0, 2, 0, 2));
          pipeline.addLast("DictionaryServerHandler", dictionaryServerHandler);
        }
      });
      bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
      bootstrap.bind(port).sync();

      LOGGER.info("Dictionary Server started, Time spent " + (System.currentTimeMillis() - start)
          + " Listening on port " + port);
    } catch (Exception e) {
      LOGGER.error(e, "Dictionary Server Start Failed");
      throw new RuntimeException(e);
    }
  }

  /**
   * shutdown dictionary server
   *
   * @throws Exception
   */
  public void shutdown() throws Exception {
    worker.shutdownGracefully();
    boss.shutdownGracefully();
    // Wait until all threads are terminated.
    boss.terminationFuture().sync();
    worker.terminationFuture().sync();
  }

  /**
   * Write dictionary to the store.
   * @throws Exception
   */
  public void writeDictionary() throws Exception {
    DictionaryMessage key = new DictionaryMessage();
    key.setType(DictionaryMessageType.WRITE_DICTIONARY);
    dictionaryServerHandler.processMessage(key);
  }
}