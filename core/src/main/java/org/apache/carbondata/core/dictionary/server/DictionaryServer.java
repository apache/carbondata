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
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.api.CarbonProperties;

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
  private int port;
  private static Object lock = new Object();
  private static volatile DictionaryServer INSTANCE = null;

  private DictionaryServer(int port) {
    startServer(port);
  }

  public static DictionaryServer getInstance(int port, CarbonTable carbonTable) throws Exception {
    if (INSTANCE == null) {
      synchronized (lock) {
        if (INSTANCE == null) {
          INSTANCE = new DictionaryServer(port);
        }
      }
    }
    INSTANCE.initializeDictionaryGenerator(carbonTable);
    return INSTANCE;
  }

  /**
   * start dictionary server
   *
   * @param port
   */
  private void startServer(int port) {
    dictionaryServerHandler = new DictionaryServerHandler();
    boss = new NioEventLoopGroup(1);
    worker = new NioEventLoopGroup(CarbonProperties.DICTIONARY_WORKER_THREADS.getOrDefault());
    // Configure the server.
    bindToPort(port);
  }

  /**
   * Binds dictionary server to an available port.
   *
   * @param port
   */
  private void bindToPort(int port) {
    long start = System.currentTimeMillis();
    // Configure the server.
    int i = 0;
    while (i < 10) {
      int newPort = port + i;
      try {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(boss, worker);
        bootstrap.channel(NioServerSocketChannel.class);
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
          @Override public void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline
                .addLast("LengthDecoder",
                    new LengthFieldBasedFrameDecoder(1048576, 0,
                        2, 0, 2));
            pipeline.addLast("DictionaryServerHandler", dictionaryServerHandler);
          }
        });
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.bind(newPort).sync();
        LOGGER.audit("Dictionary Server started, Time spent " + (System.currentTimeMillis() - start)
            + " Listening on port " + newPort);
        this.port = newPort;
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

  /**
   *
   * @return Port on which the DictionaryServer has started.
   */
  public int getPort() {
    return port;
  }

  /**
   * shutdown dictionary server
   *
   * @throws Exception
   */
  public void shutdown() throws Exception {
    LOGGER.info("Shutting down dictionary server");
    worker.shutdownGracefully();
    boss.shutdownGracefully();
  }

  public void initializeDictionaryGenerator(CarbonTable carbonTable) throws Exception {
    dictionaryServerHandler.initializeTable(carbonTable);
  }

  /**
   *  Write Dictionary for one table.
   * @throws Exception
   */
  public void writeTableDictionary(String tableId) throws Exception {
    DictionaryMessage key = new DictionaryMessage();
    key.setTableUniqueId(tableId);
    key.setType(DictionaryMessageType.WRITE_TABLE_DICTIONARY);
    dictionaryServerHandler.processMessage(key);
  }
}