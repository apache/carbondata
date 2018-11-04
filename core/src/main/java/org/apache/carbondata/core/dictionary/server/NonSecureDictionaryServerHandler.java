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

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.dictionary.generator.ServerDictionaryGenerator;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.log4j.Logger;

/**
 * Handler for Dictionary server.
 */
@ChannelHandler.Sharable public class NonSecureDictionaryServerHandler
    extends ChannelInboundHandlerAdapter {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(NonSecureDictionaryServerHandler.class.getName());

  /**
   * dictionary generator
   */
  private ServerDictionaryGenerator generatorForServer = new ServerDictionaryGenerator();

  /**
   * channel registered
   *
   * @param ctx
   * @throws Exception
   */
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    LOGGER.info("Connected " + ctx);
    super.channelActive(ctx);
  }

  @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    try {
      ByteBuf data = (ByteBuf) msg;
      DictionaryMessage key = new DictionaryMessage();
      key.readSkipLength(data);
      data.release();
      int outPut = processMessage(key);
      key.setDictionaryValue(outPut);
      // Send back the response
      ByteBuf buffer = ctx.alloc().buffer();
      key.writeData(buffer);
      ctx.writeAndFlush(buffer);
    } catch (Exception e) {
      LOGGER.error(e);
      throw e;
    }
  }

  /**
   * handle exceptions
   *
   * @param ctx
   * @param cause
   */
  @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOGGER.error("exceptionCaught", cause);
    ctx.close();
  }

  /**
   * process message by message type
   *
   * @param key
   * @return
   * @throws Exception
   */
  public int processMessage(DictionaryMessage key) throws Exception {
    switch (key.getType()) {
      case DICT_GENERATION:
        generatorForServer.initializeGeneratorForColumn(key);
        return generatorForServer.generateKey(key);
      case SIZE:
        generatorForServer.initializeGeneratorForColumn(key);
        return generatorForServer.size(key);
      case WRITE_TABLE_DICTIONARY:
        generatorForServer
            .writeTableDictionaryData(key.getTableUniqueId());
        return 0;
      default:
        return -1;
    }
  }

  void initializeTable(CarbonTable carbonTable) {
    generatorForServer.initializeGeneratorForTable(carbonTable);
  }

}
