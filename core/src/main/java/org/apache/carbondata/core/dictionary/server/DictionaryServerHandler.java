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
import org.apache.carbondata.core.dictionary.generator.ServerDictionaryGenerator;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryKey;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Handler for Dictionary server.
 */
@ChannelHandler.Sharable
public class DictionaryServerHandler extends ChannelInboundHandlerAdapter {

  private static final LogService LOGGER =
          LogServiceFactory.getLogService(DictionaryServerHandler.class.getName());

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
  public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    LOGGER.audit("Registered " + ctx);
    super.channelRegistered(ctx);
  }

  /**
   * receive message and handle
   *
   * @param ctx
   * @param msg
   * @throws Exception
   */
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg)
      throws Exception {
    DictionaryKey key = new DictionaryKey();
    msg.resetReaderIndex();
    key.readData(msg);
    msg.release();
    int outPut = processMessage(key);
    key.setDictionaryValue(outPut);
    // Send back the response
    ByteBuf buffer = Unpooled.buffer();
    key.writeData(buffer);
    ctx.writeAndFlush(buffer);
  }

  @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    channelRead0(ctx, (ByteBuf) msg);
  }

  /**
   * handle exceptions
   *
   * @param ctx
   * @param cause
   */
  @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOGGER.error(cause, "exceptionCaught");
    ctx.close();
  }

  /**
   * process message by message type
   *
   * @param key
   * @return
   * @throws Exception
   */
  public int processMessage(DictionaryKey key) throws Exception {
    switch (key.getType()) {
      case DICT_GENERATION :
        return generatorForServer.generateKey(key);
      case TABLE_INTIALIZATION :
        generatorForServer.initializeGeneratorForTable(key);
        return 0;
      case SIZE :
        return generatorForServer.size(key);
      case WRITE_DICTIONARY :
        generatorForServer.writeDictionaryData();
        return 0;
      default:
        return -1;
    }
  }

}
