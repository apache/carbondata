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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.log4j.Logger;

/**
 * Client handler to get data.
 */
public class NonSecureDictionaryClientHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOGGER =
          LogServiceFactory.getLogService(NonSecureDictionaryClientHandler.class.getName());

  private final BlockingQueue<DictionaryMessage> responseMsgQueue = new LinkedBlockingQueue<>();

  private ChannelHandlerContext ctx;

  private DictionaryChannelFutureListener channelFutureListener;

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    this.ctx = ctx;
    channelFutureListener = new DictionaryChannelFutureListener(ctx);
    LOGGER.info("Connected client " + ctx);
    super.channelActive(ctx);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    try {
      ByteBuf data = (ByteBuf) msg;
      DictionaryMessage key = new DictionaryMessage();
      key.readSkipLength(data);
      data.release();
      responseMsgQueue.add(key);
    } catch (Exception e) {
      LOGGER.error(e);
      throw e;
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOGGER.error("exceptionCaught", cause);
    ctx.close();
  }

  /**
   * client send request to server
   *
   * @param key DictionaryMessage
   * @return DictionaryMessage
   */
  public DictionaryMessage getDictionary(DictionaryMessage key) {
    DictionaryMessage dictionaryMessage;
    try {
      ByteBuf buffer = ctx.alloc().buffer();
      key.writeData(buffer);
      ctx.writeAndFlush(buffer).addListener(channelFutureListener);
    } catch (Exception e) {
      LOGGER.error("Error while send request to server ", e);
      ctx.close();
    }
    try {
      dictionaryMessage = responseMsgQueue.poll(100, TimeUnit.SECONDS);
      if (dictionaryMessage == null) {
        StringBuilder message = new StringBuilder();
        message.append("DictionaryMessage { ColumnName: ")
            .append(key.getColumnName())
            .append(", DictionaryValue: ")
            .append(key.getDictionaryValue())
            .append(", type: ")
            .append(key.getType());
        throw new RuntimeException("Request timed out for key : " + message);
      }
      return dictionaryMessage;
    } catch (Exception e) {
      LOGGER.error(e);
      throw new RuntimeException(e);
    }
  }

  private static class DictionaryChannelFutureListener implements ChannelFutureListener {

    private ChannelHandlerContext ctx;

    DictionaryChannelFutureListener(ChannelHandlerContext ctx) {
      this.ctx = ctx;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      if (!future.isSuccess()) {
        LOGGER.error("Error while sending request to Dictionary Server", future.cause());
        ctx.close();
      }
    }
  }
}
