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

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryKey;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Client handler to get data.
 */
public class DictionaryClientHandler extends ChannelInboundHandlerAdapter {

  private static final LogService LOGGER =
          LogServiceFactory.getLogService(DictionaryClientHandler.class.getName());

  final Map<Long, BlockingQueue<DictionaryKey>> dictKeyQueueMap = new ConcurrentHashMap<>();

  private ChannelHandlerContext ctx;

  private Object lock = new Object();

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    this.ctx = ctx;
    LOGGER.audit("Registered client " + ctx);
    super.channelRegistered(ctx);
  }

  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg)
      throws Exception {
    msg.resetReaderIndex();
    DictionaryKey key = new DictionaryKey();
    key.readData(msg);
    msg.release();
    BlockingQueue<DictionaryKey> dictKeyQueue = dictKeyQueueMap.get(key.getThreadNo());
    dictKeyQueue.offer(key);
  }

  @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    channelRead0(ctx, (ByteBuf)msg);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOGGER.error(cause, "exceptionCaught");
    ctx.close();
  }

  /**
   * client send request to server
   *
   * @param key
   * @return
   */
  public DictionaryKey getDictionary(DictionaryKey key) {
    DictionaryKey dictionaryKey;
    BlockingQueue<DictionaryKey> dictKeyQueue = null;
    try {
      synchronized (lock) {
        dictKeyQueue = dictKeyQueueMap.get(key.getThreadNo());
        if (dictKeyQueue == null) {
          dictKeyQueue = new LinkedBlockingQueue<DictionaryKey>();
          dictKeyQueueMap.put(key.getThreadNo(), dictKeyQueue);
        }
      }
      ByteBuf buffer = Unpooled.buffer();
      key.writeData(buffer);
      ctx.writeAndFlush(buffer);
    } catch (Exception e) {
      LOGGER.error(e, "Error while send request to server " + e.getMessage());
      ctx.close();
    }
    boolean interrupted = false;
    try {
      for (; ; ) {
        try {
          dictionaryKey = dictKeyQueue.take();
          return dictionaryKey;
        } catch (InterruptedException ignore) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
