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

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryKey;

import com.alibaba.fastjson.JSON;

import org.jboss.netty.channel.*;

/**
 * Client handler to get data.
 */
public class DictionaryClientHandler extends SimpleChannelHandler {

  private static final LogService LOGGER =
          LogServiceFactory.getLogService(DictionaryClientHandler.class.getName());

  final Map<String, BlockingQueue<DictionaryKey>> dictKeyQueueMap = new ConcurrentHashMap<>();

  private ChannelHandlerContext ctx;

  private Object lock = new Object();

  @Override
  public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    this.ctx = ctx;
    System.out.println("Connected " + ctx.getHandler());
    super.channelConnected(ctx, e);
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    String backkeyString = (String) e.getMessage();
    DictionaryKey key = JSON.parseObject(backkeyString, DictionaryKey.class);
    BlockingQueue<DictionaryKey> dictKeyQueue = dictKeyQueueMap.get(key.getThreadNo());
    dictKeyQueue.offer(key);
    super.messageReceived(ctx, e);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    System.out.println("exceptionCaught");
    ctx.getChannel().close();
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
      String keyString = JSON.toJSONString(key);
      ctx.getChannel().write(keyString);
    } catch (Exception e) {
      LOGGER.error("Error while send request to server " + e.getMessage());
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
