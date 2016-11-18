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

import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.carbondata.core.dictionary.generator.key.DictionaryKey;

import org.jboss.netty.channel.*;

/**
 * Client handler to get data.
 */
public class DictionaryClientHandler extends SimpleChannelHandler {

  final BlockingQueue<DictionaryKey> answer = new LinkedBlockingQueue<DictionaryKey>();
  private ChannelHandlerContext ctx;

  @Override
  public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    this.ctx = ctx;
    System.out.println("Connected " + ctx.getHandler());
    super.channelConnected(ctx, e);
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    DictionaryKey key = (DictionaryKey) e.getMessage();
    System.out.println("Received new Dictionary Key!");
    answer.offer(key);
    super.messageReceived(ctx, e);
  }

//  @Override
//  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
//    System.out.println("exceptionCaught");
//    ctx.getChannel().close();
//  }

  @Override
  public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    System.out.println("channelDisconnected");
    super.channelDisconnected(ctx, e);
  }

  @Override
  public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    System.out.println("channelClosed");
    super.channelClosed(ctx, e);
  }

  public DictionaryKey getDictionary(DictionaryKey key) {
    DictionaryKey dictionaryKey;
    try {
      ctx.getChannel().write(key);
    } catch (Exception e) {
      e.printStackTrace();
    }
    boolean interrupted = false;
    try {
      for (; ; ) {
        try {
          dictionaryKey = answer.take();
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
