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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.carbondata.core.dictionary.generator.key.DictionaryKey;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * Client handler to get data.
 */
public class DictionaryClientHandler extends SimpleChannelHandler {

  final BlockingQueue<DictionaryKey> answer = new LinkedBlockingQueue<DictionaryKey>();
  private ChannelHandlerContext ctx;

  @Override public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
      throws Exception {
    System.out.println("Connected");
    ctx.sendUpstream(e);
    this.ctx = ctx;
  }

  public DictionaryKey getDictionary(DictionaryKey key) {
    ctx.getChannel().write(key);
    boolean interrupted = false;
    try {
      for (; ; ) {
        try {
          return answer.take();
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

  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    DictionaryKey key = (DictionaryKey) e.getMessage();
    answer.offer(key);
  }

  @Override public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    // TODO handle properly
    e.getCause().printStackTrace();
    e.getChannel().close();
  }

}
