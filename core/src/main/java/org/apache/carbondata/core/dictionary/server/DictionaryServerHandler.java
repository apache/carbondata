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
package org.apache.carbondata.core.dictionary.server;

import org.apache.carbondata.core.dictionary.generator.DictionaryGeneratorForServer;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryKey;
import org.jboss.netty.channel.*;


/**
 * Handler for Dictionary server.
 */
public class DictionaryServerHandler extends SimpleChannelHandler {

  private DictionaryGeneratorForServer generatorForServer = new DictionaryGeneratorForServer();

  public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    System.out.println("Connected " + ctx.getHandler());
  }

  @Override public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
      throws Exception {
    DictionaryKey key = (DictionaryKey) e.getMessage();
    int outPut = processMessage(key);
    key.setData(outPut);
    // Send back the response
    ctx.getChannel().write(key);
    super.messageReceived(ctx, e);
  }

  @Override public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    e.getCause().printStackTrace();
    Channel ch = e.getChannel();
    ch.close();
  }

  public Integer processMessage(DictionaryKey key) throws Exception {
    switch (key.getMessageType()) {
      case DICTIONARY_GENERATION:
        return generatorForServer.generateKey(key);
      case TABLE_INTIALIZATION:
        generatorForServer.initializeGeneratorForTable(key);
        return 0;
      case SIZE:
        return generatorForServer.size(key);
      case WRITE_DICTIONARY:
        generatorForServer.writeDictionaryData();
        return 0;
      default:
        return -1;
    }
  }

}
