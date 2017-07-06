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

import java.nio.ByteBuffer;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.dictionary.generator.ServerDictionaryGenerator;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;

/**
 * Handler for Dictionary server.
 */
@ChannelHandler.Sharable public class SecureDictionaryServerHandler extends RpcHandler {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SecureDictionaryServerHandler.class.getName());

  /**
   * dictionary generator
   */
  private ServerDictionaryGenerator generatorForServer = new ServerDictionaryGenerator();

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
        return generatorForServer.generateKey(key);
      case TABLE_INTIALIZATION:
        generatorForServer.initializeGeneratorForTable(key);
        return 0;
      case SIZE:
        return generatorForServer.size(key);
      case WRITE_DICTIONARY:
        generatorForServer.writeDictionaryData();
        return 0;
      case WRITE_TABLE_DICTIONARY:
        generatorForServer.writeTableDictionaryData(key.getTableUniqueName());
        return 0;
      default:
        return -1;
    }
  }

  @Override public void receive(TransportClient transportClient, ByteBuffer byteBuffer,
      RpcResponseCallback rpcResponseCallback) {
    try {
      ByteBuf data = Unpooled.wrappedBuffer(byteBuffer);
      DictionaryMessage key = new DictionaryMessage();
      key.readFullLength(data);
      data.release();
      int outPut = processMessage(key);
      key.setDictionaryValue(outPut);
      // Send back the response
      ByteBuf buff = ByteBufAllocator.DEFAULT.buffer();
      key.writeData(buff);
      rpcResponseCallback.onSuccess(buff.nioBuffer());
    } catch (Exception e) {
      LOGGER.error(e);
    }
  }

  @Override public StreamManager getStreamManager() {
    return new OneForOneStreamManager();
  }
}
