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
package org.apache.carbondata.spark.dictionary.client;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;

/**
 * Client handler to get data.
 */
public class SecureDictionaryClientHandler extends RpcHandler {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(SecureDictionaryClientHandler.class.getName());

  private final BlockingQueue<DictionaryMessage> responseMsgQueue = new LinkedBlockingQueue<>();

  /**
   * client send request to server
   *
   * @param key DictionaryMessage
   * @return DictionaryMessage
   */
  public DictionaryMessage getDictionary(DictionaryMessage key, TransportClient client) {
    DictionaryMessage dictionaryMessage;
    ByteBuffer resp = null;
    try {

      ByteBuf buffer = ByteBufAllocator.DEFAULT.heapBuffer();
      key.writeData(buffer);
      resp = client.sendRpcSync(buffer.nioBuffer(), 100000);
    } catch (Exception e) {
      LOGGER.error("Error while send request to server ", e);
    }
    try {
      if (resp == null) {
        StringBuilder message = new StringBuilder();
        message.append("DictionaryMessage { ColumnName: ").append(key.getColumnName())
            .append(", DictionaryValue: ").append(key.getDictionaryValue()).append(", type: ")
            .append(key.getType()).append(" }");
        throw new RuntimeException("Request timed out for key : " + message);
      }
      DictionaryMessage newKey = new DictionaryMessage();
      ByteBuf data = Unpooled.wrappedBuffer(resp);
      newKey.readFullLength(data);
      data.release();
      return newKey;
    } catch (Exception e) {
      LOGGER.error(e);
      throw new RuntimeException(e);
    }
  }

  @Override public void receive(TransportClient transportClient, ByteBuffer byteBuffer,
      RpcResponseCallback rpcResponseCallback) {
    try {
      ByteBuf data = Unpooled.wrappedBuffer(byteBuffer);
      DictionaryMessage key = new DictionaryMessage();
      key.readFullLength(data);
      data.release();
      if (responseMsgQueue.offer(key)) {
        LOGGER.info("key: " + key + " added to queue");
      } else {
        LOGGER.error("Failed to add key: " + key + " to queue");
      }
    } catch (Exception e) {
      LOGGER.error(e);
      throw e;
    }
  }

  @Override public StreamManager getStreamManager() {
    return new OneForOneStreamManager();
  }

}
