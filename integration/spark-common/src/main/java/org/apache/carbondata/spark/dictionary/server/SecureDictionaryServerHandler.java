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
package org.apache.carbondata.spark.dictionary.server;

import java.nio.ByteBuffer;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.dictionary.generator.ServerDictionaryGenerator;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import org.apache.log4j.Logger;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;

/**
 * Handler for Dictionary server.
 */
@ChannelHandler.Sharable public class SecureDictionaryServerHandler extends RpcHandler {

  private static final Logger LOGGER =
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
        generatorForServer.initializeGeneratorForColumn(key);
        return generatorForServer.generateKey(key);
      case SIZE:
        generatorForServer.initializeGeneratorForColumn(key);
        return generatorForServer.size(key);
      case WRITE_TABLE_DICTIONARY:
        generatorForServer.writeTableDictionaryData(key.getTableUniqueId());
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

  public void initializeTable(CarbonTable carbonTable) {
    generatorForServer.initializeGeneratorForTable(carbonTable);
  }
}
