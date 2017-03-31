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
package org.apache.carbondata.core.dictionary.generator.key;

import org.apache.carbondata.core.constants.CarbonCommonConstants;

import io.netty.buffer.ByteBuf;

/**
 * Dictionary key to generate dictionary
 */
public class DictionaryMessage {

  /**
   * tableUniqueName
   */
  private String tableUniqueName;

  /**
   * columnName
   */
  private String columnName;

  /**
   * message data
   */
  private String data;

  /**
   * Dictionary Value
   */
  private int dictionaryValue = CarbonCommonConstants.INVALID_SURROGATE_KEY;

  /**
   * message type
   */
  private DictionaryMessageType type;

  public void readData(ByteBuf byteBuf) {
    byte[] tableBytes = new byte[byteBuf.readInt()];
    byteBuf.readBytes(tableBytes);
    tableUniqueName = new String(tableBytes);

    byte[] colBytes = new byte[byteBuf.readInt()];
    byteBuf.readBytes(colBytes);
    columnName = new String(colBytes);

    byte typeByte = byteBuf.readByte();
    type = getKeyType(typeByte);

    byte dataType = byteBuf.readByte();
    if (dataType == 0) {
      dictionaryValue = byteBuf.readInt();
    } else {
      byte[] dataBytes = new byte[byteBuf.readInt()];
      byteBuf.readBytes(dataBytes);
      data = new String(dataBytes);
    }
  }

  public void writeData(ByteBuf byteBuf) {
    int startIndex = byteBuf.writerIndex();
    // Just reserve the bytes to add length of header at last.
    byteBuf.writeShort(Short.MAX_VALUE);

    byte[] tableBytes = tableUniqueName.getBytes();
    byteBuf.writeInt(tableBytes.length);
    byteBuf.writeBytes(tableBytes);

    byte[] colBytes = columnName.getBytes();
    byteBuf.writeInt(colBytes.length);
    byteBuf.writeBytes(colBytes);

    byteBuf.writeByte(type.getType());

    if (dictionaryValue > 0) {
      byteBuf.writeByte(0);
      byteBuf.writeInt(dictionaryValue);
    } else {
      byteBuf.writeByte(1);
      byte[] dataBytes = data.getBytes();
      byteBuf.writeInt(dataBytes.length);
      byteBuf.writeBytes(dataBytes);
    }
    int endIndex = byteBuf.writerIndex();
    // Add the length of message at the starting.it is required while decoding as in TCP protocol
    // it not guarantee that we receive all data in one packet, so we need to wait to receive all
    // packets before proceeding to process the message.Based on the length it waits.
    byteBuf.setShort(startIndex, endIndex - startIndex - 2);
  }

  private DictionaryMessageType getKeyType(byte type) {
    switch (type) {
      case 1:
        return DictionaryMessageType.DICT_GENERATION;
      case 2:
        return DictionaryMessageType.TABLE_INTIALIZATION;
      case 3:
        return DictionaryMessageType.SIZE;
      case 4:
        return DictionaryMessageType.WRITE_DICTIONARY;
      default:
        return DictionaryMessageType.DICT_GENERATION;
    }
  }

  public String getTableUniqueName() {
    return tableUniqueName;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }

  public DictionaryMessageType getType() {
    return type;
  }

  public void setType(DictionaryMessageType type) {
    this.type = type;
  }

  public void setTableUniqueName(String tableUniqueName) {
    this.tableUniqueName = tableUniqueName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public int getDictionaryValue() {
    return dictionaryValue;
  }

  public void setDictionaryValue(int dictionaryValue) {
    this.dictionaryValue = dictionaryValue;
  }

  @Override public String toString() {
    return "DictionaryKey{ columnName='" + columnName + '\'' + ", data='" + data + '\''
        + ", dictionaryValue=" + dictionaryValue + ", type=" + type + '}';
  }
}
