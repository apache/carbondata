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

import io.netty.buffer.ByteBuf;

/**
 * Dictionary key to generate dictionary
 */
public class DictionaryKey {

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
  private int dictionaryValue = -1;

  /**
   * message type
   */
  private DictionaryKeyType type;

  /**
   * dictionary client thread no
   */
  private long threadNo;

  public void readData(ByteBuf byteBuf) {
    byte[] tableBytes = new byte[byteBuf.readInt()];
    byteBuf.readBytes(tableBytes);
    tableUniqueName = new String(tableBytes);

    byte[] colBytes = new byte[byteBuf.readInt()];
    byteBuf.readBytes(colBytes);
    columnName = new String(colBytes);

    threadNo = byteBuf.readLong();

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
    byte[] tableBytes = tableUniqueName.getBytes();
    byteBuf.writeInt(tableBytes.length);
    byteBuf.writeBytes(tableBytes);

    byte[] colBytes = columnName.getBytes();
    byteBuf.writeInt(colBytes.length);
    byteBuf.writeBytes(colBytes);

    byteBuf.writeLong(threadNo);

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
  }

  private DictionaryKeyType getKeyType(byte type) {
    switch (type) {
      case 1 :
        return DictionaryKeyType.DICT_GENERATION;
      case 2 :
        return DictionaryKeyType.TABLE_INTIALIZATION;
      case 3 :
        return DictionaryKeyType.SIZE;
      case 4 :
        return DictionaryKeyType.WRITE_DICTIONARY;
      default:
        return DictionaryKeyType.DICT_GENERATION;
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

  public void setThreadNo(long threadNo) {
    this.threadNo = threadNo;
  }

  public long getThreadNo() {
    return this.threadNo;
  }

  public DictionaryKeyType getType() {
    return type;
  }

  public void setType(DictionaryKeyType type) {
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
}
