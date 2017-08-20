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

package org.apache.carbondata.core.reader;

import java.io.IOException;

import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.format.ColumnDictionaryChunkMeta;

import mockit.Mock;
import mockit.MockUp;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;

public class ThriftReaderTest {

  private static ThriftReader thriftReader = null;

  @BeforeClass public static void setup() {
    thriftReader = new ThriftReader("TestFile.carbon");
  }

  @Test(expected = java.io.IOException.class) public void testReadForException()
      throws IOException {
    ThriftReader.TBaseCreator tBaseCreator = new ThriftReader.TBaseCreator() {
      @Override public TBase create() {
        return new ColumnDictionaryChunkMeta();
      }

    };
    new MockUp<ColumnDictionaryChunkMeta>() {
      @Mock public void read(org.apache.thrift.protocol.TProtocol iprot)
          throws org.apache.thrift.TException {
        throw new TException("TException Occur");
      }

    };
    thriftReader = new ThriftReader("TestFile.carbon", tBaseCreator);
    thriftReader.read();
  }

  @Test public void testReadWithTBaseCreator() throws IOException {
    ThriftReader.TBaseCreator tBaseCreator = new ThriftReader.TBaseCreator() {
      @Override public TBase create() {
        return new ColumnDictionaryChunkMeta();
      }
    };
    new MockUp<ColumnDictionaryChunkMeta>() {
      @Mock public void read(org.apache.thrift.protocol.TProtocol iprot) {

      }

    };
    assertNotNull(thriftReader.read(tBaseCreator));

  }

  @Test(expected = java.io.IOException.class) public void testReadWithTBaseCreatorForException()
      throws IOException {
    ThriftReader.TBaseCreator tBaseCreator = new ThriftReader.TBaseCreator() {
      @Override public TBase create() {
        return new ColumnDictionaryChunkMeta();
      }
    };
    new MockUp<ColumnDictionaryChunkMeta>() {
      @Mock public void read(org.apache.thrift.protocol.TProtocol iprot)
          throws org.apache.thrift.TException {
        throw new TException("TException Occur");
      }

    };
    thriftReader.read(tBaseCreator);
  }

}
