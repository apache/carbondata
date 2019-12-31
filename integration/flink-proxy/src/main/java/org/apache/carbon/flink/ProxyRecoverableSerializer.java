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

package org.apache.carbon.flink;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.flink.core.io.SimpleVersionedSerializer;

public final class ProxyRecoverableSerializer
        implements SimpleVersionedSerializer<ProxyRecoverable> {

  public static final ProxyRecoverableSerializer INSTANCE = new ProxyRecoverableSerializer();

  public static final int VERSION = 1;

  private static final byte TRUE = 0;

  private static final byte FALSE = 1;

  private static final String CHARSET = "UTF-8";

  // TODO: make it configurable
  private static final int BUFFER_SIZE = 10240;

  private ProxyRecoverableSerializer() {
    // private constructor.
  }

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(final ProxyRecoverable proxyRecoverable) {
    final ByteBuffer byteBuffer = ByteBuffer.allocate(BUFFER_SIZE);
    serializeString(byteBuffer, proxyRecoverable.getWriterType());
    serializeConfiguration(byteBuffer, proxyRecoverable.getWriterConfiguration());
    serializeString(byteBuffer, proxyRecoverable.getWriterIdentifier());
    serializeString(byteBuffer, proxyRecoverable.getWritePath());
    final byte[] bytes = new byte[byteBuffer.position()];
    byteBuffer.position(0);
    byteBuffer.get(bytes);
    return bytes;
  }

  private static void serializeConfiguration(
      final ByteBuffer byteBuffer,
      final ProxyFileWriterFactory.Configuration configuration
  ) {
    serializeString(byteBuffer, configuration.getDatabaseName());
    serializeString(byteBuffer, configuration.getTableName());
    serializeString(byteBuffer, configuration.getTablePath());
    serializeProperties(byteBuffer, configuration.getTableProperties());
    serializeProperties(byteBuffer, configuration.getWriterProperties());
    serializeProperties(byteBuffer, configuration.getCarbonProperties());
  }

  private static void serializeString(final ByteBuffer byteBuffer, final String string) {
    if (string == null) {
      byteBuffer.put(TRUE);
    } else {
      byteBuffer.put(FALSE);
      final byte[] stringBytes;
      try {
        stringBytes = string.getBytes(CHARSET);
      } catch (UnsupportedEncodingException exception) {
        throw new RuntimeException(exception);
      }
      byteBuffer.putInt(stringBytes.length);
      byteBuffer.put(stringBytes);
    }
  }

  private static void serializeProperties(
        final ByteBuffer byteBuffer,
        final Properties properties
  ) {
    if (properties == null) {
      byteBuffer.put(TRUE);
    } else {
      byteBuffer.put(FALSE);
      byteBuffer.putInt(properties.size());
      for (String propertyName : properties.stringPropertyNames()) {
        serializeString(byteBuffer, propertyName);
        serializeString(byteBuffer, properties.getProperty(propertyName));
      }
    }
  }

  @Override
  public ProxyRecoverable deserialize(final int version, final byte[] bytes) {
    if (version != VERSION) {
      throw new UnsupportedOperationException("Unsupported version: " + version + ".");
    }
    final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    final String writerType = deserializeString(byteBuffer);
    final ProxyFileWriterFactory.Configuration writerConfiguration =
        deserializeConfiguration(byteBuffer);
    final String writerIdentifier = deserializeString(byteBuffer);
    final String writePath = deserializeString(byteBuffer);
    return new ProxyRecoverable(writerType, writerConfiguration, writerIdentifier, writePath);
  }

  private static ProxyFileWriterFactory.Configuration deserializeConfiguration(
        final ByteBuffer byteBuffer
  ) {
    final String databaseName = deserializeString(byteBuffer);
    final String tableName = deserializeString(byteBuffer);
    final String tablePath = deserializeString(byteBuffer);
    final Properties tableProperties = deserializeProperties(byteBuffer);
    final Properties writerProperties = deserializeProperties(byteBuffer);
    final Properties carbonProperties = deserializeProperties(byteBuffer);
    return new ProxyFileWriterFactory.Configuration(
        databaseName,
        tableName,
        tablePath,
        tableProperties,
        writerProperties,
        carbonProperties
    );
  }

  private static String deserializeString(final ByteBuffer byteBuffer) {
    switch (byteBuffer.get()) {
      case TRUE:
        return null;
      case FALSE:
        final int stringByteLength = byteBuffer.getInt();
        final byte[] stringBytes = new byte[stringByteLength];
        byteBuffer.get(stringBytes);
        try {
          return new String(stringBytes, CHARSET);
        } catch (UnsupportedEncodingException exception) {
          throw new RuntimeException(exception);
        }
      default:
        throw new RuntimeException();
    }
  }

  @SuppressWarnings("ConstantConditions")
  private static Properties deserializeProperties(final ByteBuffer byteBuffer) {
    switch (byteBuffer.get()) {
      case TRUE:
        return null;
      case FALSE:
        final int propertyCount = byteBuffer.getInt();
        final Properties properties = new Properties();
        for (int index = 0; index < propertyCount; index++) {
          properties.put(deserializeString(byteBuffer), deserializeString(byteBuffer));
        }
        return properties;
      default:
        throw new RuntimeException();
    }
  }

}
