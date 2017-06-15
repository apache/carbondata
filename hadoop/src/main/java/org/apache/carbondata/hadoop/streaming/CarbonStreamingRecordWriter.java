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

package org.apache.carbondata.hadoop.streaming;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.carbondata.core.util.path.CarbonTablePath;


public class CarbonStreamingRecordWriter<K,V> extends RecordWriter<K, V> {

  private static final String utf8 = "UTF-8";

  private static final byte[] newline;

  static {

    try {

      newline = "\n".getBytes(utf8);

    } catch (UnsupportedEncodingException uee) {

      throw new IllegalArgumentException("Can't find " + utf8 + " encoding");
    }
  }

  private FSDataOutputStream outputStream;

  private FileSystem fs;

  private Path file;

  private volatile boolean isClosed;

  private final byte[] keyValueSeparator;

  public void initOut() throws IOException {

    outputStream = fs.create(file, false);

    isClosed = false;
  }

  public CarbonStreamingRecordWriter(
          Configuration conf,
          Path file,
          String keyValueSeparator) throws IOException {

    this.file = file;

    fs = FileSystem.get(conf);

    outputStream = fs.create(file, false);

    isClosed = false;

    try {

      this.keyValueSeparator = keyValueSeparator.getBytes(utf8);

    } catch (UnsupportedEncodingException uee) {

      throw new IllegalArgumentException("Can't find " + utf8 + "encoding");

    }

  }

  public CarbonStreamingRecordWriter(
          Configuration conf,
          Path file) throws IOException {

    this(conf, file, ",");

  }

  /**
   *  Write Object to byte stream.
   */

  private void writeObject(Object o) throws IOException {

    if (o instanceof Text) {
      Text to = (Text)o;

      outputStream.write(to.getBytes(), 0, to.getLength());

    } else {

      outputStream.write(o.toString().getBytes(utf8));

    }
  }

  /**
   * Write streaming data as text file (temporary)
   */

  @Override
  public synchronized void write(K key, V value) throws IOException {

    boolean isNULLKey = key == null || key instanceof NullWritable;

    boolean isNULLValue = value == null || value instanceof NullWritable;

    if (isNULLKey && isNULLValue) {

      return;
    }

    if (!isNULLKey) {

      writeObject(key);
    }

    if (!isNULLKey || !isNULLValue) {

      outputStream.write(keyValueSeparator);
    }

    if (!isNULLValue) {

      writeObject(value);
    }

    outputStream.write(newline);
  }

  private void closeInternal() throws IOException {

    if (!isClosed) {

      outputStream.close();

      isClosed = true;
    }

  }

  public void flush() throws IOException {

    outputStream.hflush();
  }

  public long getOffset() throws IOException {

    return outputStream.getPos();
  }

  public void commit(boolean finalCommit) throws IOException {

    closeInternal();

    Path commitFile = new Path(file.getParent(),
            CarbonTablePath.getCarbonDataPrefix() + System.currentTimeMillis());

    fs.rename(file, commitFile);

    if (!finalCommit) {
      initOut();
    }
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {

    closeInternal();
  }

}
