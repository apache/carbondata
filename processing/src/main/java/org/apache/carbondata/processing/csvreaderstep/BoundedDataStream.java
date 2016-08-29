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
package org.apache.carbondata.processing.csvreaderstep;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Custom reader class to read the data from file it will take care of reading
 * till the limit assigned to this class
 */
public class BoundedDataStream extends InputStream {

  /**
   * byte value of the new line character
   */
  private static final byte END_OF_LINE_BYTE_VALUE = '\n';

  /**
   * number of extra character to read
   */
  private static final int NUMBER_OF_EXTRA_CHARACTER_TO_READ = 100;

  /**
   * number of bytes remaining
   */
  private long remaining;
  /**
   * to check whether end of line is found
   */
  private boolean endOfLineFound = false;

  private DataInputStream in;

  public BoundedDataStream(DataInputStream in, long limit) {
    this.in = in;
    this.remaining = limit;
  }

  /**
   * Below method will be used to read the data from file
   *
   * @throws IOException
   *           problem while reading
   */
  @Override
  public int read() throws IOException {
    if (this.remaining == 0) {
      return -1;
    } else {
      int var1 = this.in.read();
      if (var1 >= 0) {
        --this.remaining;
      }

      return var1;
    }
  }

  /**
   * Below method will be used to read the data from file. If limit reaches in
   * that case it will read until new line character is reached
   *
   * @param buffer
   *          buffer in which data will be read
   * @param offset
   *          from position to buffer will be filled
   * @param length
   *          number of character to be read
   * @throws IOException
   *           problem while reading
   */
  @Override
  public int read(byte[] buffer, int offset, int length) throws IOException {
    if (this.remaining == 0) {
      return -1;
    } else {
      if (this.remaining < length) {
        length = (int) this.remaining;
      }

      length = this.in.read(buffer, offset, length);
      if (length >= 0) {
        this.remaining -= length;
        if (this.remaining == 0 && !endOfLineFound) {
          endOfLineFound = true;
          this.remaining += NUMBER_OF_EXTRA_CHARACTER_TO_READ;
        } else if (endOfLineFound) {
          int end = offset + length;
          for (int i = offset; i < end; i++) {
            if (buffer[i] == END_OF_LINE_BYTE_VALUE) {
              this.remaining = 0;
              return (i - offset) + 1;
            }
          }
          this.remaining += NUMBER_OF_EXTRA_CHARACTER_TO_READ;
        }
      }
      return length;
    }
  }

  @Override
  public void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }
}
