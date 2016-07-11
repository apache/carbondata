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

package org.carbondata.processing.csvreaderstep;

import java.io.IOException;
import java.io.Reader;

/**
 * Custom reader class to read the data from file
 * it will take care of reading till the limit assigned to  this
 * class
 */
public final class CustomReader extends Reader {

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
   * reader
   */
  private Reader reader;

  /**
   * to check whether end of line is found
   */
  private boolean endOfLineFound = false;

  public CustomReader(Reader var1) {
    this.reader = var1;
  }

  /**
   * Below method will  be used to read the data from file
   *
   * @throws IOException problem while reading
   */
  public int read() throws IOException {
    if (this.remaining == 0) {
      return -1;
    } else {
      int var1 = this.reader.read();
      if (var1 >= 0) {
        --this.remaining;
      }

      return var1;
    }
  }

  /**
   * Below method will be used to read the data from file.
   * If limit reaches in that case it will read until new line character is reached
   *
   * @param buffer buffer in which data will be read
   * @param offset from position to buffer will be filled
   * @param length number of character to be read
   * @throws IOException problem while reading
   */
  public int read(char[] buffer, int offset, int length) throws IOException {
    if (this.remaining == 0) {
      return -1;
    } else {
      if (this.remaining < length) {
        length = (int) this.remaining;
      }

      length = this.reader.read(buffer, offset, length);
      if (length >= 0) {
        this.remaining -= length;
        if (this.remaining == 0 && !endOfLineFound) {
          if (buffer[length - 1] != END_OF_LINE_BYTE_VALUE) {
            endOfLineFound = true;
            this.remaining += NUMBER_OF_EXTRA_CHARACTER_TO_READ;
          }
        } else if (endOfLineFound) {
          for (int i = 0; i < length; i++) {
            if (buffer[i] == END_OF_LINE_BYTE_VALUE) {
              this.remaining = 0;
              return i + 1;
            }
          }
          this.remaining += NUMBER_OF_EXTRA_CHARACTER_TO_READ;
        }
      }
      return length;
    }
  }

  /**
   * number of bytes to skip
   */
  public long skip(long numberOfCharacterToSkip) throws IOException {
    throw new UnsupportedOperationException("Not supported");
  }

  /**
   * to set the stream position
   */
  public void mark(int readAheadLimit) throws IOException {
    throw new UnsupportedOperationException("Not supported");
  }

  /**
   * to close the stream
   */
  public void close() throws IOException {
    this.reader.close();
  }

  /**
   * Max number of bytes that can be read by the stream
   *
   * @param limit
   */
  public void setLimit(long limit) {
    this.remaining = limit;
  }

  /**
   * number of remaining bytes
   *
   * @return number of remaining bytes
   */
  public final long getRemaining() {
    return this.remaining;
  }

}
