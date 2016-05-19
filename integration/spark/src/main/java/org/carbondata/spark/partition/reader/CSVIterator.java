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

package org.carbondata.spark.partition.reader;

import java.io.IOException;
import java.util.Iterator;

/**
 * Provides an Iterator over the data found in opencsv.
 */
public class CSVIterator implements Iterator<String[]> {
  private CSVReader reader;
  private String[] nextLine;

  /**
   * @param reader reader for the csv data.
   * @throws IOException if unable to read data from the reader.
   */
  public CSVIterator(CSVReader reader) throws IOException {
    this.reader = reader;
    nextLine = reader.readNext();
  }

  /**
   * Returns true if the iteration has more elements.
   * In other words, returns true if next() would return an element rather
   * than throwing an exception.
   *
   * @return true if the CSVIterator has more elements.
   */
  public boolean hasNext() {
    return nextLine != null;
  }

  /**
   * Returns the next elenebt in the iterator.
   *
   * @return The next element of the iterator.
   */
  public String[] next() {
    String[] temp = nextLine;
    try {
      nextLine = reader.readNext();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return temp;
  }

  /**
   * This method is not supported by openCSV and will throw a UnsupportedOperationException
   * if called.
   */
  public void remove() {
    throw new UnsupportedOperationException("This is a read only iterator.");
  }
}
