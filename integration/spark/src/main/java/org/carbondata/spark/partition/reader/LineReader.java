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

import java.io.BufferedReader;
import java.io.IOException;

/**
 * This class was created for issue #106 (https://sourceforge.net/p/opencsv/bugs/106/) where
 * carriage returns were being removed.  This class allows the user to determine if they wish to
 * keep or remove them from the data being read.
 * Created by scott on 2/19/15.
 */

public class LineReader {
  private BufferedReader reader;
  private boolean keepCarriageReturns;

  /**
   * LineReader constructor.
   *
   * @param reader              - Reader that data will be read from.
   * @param keepCarriageReturns - true if carriage returns should remain in the data, false
   *                            to remove them.
   */
  public LineReader(BufferedReader reader, boolean keepCarriageReturns) {
    this.reader = reader;
    this.keepCarriageReturns = keepCarriageReturns;
  }

  /**
   * Reads the next line from the Reader.
   *
   * @return - Line read from reader.
   * @throws IOException - on error from BufferedReader
   */
  public String readLine() throws IOException {
    return keepCarriageReturns ? readUntilNewline() : reader.readLine();
  }

  private String readUntilNewline() throws IOException {
    StringBuilder sb = new StringBuilder(CSVParser.INITIAL_READ_SIZE);
    for (int c = reader.read();
         c > -1 && c != '\n';
         c = reader.read()) {      //CHECKSTYLE:OFF    Approval No:Approval-V1R2C10_006
      sb.append((char) c);
    }//CHECKSTYLE:ON

    return sb.length() > 0 ? sb.toString() : null;
  }
}
